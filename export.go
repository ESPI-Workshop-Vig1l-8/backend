package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// Training export: same rules as IA_Predictions/exporter_couchdb.py, so the CSV
// can be used as is by entrainement.py (columns of generer_données.py).
const (
	exportMinSegment = 60                // one Isolation Forest window (2 min)
	exportMaxGapMs   = 3 * 2 * 1000      // 3 × the 2 s period
	exportMaxRange   = 7 * 24 * 3600_000 // 7 days ≈ 300,000 readings
	exportPageSize   = 5000
)

var exportHeader = []string{"horodatage", "segment", "temperature", "humidité", "gaz"}

type trainingRow struct {
	At             int64
	Segment        int
	Temp, Hum, Gas float64
}

type period struct{ Start, End int64 }

func num(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

// trainingRows keeps only "normal" readings and splits them into continuous
// segments. A new segment starts after a lost message (gap in seq), a reboot
// (uptime going back), a time gap, or any rejected reading; segments shorter
// than minSegment are dropped. docs must be sorted by received_at.
func trainingRows(docs []map[string]any, annotations []period, keepAnnotated bool, minSegment int) []trainingRow {
	var out, current []trainingRow
	segment := 0
	flush := func() {
		if len(current) >= minSegment {
			for i := range current {
				current[i].Segment = segment
			}
			out = append(out, current...)
			segment++
		}
		current = nil
	}

	var prev map[string]any
	for _, d := range docs {
		at, _ := num(d["received_at"])
		if prev != nil {
			pAt, _ := num(prev["received_at"])
			pSeq, _ := num(prev["seq"])
			seq, _ := num(d["seq"])
			pUp, _ := num(prev["uptime_ms"])
			up, _ := num(d["uptime_ms"])
			if seq != pSeq+1 || up < pUp || at-pAt > exportMaxGapMs {
				flush()
			}
		}
		prev = d

		status, _ := d["status"].(map[string]any)
		temp, okT := num(d["temp_c"])
		hum, okH := num(d["hum_pct"])
		gas, okG := num(d["gas_mv"])
		valid := status["dht"] == "ok" && okT && okH && okG && status["gas_warm"] == true
		if valid && !keepAnnotated {
			for _, p := range annotations {
				if int64(at) >= p.Start && int64(at) <= p.End {
					valid = false
					break
				}
			}
		}
		if !valid {
			flush()
			continue
		}
		current = append(current, trainingRow{At: int64(at), Temp: temp, Hum: hum, Gas: gas})
	}
	flush()
	return out
}

// allTelemetry reads every telemetry document of a device in [from, to], page by page.
func (a *API) allTelemetry(ctx context.Context, device string, from, to int64) ([]map[string]any, error) {
	start := fmt.Sprintf("%s:%013d", device, from)
	end := fmt.Sprintf("%s:%013d", device, to)
	var docs []map[string]any
	for {
		page, err := a.couch.AllDocs(ctx, "telemetry", start, end, false, exportPageSize+1)
		if err != nil {
			return nil, err
		}
		if len(docs) > 0 && len(page) > 0 && page[0]["_id"] == start {
			page = page[1:] // the cursor document was already read
		}
		docs = append(docs, page...)
		if len(page) < exportPageSize {
			return docs, nil
		}
		start, _ = page[len(page)-1]["_id"].(string)
	}
}

func (a *API) deviceAnnotations(ctx context.Context, device string) ([]period, error) {
	docs, err := a.couch.AllDocs(ctx, "events", "annotation:", "annotation:￰", false, 1000)
	if err != nil {
		return nil, err
	}
	var out []period
	for _, d := range docs {
		if d["type"] != "annotation" || (d["device_id"] != device && d["device_id"] != nil) {
			continue
		}
		s, ok1 := num(d["start"])
		e, ok2 := num(d["end"])
		if ok1 && ok2 {
			out = append(out, period{int64(s), int64(e)})
		}
	}
	return out, nil
}

// GET /api/v1/devices/{id}/export.csv?from=&to=&annotations=keep
func (a *API) handleExportCSV(w http.ResponseWriter, r *http.Request) {
	device, ok := deviceParam(w, r)
	if !ok {
		return
	}
	now := time.Now().UnixMilli()
	to, err := queryInt(r, "to", now, 1, now+60_000)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	from, err := queryInt(r, "from", to-24*3600_000, 1, to-1)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if to-from > exportMaxRange {
		writeError(w, http.StatusBadRequest, "range too large: 7 days max")
		return
	}
	keep := r.URL.Query().Get("annotations") == "keep"

	docs, err := a.allTelemetry(r.Context(), device, from, to)
	if err != nil {
		a.storageError(w, err)
		return
	}
	sort.SliceStable(docs, func(i, j int) bool {
		ai, _ := num(docs[i]["received_at"])
		aj, _ := num(docs[j]["received_at"])
		return ai < aj
	})
	annotations, err := a.deviceAnnotations(r.Context(), device)
	if err != nil {
		a.storageError(w, err)
		return
	}
	rows := trainingRows(docs, annotations, keep, exportMinSegment)

	kind := "normal"
	if keep {
		kind = "evaluation"
	}
	name := fmt.Sprintf("%s_%s_%s.csv", device, kind, time.UnixMilli(to).UTC().Format("20060102-1504"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Export-Readings", strconv.Itoa(len(docs)))
	w.Header().Set("X-Export-Kept", strconv.Itoa(len(rows)))

	cw := csv.NewWriter(w)
	cw.Write(exportHeader)
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	for _, row := range rows {
		cw.Write([]string{
			time.UnixMilli(row.At).UTC().Format("2006-01-02T15:04:05.000Z"),
			strconv.Itoa(row.Segment), f(row.Temp), f(row.Hum), f(row.Gas),
		})
	}
	cw.Flush()
}
