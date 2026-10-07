package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"golang.org/x/time/rate"
)

type Role int

const (
	RoleNone Role = iota
	RoleService
	RoleOperator
)

type ctxKey struct{}

type API struct {
	cfg       Config
	couch     *Couch
	ids       *IDGen
	state     *State
	hub       *Hub
	ingest    *Ingestor
	telemetry *BatchWriter
	events    *BatchWriter
	started   time.Time

	couchOK   bool
	couchMu   sync.RWMutex
	couchSeen time.Time
}

func (a *API) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RealIP, middleware.Recoverer, requestLog, newRateLimiter(20, 40).middleware)
	if len(a.cfg.AllowedOrigins) > 0 {
		r.Use(cors.Handler(cors.Options{
			AllowedOrigins: a.cfg.AllowedOrigins,
			AllowedMethods: []string{"GET", "POST"},
			AllowedHeaders: []string{"Authorization", "Content-Type"},
			MaxAge:         300,
		}))
	}

	r.Get("/api/v1/health", a.handleHealth)

	// Any valid token: dashboard (operator) or AI services
	r.Group(func(r chi.Router) {
		r.Use(a.requireRole(RoleService))
		r.Get("/ws", a.handleWS)
		r.Get("/api/v1/session", a.handleSession)
		r.Get("/api/v1/devices", a.handleDevices)
		r.Get("/api/v1/devices/{id}/telemetry", a.handleTelemetryHistory)
		r.Get("/api/v1/devices/{id}/events", a.handleDeviceEvents)
		r.Get("/api/v1/alerts", a.handleListAlerts)
		r.Post("/api/v1/alerts", a.handlePostAlert)
		r.Get("/api/v1/annotations", a.handleListAnnotations)
	})

	// Operator only: actions on the nodes and on the stored data
	r.Group(func(r chi.Router) {
		r.Use(a.requireRole(RoleOperator))
		r.Post("/api/v1/alerts/{id}/ack", a.handleAckAlert)
		r.Post("/api/v1/annotations", a.handlePostAnnotation)
		r.Post("/api/v1/devices/{id}/command", a.handleCommand)
	})
	return r
}

// ============================================================================
// Middleware
// ============================================================================

// tokenRole compares in constant time so the token cannot be guessed by timing.
func (a *API) tokenRole(token string) Role {
	if token == "" {
		return RoleNone
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.OperatorToken)) == 1 {
		return RoleOperator
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.ServiceToken)) == 1 {
		return RoleService
	}
	return RoleNone
}

// requireRole accepts "Authorization: Bearer <token>"; the WebSocket also
// accepts ?token= because browsers cannot set headers on it.
func (a *API) requireRole(min Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" && r.URL.Path == "/ws" {
				token = r.URL.Query().Get("token")
			}
			role := a.tokenRole(token)
			switch {
			case role == RoleNone:
				writeError(w, http.StatusUnauthorized, "missing or invalid token")
			case role < min:
				writeError(w, http.StatusForbidden, "operator token required")
			default:
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, role)))
			}
		})
	}
}

// requestLog logs the path without the query string (it may hold the WS token).
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		if r.URL.Path != "/api/v1/health" {
			log.Printf("[HTTP] %s %s %d %s %s", r.Method, r.URL.Path, ww.Status(), time.Since(start).Round(time.Millisecond), r.RemoteAddr)
		}
	})
}

type rateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	r        rate.Limit
	burst    int
}

func newRateLimiter(perSecond float64, burst int) *rateLimiter {
	rl := &rateLimiter{limiters: map[string]*rate.Limiter{}, r: rate.Limit(perSecond), burst: burst}
	go func() {
		for range time.Tick(10 * time.Minute) {
			rl.mu.Lock()
			rl.limiters = map[string]*rate.Limiter{}
			rl.mu.Unlock()
		}
	}()
	return rl
}

func (rl *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		rl.mu.Lock()
		l, ok := rl.limiters[ip]
		if !ok {
			l = rate.NewLimiter(rl.r, rl.burst)
			rl.limiters[ip] = l
		}
		rl.mu.Unlock()
		if !l.Allow() {
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ============================================================================
// Helpers
// ============================================================================

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// readJSON decodes a small JSON body and refuses unknown fields.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func (a *API) storageError(w http.ResponseWriter, err error) {
	log.Printf("[CouchDB] %v", err)
	writeError(w, http.StatusBadGateway, "storage unavailable")
}

func queryInt(r *http.Request, key string, def, min, max int64) (int64, error) {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < min || v > max {
		return 0, fmt.Errorf("%s: integer between %d and %d", key, min, max)
	}
	return v, nil
}

// pathParam returns a decoded path parameter: chi gives the raw segment, so
// an id sent as encodeURIComponent("alert:…") arrives as "alert%3A…".
func pathParam(r *http.Request, name string) string {
	raw := chi.URLParam(r, name)
	if v, err := url.PathUnescape(raw); err == nil {
		return v
	}
	return raw
}

func deviceParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := pathParam(r, "id")
	if !deviceIDPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid device id")
		return "", false
	}
	return id, true
}

// ============================================================================
// Handlers
// ============================================================================

func (a *API) checkCouch(ctx context.Context) bool {
	a.couchMu.RLock()
	fresh := time.Since(a.couchSeen) < 10*time.Second
	ok := a.couchOK
	a.couchMu.RUnlock()
	if fresh {
		return ok
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ok = a.couch.Up(ctx) == nil
	a.couchMu.Lock()
	a.couchOK, a.couchSeen = ok, time.Now()
	a.couchMu.Unlock()
	return ok
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	mqttOK, couchOK := a.ingest.Connected(), a.checkCouch(r.Context())
	status, code := "ok", http.StatusOK
	if !mqttOK || !couchOK {
		status, code = "degraded", http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status":   status,
		"mqtt":     mqttOK,
		"couchdb":  couchOK,
		"uptime_s": int(time.Since(a.started).Seconds()),
		"queued":   map[string]int{"telemetry": a.telemetry.Len(), "events": a.events.Len()},
		"rejected": a.ingest.Rejected(),
		"ws":       a.hub.Count(),
	})
}

func (a *API) handleWS(w http.ResponseWriter, r *http.Request) {
	alerts, err := a.couch.AllDocs(r.Context(), "events", "alert:￰", "alert:", true, 20)
	if err != nil {
		alerts = []map[string]any{}
	}
	a.hub.Serve(w, r, map[string]any{"devices": a.state.Snapshot(), "alerts": alerts})
}

func (a *API) handleSession(w http.ResponseWriter, r *http.Request) {
	role := "service"
	if r.Context().Value(ctxKey{}) == RoleOperator {
		role = "operator"
	}
	writeJSON(w, http.StatusOK, map[string]string{"role": role})
}

func (a *API) handleDevices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.state.Snapshot())
}

// GET /api/v1/devices/{id}/telemetry?from=<ms>&to=<ms>&step=raw|minute|hour
// raw: stored readings (6 h max); minute/hour: avg/min/max from the CouchDB views.
func (a *API) handleTelemetryHistory(w http.ResponseWriter, r *http.Request) {
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
	from, err := queryInt(r, "from", to-3600_000, 1, to-1)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	step := r.URL.Query().Get("step")
	if step == "" {
		step = "raw"
	}
	maxRange := map[string]int64{"raw": 6 * 3600_000, "minute": 7 * 24 * 3600_000, "hour": 90 * 24 * 3600_000}
	limit, ok := maxRange[step]
	if !ok {
		writeError(w, http.StatusBadRequest, "step: raw, minute or hour")
		return
	}
	if to-from > limit {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("range too large for step %s", step))
		return
	}

	var points []map[string]any
	if step == "raw" {
		points, err = a.rawTelemetry(r.Context(), device, from, to)
	} else {
		points, err = a.aggregatedTelemetry(r.Context(), device, from, to, step)
	}
	if err != nil {
		a.storageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"device_id": device, "step": step, "from": from, "to": to, "points": points})
}

func (a *API) rawTelemetry(ctx context.Context, device string, from, to int64) ([]map[string]any, error) {
	docs, err := a.couch.AllDocs(ctx, "telemetry",
		fmt.Sprintf("%s:%013d", device, from), fmt.Sprintf("%s:%013d", device, to), false, 11000)
	if err != nil {
		return nil, err
	}
	points := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		status, _ := d["status"].(map[string]any)
		points = append(points, map[string]any{
			"t": d["received_at"], "temp_c": d["temp_c"], "hum_pct": d["hum_pct"], "gas_mv": d["gas_mv"],
			"pir": d["pir"], "env_warn": status["env_warn"], "gas_warm": status["gas_warm"],
		})
	}
	return points, nil
}

// viewKey builds the [device, Y, M, D, h(, m)] key of the telemetry views (UTC).
func viewKey(device string, ms int64, level int) []any {
	t := time.UnixMilli(ms).UTC()
	key := []any{device, t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute()}
	return key[:level]
}

func keyTime(key []any) (int64, bool) {
	if len(key) < 5 {
		return 0, false
	}
	n := make([]int, 6)
	for i := 1; i < len(key) && i < 6; i++ {
		f, ok := key[i].(float64)
		if !ok {
			return 0, false
		}
		n[i] = int(f)
	}
	return time.Date(n[1], time.Month(n[2]), n[3], n[4], n[5], 0, 0, time.UTC).UnixMilli(), true
}

func (a *API) aggregatedTelemetry(ctx context.Context, device string, from, to int64, step string) ([]map[string]any, error) {
	level := 6
	if step == "hour" {
		level = 5
	}
	start, end := viewKey(device, from, level), append(viewKey(device, to, level), map[string]any{})
	byTime := map[int64]map[string]any{}
	var order []int64
	for _, metric := range []string{"temp_c", "hum_pct", "gas_mv"} {
		rows, err := a.couch.StatsView(ctx, "telemetry", "telemetry", metric, start, end, level)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			t, ok := keyTime(row.Key)
			if !ok || row.Value.Count == 0 {
				continue
			}
			p, seen := byTime[t]
			if !seen {
				p = map[string]any{"t": t, "min": map[string]float64{}, "max": map[string]float64{}}
				byTime[t] = p
				order = append(order, t)
			}
			p[metric] = row.Value.Sum / row.Value.Count
			p["min"].(map[string]float64)[metric] = row.Value.Min
			p["max"].(map[string]float64)[metric] = row.Value.Max
		}
	}
	points := make([]map[string]any, 0, len(order))
	for _, t := range sortedInt64(order) {
		points = append(points, byTime[t])
	}
	return points, nil
}

func sortedInt64(v []int64) []int64 {
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v
}

func (a *API) handleDeviceEvents(w http.ResponseWriter, r *http.Request) {
	device, ok := deviceParam(w, r)
	if !ok {
		return
	}
	limit, err := queryInt(r, "limit", 50, 1, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	docs, err := a.couch.AllDocs(r.Context(), "events", device+":￰", device+":", true, int(limit))
	if err != nil {
		a.storageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, docs)
}

func (a *API) handleListAlerts(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 50, 1, 200)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	docs, err := a.couch.AllDocs(r.Context(), "events", "alert:￰", "alert:", true, int(limit))
	if err != nil {
		a.storageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, docs)
}

// POST /api/v1/alerts — mandatory endpoint of the subject. A "confirmed"
// alert on a node makes its environment LED blink.
func (a *API) handlePostAlert(w http.ResponseWriter, r *http.Request) {
	var in AlertInput
	if !readJSON(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, ts := a.ids.Next("alert", time.Now().UnixMilli())
	doc := map[string]any{
		"_id": id, "received_at": ts, "type": "alert",
		"category": in.Type, "level": in.Level, "source": in.Source,
		"device_id": in.DeviceID, "confidence": in.Confidence, "details": in.Details,
		"acked_at": nil,
	}
	if len(in.Data) > 0 {
		doc["data"] = in.Data
	}
	if err := a.couch.Put(r.Context(), "events", id, doc); err != nil {
		a.storageError(w, err)
		return
	}
	log.Printf("[ALERT] %s %s from %s (device %q)", in.Level, in.Type, in.Source, in.DeviceID)
	a.hub.Broadcast("alert", doc)

	action := "recorded"
	if in.Level == LevelConfirmed && in.DeviceID != "" {
		cmd := CommandInput{Strobe: true, DurationS: a.cfg.AlertStrobeS}
		if _, err := a.ingest.SendCommand(in.DeviceID, cmd, "alert:"+in.Source); err != nil {
			log.Printf("[ALERT] LED command to %s failed: %v", in.DeviceID, err)
			action = "recorded, LED command failed"
		} else {
			action = "recorded, LED blinking on " + in.DeviceID
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "level": in.Level, "action": action})
}

func (a *API) handleAckAlert(w http.ResponseWriter, r *http.Request) {
	id := pathParam(r, "id")
	if !alertIDPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid alert id")
		return
	}
	var doc map[string]any
	if err := a.couch.Get(r.Context(), "events", id, &doc); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "alert not found")
		} else {
			a.storageError(w, err)
		}
		return
	}
	if doc["acked_at"] != nil {
		writeJSON(w, http.StatusOK, doc)
		return
	}
	doc["acked_at"] = time.Now().UnixMilli()
	if err := a.couch.Put(r.Context(), "events", id, doc); err != nil {
		if errors.Is(err, ErrConflict) {
			writeError(w, http.StatusConflict, "alert modified meanwhile, retry")
		} else {
			a.storageError(w, err)
		}
		return
	}
	a.hub.Broadcast("alert_ack", doc)
	writeJSON(w, http.StatusOK, doc)
}

func (a *API) handleListAnnotations(w http.ResponseWriter, r *http.Request) {
	docs, err := a.couch.AllDocs(r.Context(), "events", "annotation:￰", "annotation:", true, 100)
	if err != nil {
		a.storageError(w, err)
		return
	}
	if device := r.URL.Query().Get("device"); device != "" {
		filtered := docs[:0]
		for _, d := range docs {
			if d["device_id"] == device {
				filtered = append(filtered, d)
			}
		}
		docs = filtered
	}
	writeJSON(w, http.StatusOK, docs)
}

func (a *API) handlePostAnnotation(w http.ResponseWriter, r *http.Request) {
	var in AnnotationInput
	if !readJSON(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, _ := a.ids.Next("annotation", in.Start)
	doc := map[string]any{
		"_id": id, "type": "annotation", "device_id": in.DeviceID,
		"start": in.Start, "end": in.End, "label": in.Label, "note": in.Note,
		"created_at": time.Now().UnixMilli(),
	}
	if err := a.couch.Put(r.Context(), "events", id, doc); err != nil {
		a.storageError(w, err)
		return
	}
	a.hub.Broadcast("annotation", doc)
	writeJSON(w, http.StatusCreated, doc)
}

func (a *API) handleCommand(w http.ResponseWriter, r *http.Request) {
	device, ok := deviceParam(w, r)
	if !ok {
		return
	}
	if !a.state.Known(device) {
		writeError(w, http.StatusNotFound, "unknown device")
		return
	}
	var in CommandInput
	if !readJSON(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	doc, err := a.ingest.SendCommand(device, in, "operator")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, doc)
}
