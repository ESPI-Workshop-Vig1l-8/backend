package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// Couch is a minimal CouchDB HTTP client.
type Couch struct {
	base       string
	user, pass string
	http       *http.Client
}

func NewCouch(base, user, pass string) *Couch {
	return &Couch{
		base: strings.TrimRight(base, "/"),
		user: user,
		pass: pass,
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Couch) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return ErrNotFound
	case resp.StatusCode == http.StatusConflict:
		return ErrConflict
	case resp.StatusCode >= 300:
		return fmt.Errorf("couchdb %s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func docPath(db, id string) string {
	return "/" + db + "/" + url.PathEscape(id)
}

// Up checks that CouchDB answers and our credentials are accepted.
func (c *Couch) Up(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/telemetry", nil, nil, nil)
}

func (c *Couch) Get(ctx context.Context, db, id string, out any) error {
	return c.do(ctx, http.MethodGet, docPath(db, id), nil, nil, out)
}

func (c *Couch) Put(ctx context.Context, db, id string, doc any) error {
	return c.do(ctx, http.MethodPut, docPath(db, id), nil, doc, nil)
}

// BulkDocs writes several documents in one request. Documents refused by
// CouchDB (e.g. duplicate _id) are logged and dropped; an HTTP error is
// returned so that the caller can retry the whole batch.
func (c *Couch) BulkDocs(ctx context.Context, db string, docs []map[string]any) error {
	var results []struct {
		ID     string `json:"id"`
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if err := c.do(ctx, http.MethodPost, "/"+db+"/_bulk_docs", nil, map[string]any{"docs": docs}, &results); err != nil {
		return err
	}
	for _, r := range results {
		if r.Error != "" {
			log.Printf("[CouchDB] %s/%s refused: %s (%s)", db, r.ID, r.Error, r.Reason)
		}
	}
	return nil
}

// AllDocs reads documents whose _id is between startKey and endKey.
func (c *Couch) AllDocs(ctx context.Context, db, startKey, endKey string, descending bool, limit int) ([]map[string]any, error) {
	q := url.Values{}
	sk, _ := json.Marshal(startKey)
	ek, _ := json.Marshal(endKey)
	q.Set("startkey", string(sk))
	q.Set("endkey", string(ek))
	q.Set("include_docs", "true")
	q.Set("limit", fmt.Sprint(limit))
	if descending {
		q.Set("descending", "true")
	}
	var res struct {
		Rows []struct {
			Doc map[string]any `json:"doc"`
		} `json:"rows"`
	}
	if err := c.do(ctx, http.MethodGet, "/"+db+"/_all_docs", q, nil, &res); err != nil {
		return nil, err
	}
	docs := make([]map[string]any, 0, len(res.Rows))
	for _, r := range res.Rows {
		if r.Doc != nil {
			docs = append(docs, r.Doc)
		}
	}
	return docs, nil
}

// StatsRow is a row of a view reduced with _stats.
type StatsRow struct {
	Key   []any `json:"key"`
	Value struct {
		Sum   float64 `json:"sum"`
		Count float64 `json:"count"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
	} `json:"value"`
}

func (c *Couch) StatsView(ctx context.Context, db, ddoc, view string, startKey, endKey []any, groupLevel int) ([]StatsRow, error) {
	q := url.Values{}
	sk, _ := json.Marshal(startKey)
	ek, _ := json.Marshal(endKey)
	q.Set("startkey", string(sk))
	q.Set("endkey", string(ek))
	q.Set("group_level", fmt.Sprint(groupLevel))
	var res struct {
		Rows []StatsRow `json:"rows"`
	}
	path := fmt.Sprintf("/%s/_design/%s/_view/%s", db, ddoc, view)
	if err := c.do(ctx, http.MethodGet, path, q, nil, &res); err != nil {
		return nil, err
	}
	return res.Rows, nil
}

// BatchWriter buffers documents and writes them with _bulk_docs. A failed
// batch stays in the queue and is retried on the next tick.
type BatchWriter struct {
	couch    *Couch
	db       string
	interval time.Duration
	maxQueue int

	mu    sync.Mutex
	queue []map[string]any
}

func NewBatchWriter(couch *Couch, db string) *BatchWriter {
	return &BatchWriter{couch: couch, db: db, interval: 2 * time.Second, maxQueue: 20000}
}

func (w *BatchWriter) Add(doc map[string]any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) >= w.maxQueue {
		w.queue = w.queue[1:]
		log.Printf("[CouchDB] %s queue full, oldest document dropped", w.db)
	}
	w.queue = append(w.queue, doc)
}

func (w *BatchWriter) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.queue)
}

// Run flushes the queue periodically until ctx is cancelled, then flushes once more.
func (w *BatchWriter) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flush(ctx)
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			w.flush(final)
			cancel()
			return
		}
	}
}

func (w *BatchWriter) flush(ctx context.Context) {
	const batchSize = 500
	for {
		w.mu.Lock()
		n := min(len(w.queue), batchSize)
		batch := append([]map[string]any(nil), w.queue[:n]...)
		w.mu.Unlock()
		if n == 0 {
			return
		}
		if err := w.couch.BulkDocs(ctx, w.db, batch); err != nil {
			log.Printf("[CouchDB] %s: %d documents not written yet: %v", w.db, w.Len(), err)
			return
		}
		w.mu.Lock()
		w.queue = w.queue[n:]
		w.mu.Unlock()
	}
}

// IDGen builds time-sorted document ids "<prefix>:<epoch ms, 13 digits>".
// The timestamp is bumped by 1 ms when needed so ids stay unique per prefix.
type IDGen struct {
	mu   sync.Mutex
	last map[string]int64
}

func NewIDGen() *IDGen {
	return &IDGen{last: map[string]int64{}}
}

func (g *IDGen) Next(prefix string, ms int64) (string, int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if last, ok := g.last[prefix]; ok && ms <= last {
		ms = last + 1
	}
	g.last[prefix] = ms
	return fmt.Sprintf("%s:%013d", prefix, ms), ms
}
