package main

import (
	"encoding/json"

	"github.com/go-chi/chi/v5"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func telemetryJSON(mod func(m map[string]any)) []byte {
	m := map[string]any{
		"v": 1, "device_id": "VIG1L-8-NODE04", "seq": 12, "uptime_ms": 24000,
		"temp_c": 25.8, "hum_pct": 61.5, "gas_mv": 259, "pir": false, "pir_events": 0,
		"status": map[string]any{"dht": "ok", "gas_warm": true, "env_warn": false, "rssi": -58},
	}
	if mod != nil {
		mod(m)
	}
	b, _ := json.Marshal(m)
	return b
}

func TestTelemetryValidate(t *testing.T) {
	cases := []struct {
		name    string
		mod     func(m map[string]any)
		topic   string
		wantErr string
	}{
		{"valid", nil, "VIG1L-8-NODE04", ""},
		{"device spoofing", nil, "VIG1L-8-NODE05", "does not match topic"},
		{"bad version", func(m map[string]any) { m["v"] = 2 }, "VIG1L-8-NODE04", "version"},
		{"dht error with nulls", func(m map[string]any) {
			m["temp_c"], m["hum_pct"] = nil, nil
			m["status"].(map[string]any)["dht"] = "error"
		}, "VIG1L-8-NODE04", ""},
		{"dht ok without values", func(m map[string]any) { m["temp_c"] = nil }, "VIG1L-8-NODE04", "missing"},
		{"dht error with values", func(m map[string]any) { m["status"].(map[string]any)["dht"] = "error" }, "VIG1L-8-NODE04", "present"},
		{"temperature out of range", func(m map[string]any) { m["temp_c"] = 300 }, "VIG1L-8-NODE04", "range"},
		{"gas missing", func(m map[string]any) { delete(m, "gas_mv") }, "VIG1L-8-NODE04", "gas_mv"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var tel Telemetry
			if _, err := decode(telemetryJSON(c.mod), &tel); err != nil {
				t.Fatal(err)
			}
			err := tel.Validate(c.topic)
			if c.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestDecodeKeepsUnknownFields(t *testing.T) {
	var tel Telemetry
	raw, err := decode(telemetryJSON(func(m map[string]any) { m["future_field"] = 1 }), &tel)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["future_field"]; !ok {
		t.Fatal("stored document must keep the payload unchanged")
	}
}

func TestAlertValidate(t *testing.T) {
	ok := AlertInput{Source: "ia-vision", Type: "INTRUSION"}
	if err := ok.Validate(); err != nil || ok.Level != LevelWarning || ok.Type != "intrusion" {
		t.Fatalf("defaults not applied: %+v %v", ok, err)
	}
	conf := 1.5
	for _, bad := range []AlertInput{
		{Source: "", Type: "intrusion"},
		{Source: "ia", Type: "intrusion", Level: "critical"},
		{Source: "ia", Type: "intrusion", Confidence: &conf},
		{Source: "ia", Type: "intrusion", DeviceID: "../etc"},
		{Source: "ia", Type: "intrusion", Data: json.RawMessage("{bad")},
	} {
		if err := bad.Validate(); err == nil {
			t.Errorf("expected an error for %+v", bad)
		}
	}
}

func TestAnnotationAndCommandValidate(t *testing.T) {
	now := time.Now().UnixMilli()
	if err := (&AnnotationInput{DeviceID: "N1", Start: now, End: now + 1000, Label: "gas_test"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (&AnnotationInput{DeviceID: "N1", Start: now, End: now, Label: "gas_test"}).Validate(); err == nil {
		t.Fatal("start == end must be refused")
	}
	cmd := CommandInput{Strobe: true}
	if err := cmd.Validate(); err != nil || cmd.DurationS != 5 {
		t.Fatalf("default duration not applied: %+v %v", cmd, err)
	}
	if err := (&CommandInput{DurationS: 600}).Validate(); err == nil {
		t.Fatal("600 s must be refused")
	}
}

func TestIDGenUniqueAndSorted(t *testing.T) {
	g := NewIDGen()
	id1, t1 := g.Next("N1", 1000)
	id2, t2 := g.Next("N1", 1000)
	id3, _ := g.Next("N1", 999)
	if id1 == id2 || t2 != t1+1 || id3 <= id2 {
		t.Fatalf("ids not unique/increasing: %s %s %s", id1, id2, id3)
	}
	if id1 != "N1:0000000001000" {
		t.Fatalf("unexpected format %s", id1)
	}
}

func TestViewKeyAndKeyTime(t *testing.T) {
	ms := time.Date(2026, 10, 6, 14, 37, 12, 0, time.UTC).UnixMilli()
	key := viewKey("N1", ms, 6)
	b, _ := json.Marshal(key)
	if string(b) != `["N1",2026,10,6,14,37]` {
		t.Fatalf("unexpected key %s", b)
	}
	var decoded []any
	json.Unmarshal(b, &decoded)
	got, ok := keyTime(decoded)
	if !ok || got != time.Date(2026, 10, 6, 14, 37, 0, 0, time.UTC).UnixMilli() {
		t.Fatalf("keyTime: %v %v", got, ok)
	}
	if len(viewKey("N1", ms, 5)) != 5 {
		t.Fatal("hour key must have 5 elements")
	}
}

func TestAuthRoles(t *testing.T) {
	a := &API{cfg: Config{OperatorToken: "operator-token-123456", ServiceToken: "service-token-1234567"}}
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	cases := []struct {
		min  Role
		auth string
		path string
		want int
	}{
		{RoleService, "", "/api/v1/devices", http.StatusUnauthorized},
		{RoleService, "Bearer wrong", "/api/v1/devices", http.StatusUnauthorized},
		{RoleService, "Bearer service-token-1234567", "/api/v1/devices", http.StatusNoContent},
		{RoleOperator, "Bearer service-token-1234567", "/api/v1/devices/N1/command", http.StatusForbidden},
		{RoleOperator, "Bearer operator-token-123456", "/api/v1/devices/N1/command", http.StatusNoContent},
		{RoleService, "", "/ws?token=operator-token-123456", http.StatusNoContent},
		{RoleService, "", "/api/v1/devices?token=operator-token-123456", http.StatusUnauthorized},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, c.path, nil)
		if c.auth != "" {
			req.Header.Set("Authorization", c.auth)
		}
		rec := httptest.NewRecorder()
		a.requireRole(c.min)(ok).ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s %q: got %d, want %d", c.path, c.auth, rec.Code, c.want)
		}
	}
}

func TestReadJSONRefusesUnknownFields(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"strobe":true,"admin":true}`))
	rec := httptest.NewRecorder()
	var cmd CommandInput
	if readJSON(rec, req, &cmd) || rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field accepted (code %d)", rec.Code)
	}
}

func TestOriginAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://192.168.10.1:10443/ws", nil)
	req.Header.Set("Origin", "http://192.168.10.1:10443")
	if !originAllowed(req, nil) {
		t.Fatal("same origin must be allowed")
	}
	req.Header.Set("Origin", "http://evil.example")
	if originAllowed(req, nil) {
		t.Fatal("foreign origin must be refused")
	}
	if !originAllowed(req, []string{"http://evil.example"}) {
		t.Fatal("configured origin must be allowed")
	}
}

func TestRateLimiter(t *testing.T) {
	rl := newRateLimiter(1, 3)
	h := rl.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	codes := map[int]int{}
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		codes[rec.Code]++
	}
	if codes[http.StatusTooManyRequests] != 2 {
		t.Fatalf("expected 2 refused requests, got %v", codes)
	}
}

func TestPathParamDecodesEncodedIDs(t *testing.T) {
	var got string
	r := chi.NewRouter()
	r.Post("/api/v1/alerts/{id}/ack", func(w http.ResponseWriter, req *http.Request) { got = pathParam(req, "id") })
	for _, path := range []string{
		"/api/v1/alerts/alert:1791290306299/ack",   // curl
		"/api/v1/alerts/alert%3A1791290306299/ack", // dashboard (encodeURIComponent)
	} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, path, nil))
		if got != "alert:1791290306299" || !alertIDPattern.MatchString(got) {
			t.Errorf("%s: got %q", path, got)
		}
	}
	for _, bad := range []string{"alert:", "alert:123", "alert:1791290306299/../x", "telemetry:1791290306299"} {
		if alertIDPattern.MatchString(bad) {
			t.Errorf("%q must be refused", bad)
		}
	}
}
