package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
)

// Data format v1, shared with the firmware (see the infra README).

var (
	deviceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	wordPattern     = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
	alertIDPattern  = regexp.MustCompile(`^alert:[0-9]{13}$`)
)

// SensorStatus is the "status" object of a telemetry message.
type SensorStatus struct {
	DHT     string `json:"dht"`
	GasWarm bool   `json:"gas_warm"`
	EnvWarn bool   `json:"env_warn"`
	RSSI    int    `json:"rssi"`
}

// Telemetry is published by a node every 2 s on vigil8/<id>/telemetry.
type Telemetry struct {
	V         int          `json:"v"`
	DeviceID  string       `json:"device_id"`
	Seq       uint32       `json:"seq"`
	UptimeMs  uint64       `json:"uptime_ms"`
	TempC     *float64     `json:"temp_c"`
	HumPct    *float64     `json:"hum_pct"`
	GasMv     *int         `json:"gas_mv"`
	PIR       bool         `json:"pir"`
	PIREvents int          `json:"pir_events"`
	Status    SensorStatus `json:"status"`
}

func (t *Telemetry) Validate(topicDevice string) error {
	if t.V != 1 {
		return fmt.Errorf("unsupported format version %d", t.V)
	}
	if t.DeviceID != topicDevice {
		return fmt.Errorf("device_id %q does not match topic device %q", t.DeviceID, topicDevice)
	}
	switch t.Status.DHT {
	case "ok":
		if t.TempC == nil || t.HumPct == nil {
			return errors.New("dht ok but temp_c/hum_pct missing")
		}
		if !inRange(*t.TempC, -40, 85) || !inRange(*t.HumPct, 0, 100) {
			return errors.New("temp_c/hum_pct out of the DHT22 range")
		}
	case "error":
		if t.TempC != nil || t.HumPct != nil {
			return errors.New("dht error but temp_c/hum_pct present")
		}
	default:
		return fmt.Errorf("invalid status.dht %q", t.Status.DHT)
	}
	if t.GasMv == nil || *t.GasMv < 0 || *t.GasMv > 5500 {
		return errors.New("gas_mv missing or out of range")
	}
	if t.PIREvents < 0 || t.PIREvents > 10000 {
		return errors.New("pir_events out of range")
	}
	return nil
}

// Event is published by a node on vigil8/<id>/event.
type Event struct {
	V        int    `json:"v"`
	DeviceID string `json:"device_id"`
	Seq      uint32 `json:"seq"`
	UptimeMs uint64 `json:"uptime_ms"`
	Type     string `json:"type"`
	State    bool   `json:"state"`
}

func (e *Event) Validate(topicDevice string) error {
	if e.V != 1 {
		return fmt.Errorf("unsupported format version %d", e.V)
	}
	if e.DeviceID != topicDevice {
		return fmt.Errorf("device_id %q does not match topic device %q", e.DeviceID, topicDevice)
	}
	if e.Type != "motion" {
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	return nil
}

// DeviceStatus is the retained message on vigil8/<id>/status (LWT when offline).
type DeviceStatus struct {
	Online bool   `json:"online"`
	FW     string `json:"fw,omitempty"`
	IP     string `json:"ip,omitempty"`
}

func (s *DeviceStatus) Validate() error {
	if len(s.FW) > 32 || len(s.IP) > 45 {
		return errors.New("fw/ip too long")
	}
	return nil
}

// Alert levels: a first warning when an anomaly pattern starts, then a
// confirmation if it keeps evolving. Only "confirmed" acts on the node.
const (
	LevelWarning   = "warning"
	LevelConfirmed = "confirmed"
)

// AlertInput is the body of POST /api/v1/alerts (AI services or operator).
type AlertInput struct {
	Source     string          `json:"source"`
	Type       string          `json:"type"`
	Level      string          `json:"level"`
	DeviceID   string          `json:"device_id"`
	Confidence *float64        `json:"confidence"`
	Details    string          `json:"details"`
	Data       json.RawMessage `json:"data"`
}

func (a *AlertInput) Validate() error {
	if !wordPattern.MatchString(a.Source) {
		return errors.New("source: 1-32 chars [a-z0-9_-]")
	}
	a.Type = strings.ToLower(a.Type)
	if !wordPattern.MatchString(a.Type) {
		return errors.New("type: 1-32 chars [a-z0-9_-]")
	}
	if a.Level == "" {
		a.Level = LevelWarning
	}
	if a.Level != LevelWarning && a.Level != LevelConfirmed {
		return errors.New(`level: "warning" or "confirmed"`)
	}
	if a.DeviceID != "" && !deviceIDPattern.MatchString(a.DeviceID) {
		return errors.New("device_id: invalid")
	}
	if a.Confidence != nil && !inRange(*a.Confidence, 0, 1) {
		return errors.New("confidence: between 0 and 1")
	}
	if len(a.Details) > 500 {
		return errors.New("details: 500 chars max")
	}
	if len(a.Data) > 2048 {
		return errors.New("data: 2 KB max")
	}
	if len(a.Data) > 0 && !json.Valid(a.Data) {
		return errors.New("data: invalid JSON")
	}
	return nil
}

// AnnotationInput marks a test period, excluded from training and used to
// evaluate the model (see IA_Predictions/exporter_couchdb.py).
type AnnotationInput struct {
	DeviceID string `json:"device_id"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Label    string `json:"label"`
	Note     string `json:"note"`
}

func (a *AnnotationInput) Validate() error {
	if !deviceIDPattern.MatchString(a.DeviceID) {
		return errors.New("device_id: invalid")
	}
	if a.Start <= 0 || a.End <= a.Start {
		return errors.New("start/end: epoch ms with start < end")
	}
	if a.End-a.Start > 24*3600*1000 {
		return errors.New("an annotation can cover 24 h at most")
	}
	if !wordPattern.MatchString(a.Label) {
		return errors.New("label: 1-32 chars [a-z0-9_-]")
	}
	if len(a.Note) > 300 {
		return errors.New("note: 300 chars max")
	}
	return nil
}

// CommandInput is the body of POST /api/v1/devices/{id}/command.
type CommandInput struct {
	Strobe    bool `json:"strobe"`
	DurationS int  `json:"duration_s"`
}

func (c *CommandInput) Validate() error {
	if c.DurationS == 0 {
		c.DurationS = 5
	}
	if c.DurationS < 1 || c.DurationS > 60 {
		return errors.New("duration_s: 1 to 60")
	}
	return nil
}

func inRange(v, min, max float64) bool {
	return !math.IsNaN(v) && v >= min && v <= max
}
