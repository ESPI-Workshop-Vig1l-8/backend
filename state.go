package main

import (
	"sort"
	"sync"
	"time"
)

// A node is considered stale when no message arrived for this long
// (telemetry is sent every 2 s).
const staleAfter = 10 * time.Second

// DeviceState is the live view of a node, sent to the dashboard.
type DeviceState struct {
	DeviceID  string         `json:"device_id"`
	Online    bool           `json:"online"`
	Stale     bool           `json:"stale"`
	FW        string         `json:"fw,omitempty"`
	IP        string         `json:"ip,omitempty"`
	LastSeen  int64          `json:"last_seen,omitempty"`
	Motion    bool           `json:"motion"`
	Telemetry map[string]any `json:"telemetry,omitempty"`
}

type State struct {
	mu      sync.RWMutex
	devices map[string]*DeviceState
}

func NewState() *State {
	return &State{devices: map[string]*DeviceState{}}
}

func (s *State) device(id string) *DeviceState {
	d, ok := s.devices[id]
	if !ok {
		d = &DeviceState{DeviceID: id}
		s.devices[id] = d
	}
	return d
}

func (s *State) OnTelemetry(id string, doc map[string]any, pir bool, receivedAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.device(id)
	d.Online = true
	d.LastSeen = receivedAt
	d.Telemetry = doc
	d.Motion = pir
}

func (s *State) OnMotion(id string, motion bool, receivedAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.device(id)
	d.LastSeen = receivedAt
	d.Motion = motion
}

func (s *State) OnStatus(id string, st DeviceStatus, receivedAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.device(id)
	d.Online = st.Online
	if st.Online {
		d.FW, d.IP = st.FW, st.IP
		d.LastSeen = receivedAt
	}
}

func (s *State) Known(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.devices[id]
	return ok
}

// Snapshot returns a copy of every device, sorted by id.
func (s *State) Snapshot() []DeviceState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UnixMilli()
	out := make([]DeviceState, 0, len(s.devices))
	for _, d := range s.devices {
		c := *d
		c.Stale = c.LastSeen == 0 || now-c.LastSeen > staleAfter.Milliseconds()
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	return out
}
