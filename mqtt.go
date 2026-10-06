package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Ingestor subscribes to every node, validates each message, stores it in
// CouchDB (unchanged, plus _id and received_at) and pushes it to the dashboard.
type Ingestor struct {
	client    mqtt.Client
	telemetry *BatchWriter
	events    *BatchWriter
	ids       *IDGen
	state     *State
	hub       *Hub
	rejected  atomic.Uint64
}

func NewIngestor(cfg Config, telemetry, events *BatchWriter, ids *IDGen, state *State, hub *Hub) *Ingestor {
	in := &Ingestor{telemetry: telemetry, events: events, ids: ids, state: state, hub: hub}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBroker).
		SetClientID(cfg.MQTTClientID).
		SetUsername(cfg.MQTTUsername).
		SetPassword(cfg.MQTTPassword).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetOrderMatters(false)
	opts.SetOnConnectHandler(in.onConnect)
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("[MQTT] Connection lost: %v (reconnecting)", err)
	})
	in.client = mqtt.NewClient(opts)
	return in
}

func (in *Ingestor) Connect() {
	in.client.Connect() // retries in the background (SetConnectRetry)
}

func (in *Ingestor) Connected() bool {
	return in.client.IsConnectionOpen()
}

func (in *Ingestor) Rejected() uint64 {
	return in.rejected.Load()
}

func (in *Ingestor) Disconnect() {
	in.client.Disconnect(500)
}

// Subscriptions are (re)done on every connection: the session is not persisted.
func (in *Ingestor) onConnect(c mqtt.Client) {
	log.Println("[MQTT] Connected")
	subs := map[string]byte{
		"vigil8/+/telemetry": 0,
		"vigil8/+/event":     1,
		"vigil8/+/status":    1,
	}
	if t := c.SubscribeMultiple(subs, in.onMessage); t.Wait() && t.Error() != nil {
		log.Printf("[MQTT] Subscribe failed: %v", t.Error())
	}
}

func (in *Ingestor) onMessage(_ mqtt.Client, msg mqtt.Message) {
	parts := strings.Split(msg.Topic(), "/")
	if len(parts) != 3 || parts[0] != "vigil8" || !deviceIDPattern.MatchString(parts[1]) {
		in.reject(msg.Topic(), errors.New("unexpected topic"))
		return
	}
	device, kind := parts[1], parts[2]
	if len(msg.Payload()) > 4096 {
		in.reject(msg.Topic(), errors.New("payload too large"))
		return
	}

	var err error
	switch kind {
	case "telemetry":
		err = in.handleTelemetry(device, msg.Payload())
	case "event":
		err = in.handleEvent(device, msg.Payload())
	case "status":
		err = in.handleStatus(device, msg.Payload(), msg.Retained())
	default:
		err = errors.New("unexpected topic")
	}
	if err != nil {
		in.reject(msg.Topic(), err)
	}
}

func (in *Ingestor) reject(topic string, err error) {
	in.rejected.Add(1)
	log.Printf("[MQTT] Rejected message on %s: %v", topic, err)
}

// decode validates the payload with the typed struct and keeps the raw map,
// so the stored document is exactly what the node sent.
func decode(payload []byte, typed any) (map[string]any, error) {
	if err := json.Unmarshal(payload, typed); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	raw := map[string]any{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return raw, nil
}

func (in *Ingestor) handleTelemetry(device string, payload []byte) error {
	var t Telemetry
	doc, err := decode(payload, &t)
	if err != nil {
		return err
	}
	if err := t.Validate(device); err != nil {
		return err
	}
	id, ts := in.ids.Next(device, time.Now().UnixMilli())
	doc["_id"], doc["received_at"] = id, ts
	in.telemetry.Add(doc)
	in.state.OnTelemetry(device, doc, t.PIR, ts)
	in.hub.Broadcast("telemetry", doc)
	return nil
}

func (in *Ingestor) handleEvent(device string, payload []byte) error {
	var e Event
	doc, err := decode(payload, &e)
	if err != nil {
		return err
	}
	if err := e.Validate(device); err != nil {
		return err
	}
	id, ts := in.ids.Next(device, time.Now().UnixMilli())
	doc["_id"], doc["received_at"] = id, ts
	in.events.Add(doc)
	in.state.OnMotion(device, e.State, ts)
	in.hub.Broadcast("event", doc)
	return nil
}

// Status changes are stored as events. The retained status received right
// after subscribing only refreshes the live state (already stored earlier).
func (in *Ingestor) handleStatus(device string, payload []byte, retained bool) error {
	var st DeviceStatus
	if _, err := decode(payload, &st); err != nil {
		return err
	}
	if err := st.Validate(); err != nil {
		return err
	}
	id, ts := in.ids.Next(device, time.Now().UnixMilli())
	in.state.OnStatus(device, st, ts)
	doc := map[string]any{
		"_id": id, "received_at": ts, "device_id": device,
		"type": "status", "online": st.Online, "fw": st.FW, "ip": st.IP,
	}
	if !retained {
		in.events.Add(doc)
	}
	in.hub.Broadcast("status", doc)
	return nil
}

// SendCommand publishes a command to a node and records it as an event.
func (in *Ingestor) SendCommand(device string, cmd CommandInput, source string) (map[string]any, error) {
	if !in.client.IsConnectionOpen() {
		return nil, errors.New("MQTT broker not connected")
	}
	payload, _ := json.Marshal(map[string]any{"strobe": cmd.Strobe, "duration_s": cmd.DurationS})
	t := in.client.Publish("vigil8/"+device+"/cmd", 1, false, payload)
	if !t.WaitTimeout(5*time.Second) || t.Error() != nil {
		return nil, fmt.Errorf("publish failed: %v", t.Error())
	}
	id, ts := in.ids.Next(device, time.Now().UnixMilli())
	doc := map[string]any{
		"_id": id, "received_at": ts, "device_id": device, "type": "command",
		"strobe": cmd.Strobe, "duration_s": cmd.DurationS, "source": source,
	}
	in.events.Add(doc)
	in.hub.Broadcast("command", doc)
	return doc, nil
}
