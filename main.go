package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

// Config models
type Config struct {
	Port         string
	MqttBroker   string
	CouchdbURL   string
	CouchdbUser  string
	CouchdbPass  string
	SqliteDBPath string
}

// TelemetryPayload represents sensor readings from ESP32
type TelemetryPayload struct {
	DeviceID  string  `json:"device_id"`
	Timestamp int64   `json:"timestamp"`
	Temp      float64 `json:"temp"`
	Hum       float64 `json:"hum"`
	Gas       int     `json:"gas"`
	Motion    bool    `json:"motion"`
}

// AlertPayload represents alerts from IA (YOLO / Isolation Forest) or Sensors
type AlertPayload struct {
	ID         int64     `json:"id,omitempty"`
	Source     string    `json:"source"`
	Type       string    `json:"type"`
	Confidence float64   `json:"confidence,omitempty"`
	ThreatRank string    `json:"threat_rank,omitempty"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
	Details    string    `json:"details,omitempty"`
}

// ActuatorCommand represents orders sent to ESP32
type ActuatorCommand struct {
	Buzzer    bool `json:"buzzer"`
	Strobe    bool `json:"strobe"`
	DurationS int  `json:"duration_seconds,omitempty"`
}

// WebSocket Message envelope
type WSMessage struct {
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// WebSocket Hub
type WSHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan WSMessage
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func newWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan WSMessage, 100),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *WSHub) run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()
			log.Printf("[WebSocket] Client connected (%d active)", len(h.clients))

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
			}
			h.mu.Unlock()
			log.Printf("[WebSocket] Client disconnected (%d active)", len(h.clients))

		case message := <-h.broadcast:
			msgBytes, err := json.Marshal(message)
			if err != nil {
				continue
			}
			h.mu.Lock()
			for conn := range h.clients {
				err := conn.WriteMessage(websocket.TextMessage, msgBytes)
				if err != nil {
					conn.Close()
					delete(h.clients, conn)
				}
			}
			h.mu.Unlock()
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for workshop dev
	},
}

// App Server State
type ServerState struct {
	cfg             Config
	db              *sql.DB
	mqttClient      mqtt.Client
	wsHub           *WSHub
	latestTelemetry TelemetryPayload
	telemetryMu     sync.RWMutex
	actuators       ActuatorCommand
	actuatorsMu     sync.RWMutex
}

func main() {
	log.Println("=========================================================")
	log.Println("  AETHERCORP // VIG1L-8 - Core Tactical Edge Server (Go)")
	log.Println("=========================================================")

	cfg := Config{
		Port:         getEnv("PORT", "5000"),
		MqttBroker:   getEnv("MQTT_BROKER", "tcp://localhost:1883"),
		CouchdbURL:   getEnv("COUCHDB_URL", "http://localhost:5984"),
		CouchdbUser:  getEnv("COUCHDB_USER", "admin"),
		CouchdbPass:  getEnv("COUCHDB_PASSWORD", "sentinelpass"),
		SqliteDBPath: getEnv("SQLITE_PATH", "sentinel.db"),
	}

	// 1. Initialize SQLite Database
	db, err := initSQLite(cfg.SqliteDBPath)
	if err != nil {
		log.Fatalf("[SQLite] Failed to initialize database: %v", err)
	}
	defer db.Close()
	log.Printf("[SQLite] Connected and migrated successfully -> %s", cfg.SqliteDBPath)

	// 2. Initialize WebSocket Hub
	wsHub := newWSHub()
	go wsHub.run()

	state := &ServerState{
		cfg:   cfg,
		db:    db,
		wsHub: wsHub,
		latestTelemetry: TelemetryPayload{
			DeviceID:  "VIG1L-8-SIM",
			Timestamp: time.Now().Unix(),
			Temp:      24.2,
			Hum:       46.0,
			Gas:       18,
			Motion:    false,
		},
	}

	// 3. Connect to MQTT (Mosquitto) asynchronously in background
	go state.connectMQTT()

	// 4. Setup HTTP Router with Chi & CORS
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	// API Routes
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"service": "AetherCorp Sentinel-X VIG1L-8 Backend",
			"status":  "ONLINE",
			"runtime": "Go 1.26",
		})
	})

	r.Get("/api/v1/health", state.handleHealth)
	r.Get("/api/v1/telemetry/latest", state.handleGetLatestTelemetry)
	r.Post("/api/v1/alerts", state.handlePostAlerts)
	r.Post("/api/v1/commands/actuate", state.handlePostActuate)
	r.Get("/api/v1/alerts/history", state.handleGetAlertsHistory)
	r.Get("/ws", state.handleWebSocket)

	serverAddr := ":" + cfg.Port
	log.Printf("[HTTP] Tactical API & WebSocket listening on http://localhost%s", serverAddr)
	if err := http.ListenAndServe(serverAddr, r); err != nil {
		log.Fatalf("[HTTP] Server terminated: %v", err)
	}
}

// SQLite Initialization
func initSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS alerts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		source TEXT NOT NULL,
		type TEXT NOT NULL,
		confidence REAL,
		threat_rank TEXT,
		details TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS system_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		category TEXT NOT NULL,
		level TEXT NOT NULL,
		message TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

// MQTT Connection & Subscription Handler
func (s *ServerState) connectMQTT() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.cfg.MqttBroker)
	opts.SetClientID("vigil8_backend_go")
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("[MQTT] Successfully connected to broker %s", s.cfg.MqttBroker)
		// Subscribe to telemetry topic
		topic := "vigil8/sensors/telemetry"
		if token := c.Subscribe(topic, 1, s.onTelemetryReceived); token.Wait() && token.Error() != nil {
			log.Printf("[MQTT] Failed to subscribe to %s: %v", topic, token.Error())
		} else {
			log.Printf("[MQTT] Subscribed to topic: %s", topic)
		}
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("[MQTT] Connection lost: %v. Retrying in background...", err)
	})

	client := mqtt.NewClient(opts)
	s.mqttClient = client
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("[MQTT] Broker not reachable immediately (%v). Reconnect active in background.", token.Error())
	}
}

// On Telemetry Received from ESP32
func (s *ServerState) onTelemetryReceived(client mqtt.Client, msg mqtt.Message) {
	var payload TelemetryPayload
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		log.Printf("[MQTT] Warning: Malformed JSON telemetry: %v", err)
		return
	}

	if payload.Timestamp == 0 {
		payload.Timestamp = time.Now().Unix()
	}

	// Update in-memory latest
	s.telemetryMu.Lock()
	s.latestTelemetry = payload
	s.telemetryMu.Unlock()

	// Broadcast to WebSocket clients
	s.wsHub.broadcast <- WSMessage{
		Type:      "TELEMETRY_UPDATE",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      payload,
	}

	// Asynchronously save to CouchDB (as requested by teammate)
	go s.saveToCouchDB(payload)
}

// Push to CouchDB NoSQL Document
func (s *ServerState) saveToCouchDB(payload TelemetryPayload) {
	docID := fmt.Sprintf("read_%d", payload.Timestamp)
	couchURL := fmt.Sprintf("%s/telemetry/%s", s.cfg.CouchdbURL, docID)

	docBody, _ := json.Marshal(map[string]interface{}{
		"_id":       docID,
		"timestamp": payload.Timestamp,
		"temp":      payload.Temp,
		"hum":       payload.Hum,
		"gas":       payload.Gas,
		"motion":    payload.Motion,
		"device_id": payload.DeviceID,
	})

	req, err := http.NewRequest("PUT", couchURL, bytes.NewBuffer(docBody))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(s.cfg.CouchdbUser, s.cfg.CouchdbPass)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Non-blocking warning if CouchDB isn't up yet
		return
	}
	defer resp.Body.Close()
}

// Mandatory Endpoint: POST /api/v1/alerts (Page 3 of the subject)
func (s *ServerState) handlePostAlerts(w http.ResponseWriter, r *http.Request) {
	var alert AlertPayload
	if err := json.NewDecoder(r.Body).Decode(&alert); err != nil {
		http.Error(w, `{"error": "Invalid JSON format"}`, http.StatusBadRequest)
		return
	}

	alert.Timestamp = time.Now()
	if alert.ThreatRank == "" {
		alert.ThreatRank = "CRITICAL"
	}

	// 1. Insert into SQLite
	res, err := s.db.Exec(
		"INSERT INTO alerts (source, type, confidence, threat_rank, details) VALUES (?, ?, ?, ?, ?)",
		alert.Source, alert.Type, alert.Confidence, alert.ThreatRank, alert.Details,
	)
	if err == nil {
		alert.ID, _ = res.LastInsertId()
	}

	log.Printf("[ALERT] Registered from %s: type=%s confidence=%.2f rank=%s",
		alert.Source, alert.Type, alert.Confidence, alert.ThreatRank)

	// 2. Broadcast immediately over WebSocket
	s.wsHub.broadcast <- WSMessage{
		Type:      "ALERT_TRIGGERED",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      alert,
	}

	// 3. Automated Reactive Hardware Escalation: Trigger physical buzzer on ESP32
	if alert.Type == "INTRUSION" || alert.Type == "GAS_LEAK" {
		go s.publishActuatorCommand(ActuatorCommand{Buzzer: true, Strobe: true, DurationS: 8})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "RECORDED",
		"alert_id": alert.ID,
		"action":   "ESCALATION_TRIGGERED",
	})
}

// POST /api/v1/commands/actuate - Trigger actuators from Dashboard
func (s *ServerState) handlePostActuate(w http.ResponseWriter, r *http.Request) {
	var cmd ActuatorCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, `{"error": "Invalid JSON command"}`, http.StatusBadRequest)
		return
	}

	s.actuatorsMu.Lock()
	s.actuators = cmd
	s.actuatorsMu.Unlock()

	// Publish to MQTT
	go s.publishActuatorCommand(cmd)

	// Notify WebSockets
	s.wsHub.broadcast <- WSMessage{
		Type:      "ACTUATORS_STATE",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      cmd,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "DISPATCHED",
		"command": cmd,
	})
}

// Publish MQTT Actuator Command to ESP32
func (s *ServerState) publishActuatorCommand(cmd ActuatorCommand) {
	if s.mqttClient == nil || !s.mqttClient.IsConnected() {
		log.Println("[MQTT] Actuator command buffered (broker offline)")
		return
	}

	cmdBytes, _ := json.Marshal(cmd)
	topic := "vigil8/commands/actuators"
	token := s.mqttClient.Publish(topic, 1, false, cmdBytes)
	token.Wait()
	log.Printf("[MQTT] Dispatched actuator command to topic %s: %s", topic, string(cmdBytes))
}

// GET /api/v1/telemetry/latest
func (s *ServerState) handleGetLatestTelemetry(w http.ResponseWriter, r *http.Request) {
	s.telemetryMu.RLock()
	payload := s.latestTelemetry
	s.telemetryMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

// GET /api/v1/alerts/history
func (s *ServerState) handleGetAlertsHistory(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query("SELECT id, source, type, confidence, threat_rank, details, created_at FROM alerts ORDER BY id DESC LIMIT 50")
	if err != nil {
		http.Error(w, `{"error": "Database query failure"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var alerts []AlertPayload
	for rows.Next() {
		var a AlertPayload
		var createdAt string
		rows.Scan(&a.ID, &a.Source, &a.Type, &a.Confidence, &a.ThreatRank, &a.Details, &createdAt)
		alerts = append(alerts, a)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alerts)
}

// GET /api/v1/health
func (s *ServerState) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mqttOk := s.mqttClient != nil && s.mqttClient.IsConnected()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "HEALTHY",
		"uptime_s":    time.Since(startTime).Seconds(),
		"mqtt_broker": mqttOk,
		"database":    "SQLITE_ONLINE",
	})
}

var startTime = time.Now()

// WebSocket Handler
func (s *ServerState) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WebSocket] Upgrade error: %v", err)
		return
	}

	s.wsHub.register <- conn

	// Send initial greeting state
	s.telemetryMu.RLock()
	latest := s.latestTelemetry
	s.telemetryMu.RUnlock()

	initMsg := WSMessage{
		Type:      "INITIAL_STATE",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data: map[string]interface{}{
			"latest_telemetry": latest,
			"status":           "CONNECTED",
		},
	}
	conn.WriteJSON(initMsg)

	// Keep alive read loop
	go func() {
		defer func() {
			s.wsHub.unregister <- conn
		}()
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
