package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSMessage is the envelope of every message pushed to the dashboard.
type WSMessage struct {
	Type string `json:"type"`
	TS   int64  `json:"ts"`
	Data any    `json:"data"`
}

const (
	wsWriteWait  = 10 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = 30 * time.Second
	wsSendBuffer = 64
)

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
}

// Hub fans messages out to every connected dashboard. A client that cannot
// keep up is disconnected instead of slowing down the ingestion.
type Hub struct {
	mu       sync.Mutex
	clients  map[*wsClient]struct{}
	upgrader websocket.Upgrader
}

func NewHub(allowedOrigins []string) *Hub {
	h := &Hub{clients: map[*wsClient]struct{}{}}
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			return originAllowed(r, allowedOrigins)
		},
	}
	return h
}

// originAllowed accepts same-origin requests (dashboard served through the
// same host) and the explicitly configured origins.
func originAllowed(r *http.Request, allowed []string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // not a browser (curl, services)
	}
	if u, err := url.Parse(origin); err == nil && u.Host == r.Host {
		return true
	}
	for _, a := range allowed {
		if a == origin {
			return true
		}
	}
	return false
}

func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *Hub) Broadcast(msgType string, data any) {
	msg, err := json.Marshal(WSMessage{Type: msgType, TS: time.Now().UnixMilli(), Data: data})
	if err != nil {
		log.Printf("[WS] marshal %s: %v", msgType, err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			h.removeLocked(c)
		}
	}
}

func (h *Hub) removeLocked(c *wsClient) {
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(c)
}

// Serve upgrades the request and sends the initial snapshot first.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, snapshot any) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // the upgrader already answered
	}
	c := &wsClient{conn: conn, send: make(chan []byte, wsSendBuffer)}
	if first, err := json.Marshal(WSMessage{Type: "snapshot", TS: time.Now().UnixMilli(), Data: snapshot}); err == nil {
		c.send <- first
	}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	go h.writeLoop(c)
	h.readLoop(c)
}

func (h *Hub) writeLoop(c *wsClient) {
	ticker := time.NewTicker(wsPingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				h.remove(c)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.remove(c)
				return
			}
		}
	}
}

// readLoop only handles pongs and closing: the dashboard sends commands
// through the REST API, not the socket.
func (h *Hub) readLoop(c *wsClient) {
	defer h.remove(c)
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
