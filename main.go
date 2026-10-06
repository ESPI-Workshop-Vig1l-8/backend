package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.Println("AETHERCORP // SENTINEL-X VIG1L-8 backend")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("[CONFIG] %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	couch := NewCouch(cfg.CouchURL, cfg.CouchUser, cfg.CouchPassword)
	waitForCouch(ctx, couch)

	telemetry := NewBatchWriter(couch, "telemetry")
	events := NewBatchWriter(couch, "events")
	writersCtx, stopWriters := context.WithCancel(context.Background())
	writersDone := make(chan struct{}, 2)
	for _, w := range []*BatchWriter{telemetry, events} {
		go func(w *BatchWriter) { w.Run(writersCtx); writersDone <- struct{}{} }(w)
	}

	ids := NewIDGen()
	state := NewState()
	hub := NewHub(cfg.AllowedOrigins)
	ingest := NewIngestor(cfg, telemetry, events, ids, state, hub)
	ingest.Connect()

	api := &API{cfg: cfg, couch: couch, ids: ids, state: state, hub: hub, ingest: ingest,
		telemetry: telemetry, events: events, started: time.Now()}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.Router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	go func() {
		log.Printf("[HTTP] Listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[HTTP] %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down: closing HTTP, MQTT, then flushing the CouchDB queues")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	ingest.Disconnect()
	stopWriters()
	<-writersDone
	<-writersDone
	log.Println("Stopped")
}

// waitForCouch blocks until CouchDB accepts our credentials, so that no
// message is ingested without a place to store it.
func waitForCouch(ctx context.Context, couch *Couch) {
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := couch.Up(pingCtx)
		cancel()
		if err == nil {
			log.Println("[CouchDB] Connected")
			return
		}
		log.Printf("[CouchDB] Not ready (%v), retrying in 3 s", err)
		select {
		case <-ctx.Done():
			log.Fatal("Interrupted while waiting for CouchDB")
		case <-time.After(3 * time.Second):
		}
	}
}
