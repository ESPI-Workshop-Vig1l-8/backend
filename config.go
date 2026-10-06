package main

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port           string
	MQTTBroker     string
	MQTTUsername   string
	MQTTPassword   string
	MQTTClientID   string
	CouchURL       string
	CouchUser      string
	CouchPassword  string
	OperatorToken  string   // dashboard: everything, including commands
	ServiceToken   string   // AI services: read + POST /api/v1/alerts
	AllowedOrigins []string // extra browser origins (same origin is always allowed)
	AlertStrobeS   int      // LED blink duration for a confirmed alert
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// LoadConfig reads the environment. No default secrets: the backend refuses
// to start without credentials and tokens.
func LoadConfig() (Config, error) {
	cfg := Config{
		Port:          getEnv("PORT", "5000"),
		MQTTBroker:    getEnv("MQTT_BROKER", "tcp://mqtt-broker:1883"),
		MQTTUsername:  getEnv("MQTT_USERNAME", "backend"),
		MQTTPassword:  os.Getenv("MQTT_PASSWORD"),
		MQTTClientID:  getEnv("MQTT_CLIENT_ID", "sentinel-backend"),
		CouchURL:      getEnv("COUCHDB_URL", "http://history-db:5984"),
		CouchUser:     getEnv("COUCHDB_USER", "backend"),
		CouchPassword: os.Getenv("COUCHDB_PASSWORD"),
		OperatorToken: os.Getenv("API_OPERATOR_TOKEN"),
		ServiceToken:  os.Getenv("API_SERVICE_TOKEN"),
	}
	for _, o := range strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			cfg.AllowedOrigins = append(cfg.AllowedOrigins, o)
		}
	}
	strobe, err := strconv.Atoi(getEnv("ALERT_STROBE_S", "30"))
	if err != nil || strobe < 1 || strobe > 60 {
		return cfg, errors.New("ALERT_STROBE_S: 1 to 60")
	}
	cfg.AlertStrobeS = strobe

	var missing []string
	for name, v := range map[string]string{
		"MQTT_PASSWORD": cfg.MQTTPassword, "COUCHDB_PASSWORD": cfg.CouchPassword,
		"API_OPERATOR_TOKEN": cfg.OperatorToken, "API_SERVICE_TOKEN": cfg.ServiceToken,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return cfg, errors.New("missing environment variables: " + strings.Join(missing, ", "))
	}
	if len(cfg.OperatorToken) < 16 || len(cfg.ServiceToken) < 16 {
		return cfg, errors.New("API tokens must be at least 16 characters")
	}
	if cfg.OperatorToken == cfg.ServiceToken {
		return cfg, errors.New("API_OPERATOR_TOKEN and API_SERVICE_TOKEN must differ")
	}
	return cfg, nil
}
