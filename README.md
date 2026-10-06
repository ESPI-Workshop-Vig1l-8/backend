# Sentinel-X — backend (Go)

Central service of the "PC Serveur Local":

- subscribes to every node over MQTT, **validates** each message (format v1, `device_id` must match the topic) and stores it **unchanged** in CouchDB with `_id` and `received_at` (batched `_bulk_docs` writes, retried if CouchDB is down);
- keeps the live state of each node and pushes every message to the dashboard over **WebSocket**;
- exposes the **REST API** used by the dashboard and the AI services (alerts, history, annotations, commands).

CouchDB is the only data store. The data format is documented in the `infra` README.

## API

All endpoints except `/api/v1/health` require `Authorization: Bearer <token>`.

| Token | Who | Rights |
|---|---|---|
| `API_SERVICE_TOKEN` | AI services (vision, prediction) | read + `POST /api/v1/alerts` |
| `API_OPERATOR_TOKEN` | dashboard (operator) | everything |

| Method | Path | Role | Description |
|---|---|---|---|
| GET | `/api/v1/health` | — | `{status, mqtt, couchdb, uptime_s, queued, rejected, ws}`; 503 when degraded |
| GET | `/api/v1/session` | service | `{role}`: `operator` or `service` |
| GET | `/api/v1/devices` | service | live state of every node (online, stale, last telemetry, motion) |
| GET | `/api/v1/devices/{id}/telemetry?from=&to=&step=` | service | history; `from`/`to` in epoch ms (default: last hour); `step=raw` (≤ 6 h), `minute` (≤ 7 d) or `hour` (≤ 90 d) with avg/min/max |
| GET | `/api/v1/devices/{id}/events?limit=` | service | latest events of a node: motion, status, commands |
| GET | `/api/v1/alerts?limit=` | service | latest alerts, newest first |
| POST | `/api/v1/alerts` | service | create an alert (see below) |
| POST | `/api/v1/alerts/{id}/ack` | operator | acknowledge an alert |
| GET | `/api/v1/annotations?device=` | service | test periods |
| POST | `/api/v1/annotations` | operator | `{device_id, start, end, label, note}` — marks a test period (excluded from training) |
| POST | `/api/v1/devices/{id}/command` | operator | `{strobe, duration_s}` — blinks the node's environment LED (1–60 s); recorded as an event |
| GET | `/ws?token=` | service | WebSocket (token in the query: browsers can't set headers) |

### Alerts — `POST /api/v1/alerts`

```json
{
  "source": "ia-prediction",
  "type": "anomaly",
  "level": "warning",
  "device_id": "VIG1L-8-NODE04",
  "confidence": 0.82,
  "details": "slow temperature rise with gas drift",
  "data": { "score": -0.12 }
}
```

- `level`: `warning` (an anomaly pattern begins) or `confirmed` (the pattern keeps evolving). Default `warning`.
- A `confirmed` alert with a `device_id` makes the node's environment LED blink for `ALERT_STROBE_S` seconds.
- `source` and `type`: 1–32 chars `[a-z0-9_-]`; `confidence` between 0 and 1; `details` ≤ 500 chars; `data` any JSON ≤ 2 KB.

### WebSocket messages

`{"type": "...", "ts": <epoch ms>, "data": ...}` with `type`:

| Type | Data |
|---|---|
| `snapshot` | first message: `{devices, alerts}` |
| `telemetry` | stored telemetry document |
| `event` | motion event |
| `status` | node online/offline |
| `command` | command sent to a node |
| `alert`, `alert_ack` | new / acknowledged alert |
| `annotation` | new annotation |

## Configuration

| Variable | Default | |
|---|---|---|
| `PORT` | `5000` | |
| `MQTT_BROKER` | `tcp://mqtt-broker:1883` | internal listener |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | `backend` / — | required |
| `COUCHDB_URL` | `http://history-db:5984` | |
| `COUCHDB_USER` / `COUCHDB_PASSWORD` | `backend` / — | required |
| `API_OPERATOR_TOKEN` / `API_SERVICE_TOKEN` | — | required, ≥ 16 chars, different |
| `ALLOWED_ORIGINS` | — | extra browser origins (comma-separated); same origin is always allowed |
| `ALERT_STROBE_S` | `30` | LED blink duration for a confirmed alert |

The backend refuses to start without credentials (no default secrets).

## Hardening

- Constant-time token comparison; tokens never logged (the request log omits the query string).
- Rate limit: 20 requests/s per IP (burst 40). Bodies limited to 16 KB, unknown JSON fields refused. HTTP timeouts against slow clients.
- MQTT payloads limited to 4 KB; messages whose `device_id` doesn't match the topic are rejected and counted (`rejected` in `/health`).
- Docker image: static binary, unprivileged user, health check.

## Development

```bash
go test ./...
docker build -t sentinel/backend:dev .
```
