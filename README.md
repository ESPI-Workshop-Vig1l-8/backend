# Sentinel-X — backend (Go)

Central service of the "PC Serveur Local":

- subscribes to every node over MQTT, **validates** each message (format v1, `device_id` must match the topic) and stores it **unchanged** in CouchDB with `_id` and `received_at` (batched `_bulk_docs` writes, retried if CouchDB is down);
- keeps the live state of each node and pushes every message to the dashboard over **WebSocket**;
- exposes the **REST API** used by the dashboard and the AI services (alerts, history, annotations, commands).

```
ESP32 ─MQTTS─► Mosquitto ─► backend ─► CouchDB (telemetry, events)
                               │
                   REST + WebSocket (token)
                               │
        dashboard (nginx proxy /api, /ws) · IA_Vision · IA_Predictions
```

The node data format is documented in the `infra` README.

## Data and credentials

### Where the data lives

| Data | Where | Document `_id` |
|---|---|---|
| Telemetry (every 2 s) | CouchDB `telemetry` | `<device_id>:<received_at>` |
| Motion events, status changes, commands sent | CouchDB `events` | `<device_id>:<received_at>` |
| Alerts | CouchDB `events` | `alert:<received_at>` |
| Annotations (test periods) | CouchDB `events` | `annotation:<start>` |
| Live state of the nodes | backend memory only | — |

- CouchDB is the **only store** (volume `history-data` of the infra stack). The backend has no volume: after a restart, the live state is rebuilt within seconds from the broker's retained `status` messages and the next telemetry.
- `<received_at>` is the server time in epoch ms on 13 digits, so ids sort by time; it is bumped by 1 ms if needed so ids stay unique.
- If CouchDB is down, messages wait in memory (up to 20,000 documents per database, ~11 h for one node) and are retried every 2 s; the queue is also flushed on a clean shutdown (`SIGTERM`).
- Why no SQLite any more: one store means one set of access rules, one volume to back up, and the AI can read alerts and annotations next to the measurements (`IA_Predictions/exporter_couchdb.py`).

### Where the credentials come from

All secrets are environment variables, set by `infra/docker-compose.yaml` from `infra/.env` (git-ignored). There are **no default secrets**: the backend refuses to start if one is missing.

| Variable | Origin |
|---|---|
| `MQTT_PASSWORD` | `MQTT_BACKEND_PASSWORD` in `infra/.env`; hashed into Mosquitto's `passwd` by `infra/scripts/mqtt-passwd.sh` |
| `COUCHDB_PASSWORD` | `COUCHDB_BACKEND_PASSWORD` in `infra/.env`; the CouchDB user `backend` (role `writer`) is created by the `history-db-init` job |
| `API_OPERATOR_TOKEN` / `API_SERVICE_TOKEN` | same names in `infra/.env` (16+ chars, different, e.g. `openssl rand -hex 24`) |

## Authentication

Every endpoint except `GET /api/v1/health` requires `Authorization: Bearer <token>`. The WebSocket also accepts `?token=` (browsers can't set headers on it).

| Token | Who | Rights |
|---|---|---|
| `API_SERVICE_TOKEN` | AI services (vision, prediction) | every `GET` + `POST /api/v1/alerts` |
| `API_OPERATOR_TOKEN` | dashboard (operator) | everything |

Errors are JSON `{"error": "<message>"}`:

| Status | When |
|---|---|
| 400 | invalid JSON, unknown field, value out of range |
| 401 | missing or invalid token |
| 403 | service token on an operator endpoint, or foreign browser origin on `/ws` |
| 404 | unknown device or alert |
| 409 | alert modified meanwhile (retry) |
| 429 | more than 20 requests/s from the same IP (burst 40) |
| 502 | CouchDB unavailable |
| 503 | MQTT broker not connected (commands) |

Request bodies: JSON, 16 KB max, unknown fields refused. Times are epoch milliseconds.

## Endpoints

| Method | Path | Role |
|---|---|---|
| GET | [`/api/v1/health`](#get-apiv1health) | — |
| GET | [`/api/v1/session`](#get-apiv1session) | service |
| GET | [`/api/v1/devices`](#get-apiv1devices) | service |
| GET | [`/api/v1/devices/{id}/telemetry`](#get-apiv1devicesidtelemetry) | service |
| GET | [`/api/v1/devices/{id}/events`](#get-apiv1devicesidevents) | service |
| POST | [`/api/v1/devices/{id}/command`](#post-apiv1devicesidcommand) | operator |
| GET | [`/api/v1/alerts`](#get-apiv1alerts) | service |
| POST | [`/api/v1/alerts`](#post-apiv1alerts) | service |
| POST | [`/api/v1/alerts/{id}/ack`](#post-apiv1alertsidack) | operator |
| GET | [`/api/v1/annotations`](#get-apiv1annotations) | service |
| POST | [`/api/v1/annotations`](#post-apiv1annotations) | operator |
| GET | [`/ws`](#get-ws) | service |

In the examples, `$B` is the base URL (`http://<server>:10443` through the dashboard, or `http://backend:5000` from a container) and `$TOKEN` an API token.

### GET /api/v1/health

No authentication (used by the Docker health check). `200` when MQTT and CouchDB are up, `503` otherwise.

```bash
curl $B/api/v1/health
```
```json
{"status":"ok","mqtt":true,"couchdb":true,"uptime_s":74,"queued":{"telemetry":0,"events":0},"rejected":0,"ws":1}
```

- `queued`: documents waiting to be written to CouchDB.
- `rejected`: MQTT messages refused by the validation since start (malformed, wrong format, `device_id` not matching the topic).
- `ws`: connected WebSocket clients.

### GET /api/v1/session

Role of the token, used by the dashboard login.

```json
{"role":"operator"}
```

### GET /api/v1/devices

Live state of every node seen since the backend started, sorted by id.

```json
[
  {
    "device_id": "VIG1L-8-NODE04",
    "online": true,
    "stale": false,
    "fw": "0.2.0",
    "ip": "192.168.10.20",
    "last_seen": 1791290282100,
    "motion": false,
    "telemetry": { "_id": "VIG1L-8-NODE04:1791290282100", "received_at": 1791290282100, "v": 1, "device_id": "VIG1L-8-NODE04", "seq": 14, "temp_c": 25.9, "hum_pct": 60.4, "gas_mv": 262, "...": "..." }
  }
]
```

- `online`: from the node's `status` topic (`false` when the broker publishes its LWT).
- `stale`: no message for more than 10 s (telemetry is sent every 2 s).
- `telemetry`: last telemetry document, as stored.

### GET /api/v1/devices/{id}/telemetry

History of a node, for the graphs and analysis.

| Query | Default | |
|---|---|---|
| `from` | `to` − 1 h | epoch ms |
| `to` | now | epoch ms |
| `step` | `raw` | `raw` (range ≤ 6 h), `minute` (≤ 7 days) or `hour` (≤ 90 days) |

```bash
curl -H "Authorization: Bearer $TOKEN" "$B/api/v1/devices/VIG1L-8-NODE04/telemetry?from=1791290160000&to=1791290282000"
```

`step=raw`: the stored readings. `temp_c`/`hum_pct` are `null` when the DHT22 failed.

```json
{
  "device_id": "VIG1L-8-NODE04", "step": "raw", "from": 1791290160000, "to": 1791290282000,
  "points": [
    {"t": 1791290246806, "temp_c": 25.5, "hum_pct": 61, "gas_mv": 255, "pir": false, "env_warn": false, "gas_warm": true}
  ]
}
```

`step=minute` / `hour`: averages, with min and max, from the CouchDB views (UTC buckets; `t` is the start of the bucket).

```json
{
  "device_id": "VIG1L-8-NODE04", "step": "minute", "from": 1791286682228, "to": 1791290282228,
  "points": [
    {"t": 1791290280000, "temp_c": 25.75, "hum_pct": 60.27, "gas_mv": 260,
     "min": {"temp_c": 25.6, "hum_pct": 59.9, "gas_mv": 255},
     "max": {"temp_c": 25.9, "hum_pct": 60.8, "gas_mv": 265}}
  ]
}
```

### GET /api/v1/devices/{id}/events

Latest stored events of a node, newest first: motion (`type: "motion"`), status changes (`"status"`) and commands sent (`"command"`).

| Query | Default | |
|---|---|---|
| `limit` | 50 | 1 to 500 |

```json
[
  {"_id": "VIG1L-8-NODE04:1791290306342", "received_at": 1791290306342, "device_id": "VIG1L-8-NODE04", "type": "command", "strobe": true, "duration_s": 5, "source": "operator"},
  {"_id": "VIG1L-8-NODE04:1791290288010", "received_at": 1791290288010, "v": 1, "device_id": "VIG1L-8-NODE04", "seq": 3, "uptime_ms": 228000, "type": "motion", "state": true}
]
```

### POST /api/v1/devices/{id}/command

Makes the node's environment LED blink (or stops it). Published on `vigil8/<id>/cmd` (QoS 1) and recorded as a `command` event. The node must have been seen since the backend started (`404` otherwise).

| Field | | |
|---|---|---|
| `strobe` | bool | `true` to blink, `false` to stop |
| `duration_s` | int, 1–60 | default 5 |

```bash
curl -H "Authorization: Bearer $OPERATOR_TOKEN" -X POST -d '{"strobe":true,"duration_s":5}' $B/api/v1/devices/VIG1L-8-NODE04/command
```
`202`:
```json
{"_id": "VIG1L-8-NODE04:1791290306342", "received_at": 1791290306342, "device_id": "VIG1L-8-NODE04", "type": "command", "strobe": true, "duration_s": 5, "source": "operator"}
```

### GET /api/v1/alerts

Latest alerts, newest first.

| Query | Default | |
|---|---|---|
| `limit` | 50 | 1 to 200 |

Returns a list of alert documents (see below).

### POST /api/v1/alerts

Mandatory endpoint of the subject, called by the AI services (service token) or the operator.

| Field | | |
|---|---|---|
| `source` | required, 1–32 chars `[a-z0-9_-]` | e.g. `ia-vision`, `ia-prediction` |
| `type` | required, 1–32 chars `[a-z0-9_-]` (lower-cased) | e.g. `intrusion`, `anomaly` |
| `level` | `warning` (default) or `confirmed` | |
| `device_id` | optional | node concerned |
| `confidence` | optional, 0 to 1 | |
| `details` | optional, ≤ 500 chars | |
| `data` | optional, any JSON ≤ 2 KB | e.g. bounding box, anomaly score |

Alert levels:

- `warning`: an anomaly pattern **begins**. Recorded and pushed to the dashboard.
- `confirmed`: the pattern **keeps evolving**. Recorded, pushed, and if `device_id` is set, the node's environment LED blinks for `ALERT_STROBE_S` seconds (default 30).

```bash
curl -H "Authorization: Bearer $SERVICE_TOKEN" -X POST $B/api/v1/alerts -d '{
  "source": "ia-vision", "type": "intrusion", "level": "confirmed",
  "device_id": "VIG1L-8-NODE04", "confidence": 0.94, "data": {"bbox": [218, 84, 142, 280]}
}'
```
`201`:
```json
{"id": "alert:1791290306299", "level": "confirmed", "action": "recorded, LED blinking on VIG1L-8-NODE04"}
```

Stored document (as returned by `GET /api/v1/alerts`). The `type` of the request becomes `category`; `type` is always `"alert"`:

```json
{
  "_id": "alert:1791290306299", "received_at": 1791290306299, "type": "alert",
  "category": "intrusion", "level": "confirmed", "source": "ia-vision",
  "device_id": "VIG1L-8-NODE04", "confidence": 0.94, "details": "",
  "data": {"bbox": [218, 84, 142, 280]}, "acked_at": null
}
```

### POST /api/v1/alerts/{id}/ack

Acknowledges an alert (`acked_at` set to now). Acknowledging twice returns the alert unchanged.

```bash
curl -H "Authorization: Bearer $OPERATOR_TOKEN" -X POST $B/api/v1/alerts/alert:1791290306299/ack
```
`200`: the updated alert document.

### GET /api/v1/annotations

Latest 100 test periods, newest first.

| Query | | |
|---|---|---|
| `device` | optional | only this node |

### POST /api/v1/annotations

Marks a test period (lighter, hot air…). `IA_Predictions/exporter_couchdb.py` excludes these periods from training and can keep them to evaluate the model.

| Field | | |
|---|---|---|
| `device_id` | required | |
| `start`, `end` | required, epoch ms | `start < end`, 24 h max |
| `label` | required, 1–32 chars `[a-z0-9_-]` | e.g. `gas_test`, `heat_test`, `motion_test` |
| `note` | optional, ≤ 300 chars | |

```bash
curl -H "Authorization: Bearer $OPERATOR_TOKEN" -X POST $B/api/v1/annotations -d '{
  "device_id": "VIG1L-8-NODE04", "start": 1791290243969, "end": 1791290303969, "label": "gas_test", "note": "lighter, not lit, 5 cm"
}'
```
`201`:
```json
{"_id": "annotation:1791290243969", "type": "annotation", "device_id": "VIG1L-8-NODE04", "start": 1791290243969, "end": 1791290303969, "label": "gas_test", "note": "lighter, not lit, 5 cm", "created_at": 1791290309464}
```

### GET /ws

WebSocket: `ws://<server>:10443/ws?token=<token>` (`wss://` behind HTTPS). Browsers must connect from the same origin (or one listed in `ALLOWED_ORIGINS`). The server pings every 30 s; messages from the client are ignored (commands go through the REST API).

Every message: `{"type": "...", "ts": <epoch ms>, "data": ...}`

| `type` | `data` |
|---|---|
| `snapshot` | first message: `{"devices": [<state>…], "alerts": [<20 latest alerts>]}` |
| `telemetry` | stored telemetry document |
| `event` | stored motion event |
| `status` | node online/offline (`{"device_id", "online", "fw", "ip", ...}`) |
| `command` | command sent to a node |
| `alert` | new alert document |
| `alert_ack` | acknowledged alert document |
| `annotation` | new annotation document |

## Configuration

| Variable | Default | |
|---|---|---|
| `PORT` | `5000` | |
| `MQTT_BROKER` | `tcp://mqtt-broker:1883` | internal listener of the infra stack |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | `backend` / — | password required |
| `MQTT_CLIENT_ID` | `sentinel-backend` | one backend per broker: a second one with the same id kicks the first off |
| `COUCHDB_URL` | `http://history-db:5984` | |
| `COUCHDB_USER` / `COUCHDB_PASSWORD` | `backend` / — | password required |
| `API_OPERATOR_TOKEN` / `API_SERVICE_TOKEN` | — | required, ≥ 16 chars, different |
| `ALLOWED_ORIGINS` | — | extra browser origins (comma-separated); same origin is always allowed |
| `ALERT_STROBE_S` | `30` | LED blink duration for a confirmed alert (1–60) |

## Hardening

- Constant-time token comparison; tokens never logged (the request log omits the query string).
- Rate limit per IP, request body limit, unknown JSON fields refused, HTTP timeouts against slow clients (`ReadHeaderTimeout` 5 s).
- MQTT payloads limited to 4 KB; messages whose `device_id` doesn't match the topic are rejected and counted.
- Docker image: static binary, tests run during the build, unprivileged user, health check.

## Development

```bash
go test ./...
docker build -t sentinel/backend:dev .
```
