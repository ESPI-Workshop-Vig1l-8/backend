# Multi-stage Docker build for VIG1L-8 Core Backend
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Dependencies caching
COPY go.mod go.sum ./
RUN go mod download

# Build statically linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o server main.go

# Minimalist production container
FROM alpine:3.19
WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/server .

# Expose HTTP REST & WebSocket port
EXPOSE 5000

ENV PORT=5000
ENV SQLITE_PATH=/data/sentinel.db
ENV MQTT_BROKER=tcp://mosquitto:1883
ENV COUCHDB_URL=http://couchdb:5984

CMD ["./server"]
