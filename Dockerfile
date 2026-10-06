# Multi-stage Docker build for the VIG1L-8 backend
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Dependencies caching
COPY go.mod go.sum ./
RUN go mod download

# Static binary, tests run during the build
COPY . .
RUN go vet ./... && go test ./... && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o server .

# Minimal runtime image, unprivileged user
FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -H -u 10001 sentinel
WORKDIR /app
COPY --from=builder /app/server .
USER sentinel

# REST API + WebSocket (reached through the dashboard's reverse proxy)
EXPOSE 5000
ENV PORT=5000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s \
  CMD wget -q -O /dev/null http://127.0.0.1:5000/api/v1/health || exit 1

CMD ["./server"]
