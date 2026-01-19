# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o /app/bin/evm_syncer ./cmd/syncer

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/evm_syncer /app/evm_syncer

# Copy config
COPY config/config.yaml /app/config/config.yaml

# Expose ports
EXPOSE 30303 9090

# Set entrypoint
ENTRYPOINT ["/app/evm_syncer"]
CMD ["-config", "/app/config/config.yaml"]
