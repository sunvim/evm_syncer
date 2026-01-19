# EVM Syncer Service

A production-ready, lightweight distributed EVM blockchain synchronization service that syncs block data from mainnet via P2P protocol and handles transaction broadcasting and receiving.

## Features

- **Multi-Chain Support**: Abstracted chain interface with BSC mainnet support (Ethereum ready)
- **P2P Network**: Connects to 30-50 peers using DHT Kademlia discovery
- **Full Sync Mode**: Downloads and validates blocks from genesis
- **Concurrent Processing**: 50 concurrent block downloads using worker pools
- **Parlia Consensus**: Simplified signature verification for BSC
- **Transaction Pool**: Shared pool with broadcasting and gossip protocol
- **Pika Storage**: High-performance Redis-compatible storage backend
- **Prometheus Metrics**: Complete observability with 15+ metrics
- **Graceful Shutdown**: Context-based lifecycle management

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   P2P Net   │─────▶│    Syncer    │─────▶│  Pika DB    │
│  (eth/66/67)│      │  Engine      │      │  Storage    │
└─────────────┘      └──────────────┘      └─────────────┘
      │                     │                      │
      │              ┌──────┴──────┐              │
      │              │   Worker    │              │
      └─────────────▶│   Pools     │◀─────────────┘
                     └─────────────┘
                            │
                     ┌──────┴──────┐
                     │  TxPool     │
                     │ Broadcaster │
                     └─────────────┘
```

## Quick Start

### Prerequisites

- Go 1.21+
- Pika/Redis server
- Docker (optional)

### Build

```bash
make build
```

### Run

```bash
# Start Pika
docker run -d -p 9221:9221 pikadb/pika

# Run syncer
./bin/evm_syncer -config config/config.yaml
```

### Docker

```bash
cd deployments/docker
docker-compose up -d
```

### Kubernetes

```bash
kubectl apply -f deployments/kubernetes/deployment.yaml
```

## Configuration

See `config/config.yaml` for all configuration options:

- **chain**: Chain-specific settings (name, network ID, genesis hash)
- **p2p**: Network settings (listen address, max peers, bootnodes)
- **storage**: Pika connection settings
- **sync**: Sync mode and performance tuning
- **worker_pools**: Worker pool configurations
- **metrics**: Prometheus metrics settings
- **logging**: Log level and format

## API Endpoints

- `GET /health` - Health check endpoint
- `GET /metrics` - Prometheus metrics

## Metrics

Key metrics exported:

- `sync_height` - Current synchronized block height
- `sync_lag` - Blocks behind network
- `sync_speed_bps` - Blocks per second
- `peer_count` - Connected peers
- `txpool_pending_count` - Pending transactions
- `worker_pool_active_workers` - Active workers per pool

## Development

```bash
# Install dependencies
make deps

# Run tests
make test

# Run linter
make lint

# Clean build artifacts
make clean
```

## Project Structure

```
evm_syncer/
├── cmd/syncer/          # Main entry point
├── pkg/
│   ├── chain/           # Chain abstraction layer
│   ├── p2p/             # P2P networking
│   ├── syncer/          # Block synchronization
│   ├── txpool/          # Transaction pool
│   ├── storage/         # Pika storage
│   ├── worker/          # Worker pools
│   ├── metrics/         # Prometheus metrics
│   ├── config/          # Configuration
│   └── logger/          # Logging
├── config/              # Configuration files
└── deployments/         # Docker & K8s manifests
```

## License

MIT
