# Transaction Pool Components

This package implements the transaction pool components for an EVM blockchain syncer. It provides transaction validation, broadcasting, and receiving functionality with proper integration with Pika (Redis-compatible) storage and P2P networking.

## Components

### 1. Validator (`validator.go`)

The validator performs comprehensive transaction validation according to EVM network rules.

#### Features

- **Signature Verification**: Validates transaction signatures and recovers sender addresses
- **Size Validation**: Ensures transactions don't exceed 128KB limit
- **Gas Limit Validation**: Checks gas limits are within acceptable range (max 30M gas)
- **Gas Price Validation**: Ensures gas price is valid and non-zero
- **Chain ID Validation**: Verifies transaction chain ID matches the network
- **Batch Validation**: Supports efficient validation of multiple transactions
- **Quick Validation**: Fast pre-validation without expensive signature checks

#### Usage

```go
import (
    "math/big"
    "github.com/sunvim/evm_syncer/pkg/txpool"
    "github.com/sunvim/evm_syncer/pkg/logger"
)

// Create validator
chainID := big.NewInt(1) // Ethereum mainnet
validator := txpool.NewValidator(chainID, logger.Get())

// Validate a single transaction
from, err := validator.ValidateTransaction(tx)
if err != nil {
    // Handle validation error
    switch err {
    case txpool.ErrInvalidSignature:
        // Invalid signature
    case txpool.ErrTxTooLarge:
        // Transaction too large
    case txpool.ErrGasLimitExceeded:
        // Gas limit too high
    case txpool.ErrChainIDMismatch:
        // Wrong chain ID
    }
}

// Quick validation (without signature check)
err = validator.QuickValidate(tx)

// Batch validation
senders, errors := validator.ValidateBatch(txs)
```

### 2. Broadcaster (`broadcaster.go`)

The broadcaster monitors the Pika broadcast queue and broadcasts transactions to all connected P2P peers.

#### Features

- **Worker Pool**: Uses 10 concurrent workers for parallel processing
- **Queue Monitoring**: Blocks on Pika queue using BRPop with timeout
- **Duplicate Prevention**: Tracks broadcasted transactions with 5-minute TTL
- **Peer Broadcasting**: Broadcasts to all active P2P peers concurrently
- **Graceful Shutdown**: Context-based cancellation support
- **Error Handling**: Comprehensive error handling and logging

#### Architecture

```
┌─────────────────┐
│ Broadcast Queue │ (Pika List: pool:broadcast:queue)
└────────┬────────┘
         │ BRPop (blocking)
         ▼
┌─────────────────┐
│ Worker Pool     │ (10 workers)
│ ┌─────┐ ┌─────┐│
│ │ W1  │ │ W2  ││
│ └─────┘ └─────┘│
│ ... (10 total) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Check Duplicate │ (pool:broadcasted:{hash} with TTL)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Fetch from Pool │ (pool:pending:{hash})
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Broadcast to    │
│ All P2P Peers   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Mark Broadcasted│
└─────────────────┘
```

#### Usage

```go
import (
    "github.com/sunvim/evm_syncer/pkg/txpool"
    "github.com/sunvim/evm_syncer/pkg/storage"
    "github.com/sunvim/evm_syncer/pkg/logger"
)

// Create Pika client
pikaClient, err := storage.NewPikaClient(storage.DefaultPikaConfig())
if err != nil {
    log.Fatal(err)
}

// Create broadcaster
broadcaster := txpool.NewBroadcaster(
    pikaClient,
    peerManager, // Implements PeerManager interface
    logger.Get(),
)

// Start broadcaster workers
if err := broadcaster.Start(); err != nil {
    log.Fatal(err)
}
defer broadcaster.Stop()

// Enqueue transaction for broadcast
ctx := context.Background()
if err := broadcaster.EnqueueTransaction(ctx, txHash); err != nil {
    log.Printf("Failed to enqueue: %v", err)
}

// Get statistics
stats, err := broadcaster.GetStats(ctx)
if err != nil {
    log.Printf("Failed to get stats: %v", err)
}
```

### 3. Receiver (`receiver.go`)

The receiver handles transactions from P2P peers, validates them, stores in Pika, and forwards them for gossip.

#### Features

- **P2P Integration**: Receives transactions from network peers
- **Validation**: Validates all incoming transactions
- **Duplicate Detection**: Checks for duplicates before processing
- **Storage**: Stores valid transactions in pending pool
- **Gossip Forwarding**: Re-enqueues transactions for broadcast to other peers
- **Async Processing**: Non-blocking transaction handling with buffered channel
- **Batch Support**: Handles transaction batches efficiently

#### Data Flow

```
┌─────────────┐
│ P2P Network │
│   (Peers)   │
└──────┬──────┘
       │ Transaction received
       ▼
┌─────────────────┐
│ Receiver Channel│ (Buffer: 1000 tx)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Check Duplicate │
│ - Pending pool  │
│ - Broadcasted   │
└────────┬────────┘
         │ New transaction
         ▼
┌─────────────────┐
│ Validate        │
│ - Signature     │
│ - Size          │
│ - Gas           │
│ - Chain ID      │
└────────┬────────┘
         │ Valid
         ▼
┌─────────────────┐
│ Store in Pika   │
│ (pending pool)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Enqueue for     │
│ Gossip Broadcast│
└─────────────────┘
```

#### Usage

```go
import (
    "math/big"
    "github.com/sunvim/evm_syncer/pkg/txpool"
    "github.com/sunvim/evm_syncer/pkg/storage"
    "github.com/sunvim/evm_syncer/pkg/logger"
)

// Create receiver
chainID := big.NewInt(1)
pikaClient, _ := storage.NewPikaClient(storage.DefaultPikaConfig())

receiver := txpool.NewReceiver(
    pikaClient,
    chainID,
    logger.Get(),
)

// Start receiver
if err := receiver.Start(); err != nil {
    log.Fatal(err)
}
defer receiver.Stop()

// Handle single transaction from P2P
if err := receiver.HandleTransaction(tx, peerID); err != nil {
    log.Printf("Failed to handle transaction: %v", err)
}

// Handle transaction batch
if err := receiver.HandleTransactionBatch(txs, peerID); err != nil {
    log.Printf("Failed to handle batch: %v", err)
}

// Handle confirmed transactions (remove from pool)
if err := receiver.RemoveConfirmedTransaction(ctx, txHash); err != nil {
    log.Printf("Failed to remove confirmed: %v", err)
}

// Get statistics
stats, err := receiver.GetStats(ctx)
if err != nil {
    log.Printf("Failed to get stats: %v", err)
}
```

## Integration Example

Here's a complete example of integrating all three components:

```go
package main

import (
    "context"
    "log"
    "math/big"
    "os"
    "os/signal"
    "syscall"

    "github.com/sunvim/evm_syncer/pkg/logger"
    "github.com/sunvim/evm_syncer/pkg/storage"
    "github.com/sunvim/evm_syncer/pkg/txpool"
    "github.com/sunvim/evm_syncer/pkg/p2p"
)

func main() {
    // Initialize logger
    if err := logger.Init("info", "json"); err != nil {
        log.Fatalf("Failed to initialize logger: %v", err)
    }
    defer logger.Sync()

    // Initialize Pika client
    pikaConfig := &storage.PikaConfig{
        Address:  "localhost:9221",
        PoolSize: 10,
    }
    pikaClient, err := storage.NewPikaClient(pikaConfig)
    if err != nil {
        log.Fatalf("Failed to create Pika client: %v", err)
    }
    defer pikaClient.Close()

    // Initialize P2P peer manager
    peerManager := p2p.NewPeerManager(logger.Get())
    peerManager.Start()
    defer peerManager.Stop()

    // Initialize transaction pool components
    chainID := big.NewInt(1) // Ethereum mainnet

    // Create broadcaster
    broadcaster := txpool.NewBroadcaster(
        pikaClient,
        peerManager,
        logger.Get(),
    )
    if err := broadcaster.Start(); err != nil {
        log.Fatalf("Failed to start broadcaster: %v", err)
    }
    defer broadcaster.Stop()

    // Create receiver
    receiver := txpool.NewReceiver(
        pikaClient,
        chainID,
        logger.Get(),
    )
    if err := receiver.Start(); err != nil {
        log.Fatalf("Failed to start receiver: %v", err)
    }
    defer receiver.Stop()

    logger.Info("Transaction pool initialized successfully")

    // Wait for shutdown signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    logger.Info("Shutting down transaction pool...")
}
```

## Configuration

### Pika Queue Keys

- `pool:broadcast:queue` - List for transactions waiting to be broadcasted
- `pool:pending:{hash}` - Hash storing transaction data and metadata
- `pool:broadcasted:{hash}` - Key with TTL tracking recently broadcasted transactions
- `pool:pending:set` - Set of all pending transaction hashes
- `pool:broadcasted:set` - Set of all broadcasted transaction hashes

### Constants

#### Validator
- `MaxTransactionSize`: 128KB (131,072 bytes)
- `MaxGasLimit`: 30,000,000 gas

#### Broadcaster
- `BroadcastWorkers`: 10 concurrent workers
- `BroadcastQueueTimeout`: 5 seconds
- `BroadcastedTTL`: 5 minutes
- `EmptyQueueRetryDelay`: 1 second

#### Receiver
- `MaxBatchSize`: 100 transactions per batch
- `TxChannelBuffer`: 1000 transactions

## Error Handling

All components provide detailed error types for better error handling:

### Validator Errors
- `ErrInvalidSignature` - Transaction signature is invalid
- `ErrTxTooLarge` - Transaction exceeds size limit
- `ErrGasLimitExceeded` - Gas limit exceeds maximum
- `ErrChainIDMismatch` - Chain ID doesn't match network
- `ErrInvalidGasPrice` - Gas price is zero or negative
- `ErrNilTransaction` - Transaction is nil

### Receiver Errors
- `ErrDuplicateTransaction` - Transaction already exists in pool

## Monitoring and Metrics

### Broadcaster Stats
```go
stats, _ := broadcaster.GetStats(ctx)
// Returns:
// {
//   "queue_length": 42,
//   "workers": 10
// }
```

### Receiver Stats
```go
stats, _ := receiver.GetStats(ctx)
// Returns:
// {
//   "channel": {
//     "channel_len": 5,
//     "channel_cap": 1000,
//     "channel_usage": 0.5
//   },
//   "pool_stats": {
//     "pending_count": 150,
//     "broadcasted_count": 80,
//     "broadcast_queue_length": 42,
//     "total_count": 230
//   }
// }
```

## Testing

Run tests with:

```bash
# Run all tests
go test -v ./pkg/txpool/...

# Run specific test
go test -v ./pkg/txpool/... -run TestValidator

# Run benchmarks
go test -bench=. ./pkg/txpool/...
```

## Performance Considerations

1. **Worker Pool**: The broadcaster uses 10 concurrent workers to process transactions efficiently
2. **Buffered Channels**: The receiver uses a 1000-transaction buffer to handle burst traffic
3. **Batch Processing**: Both validator and receiver support batch operations
4. **Duplicate Prevention**: Uses Redis sets and TTL keys for efficient duplicate detection
5. **Non-blocking Operations**: Receiver processes transactions asynchronously

## Best Practices

1. **Always validate** transactions before adding to the pool
2. **Use batch operations** when processing multiple transactions
3. **Monitor queue lengths** to detect bottlenecks
4. **Set appropriate TTLs** for broadcasted transaction tracking
5. **Handle errors gracefully** and log them appropriately
6. **Use context cancellation** for graceful shutdown

## License

This implementation is part of the EVM Syncer project.
