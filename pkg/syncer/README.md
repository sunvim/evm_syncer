# Syncer Package

The syncer package provides a production-ready blockchain synchronization engine for EVM-compatible chains. It handles downloading, validating, and storing blockchain data with support for concurrent operations, reorg detection, and graceful shutdown.

## Components

### 1. Syncer (`syncer.go`)
Main orchestration component that coordinates all sync operations.

**Features:**
- Full sync mode starting from genesis or latest checkpoint
- Configurable batch size and worker count
- Automatic checkpoint/resume functionality
- Graceful start/stop with context support
- Real-time progress tracking and reporting

**Usage:**
```go
config := syncer.DefaultSyncConfig()
config.StartBlock = 0
config.TargetBlock = 1000000
config.BatchSize = 100
config.WorkerCount = 50

s, err := syncer.NewSyncer(config, chainAdapter, peerManager, pikaClient, logger)
if err != nil {
    log.Fatal(err)
}

ctx := context.Background()
if err := s.Start(ctx); err != nil {
    log.Fatal(err)
}

// Wait for sync completion or handle shutdown
<-s.Done()
```

### 2. Downloader (`downloader.go`)
Manages concurrent block downloads from P2P peers using worker pools.

**Features:**
- Concurrent downloads with 50 workers by default
- Separate download paths for headers, bodies, and receipts
- Automatic retry logic with exponential backoff
- Peer selection based on score
- Request batching (192 headers, 128 bodies, 128 receipts per batch)

**Performance:**
- Headers: Batch size 192
- Bodies: Batch size 128  
- Receipts: Batch size 128
- Max retries: 3 with 2s delay

### 3. Validator (`validator.go`)
Performs comprehensive block validation according to consensus rules.

**Validation Steps:**
1. **Header Validation:**
   - Basic sanity checks (number, timestamp)
   - Parent hash verification
   - Block number continuity
   - Consensus seal verification
   - Signature validation

2. **Body Validation:**
   - Transaction root calculation and verification
   - Uncle root calculation and verification
   - Individual transaction validation
   - Block hash verification

3. **Receipt Validation:**
   - Receipt count matches transaction count
   - Receipt root calculation and verification
   - Receipt metadata validation
   - Bloom filter verification

4. **Chain Validation:**
   - Sequential block validation
   - Chain continuity verification

**Reorg Detection:**
- Compares stored vs. new block hashes at same height
- Triggers automatic reorg handling

### 4. Writer (`writer.go`)
Manages batched, pipelined writes to Pika storage.

**Features:**
- Batch writing (100 blocks by default)
- Pipeline architecture for high throughput
- Automatic flushing on batch size or timeout
- Atomic writes using Redis transactions
- Reorg handling with block deletion

**Performance:**
- Batch size: 100 blocks (configurable)
- Batch timeout: 5 seconds
- Write buffer: 500 requests
- Transaction pipelines for atomicity

### 5. Progress Tracker (`progress.go`)
Tracks and persists sync progress for checkpoint/resume.

**Tracked Metrics:**
- Current block number
- Target block number
- Start block and time
- Total blocks synced
- Reorg count
- Last block hash
- Sync speed (blocks/second)
- Estimated time remaining

**Persistence:**
- Auto-save every 1000 blocks
- Auto-save every 30 seconds
- Stored in Pika with key `sync:progress`

### 6. Metrics (`metrics.go`)
Prometheus metrics for monitoring and observability.

**Available Metrics:**
- `syncer_current_block` - Current block being synced
- `syncer_target_block` - Target block for sync
- `syncer_progress_percent` - Sync progress percentage
- `syncer_blocks_per_second` - Sync speed
- `syncer_downloaded_headers_total` - Total headers downloaded
- `syncer_downloaded_bodies_total` - Total bodies downloaded
- `syncer_downloaded_receipts_total` - Total receipts downloaded
- `syncer_validated_blocks_total` - Total blocks validated
- `syncer_validation_errors_total` - Total validation errors
- `syncer_written_blocks_total` - Total blocks written
- `syncer_reorgs_total` - Total reorgs detected
- `syncer_errors_total` - Total sync errors
- `syncer_download_duration_seconds` - Download duration histogram
- `syncer_validation_duration_seconds` - Validation duration histogram
- `syncer_write_duration_seconds` - Write duration histogram

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Syncer                              │
│  (Main Orchestrator - Manages Lifecycle & Coordination)    │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Downloader  │   │  Validator   │   │    Writer    │
│              │   │              │   │              │
│ • Headers    │──▶│ • Headers    │──▶│ • Batching   │
│ • Bodies     │   │ • Bodies     │   │ • Pipeline   │
│ • Receipts   │   │ • Receipts   │   │ • Reorg      │
│              │   │ • Chain      │   │              │
└──────────────┘   └──────────────┘   └──────────────┘
        │                                     │
        ▼                                     ▼
┌──────────────┐                     ┌──────────────┐
│ Worker Pool  │                     │ Pika Storage │
│ (50 workers) │                     │ (Redis-like) │
└──────────────┘                     └──────────────┘
        │
        ▼
┌──────────────┐
│ P2P Peers    │
└──────────────┘

        ┌──────────────────────────────────┐
        │     Supporting Components        │
        ├──────────────────────────────────┤
        │ • Progress Tracker (Checkpoints) │
        │ • Metrics (Prometheus)           │
        └──────────────────────────────────┘
```

## Sync Flow

1. **Initialization:**
   - Load progress from Pika
   - Determine start block (genesis, checkpoint, or latest)
   - Set target block (from config or best peer)
   - Initialize all components

2. **Sync Loop:**
   ```
   For each batch:
     1. Download headers from peers
     2. Check for reorgs
     3. Download bodies for headers
     4. Download receipts for blocks
     5. Validate blocks (headers, bodies, receipts)
     6. Write blocks to storage
     7. Update progress
     8. Report metrics
   ```

3. **Reorg Handling:**
   - Detect reorg by comparing block hashes
   - Find common ancestor
   - Delete divergent blocks
   - Resume sync from common ancestor

4. **Shutdown:**
   - Stop accepting new requests
   - Drain pending batches
   - Flush writer pipeline
   - Save final progress checkpoint
   - Close all components

## Configuration

```go
type SyncConfig struct {
    Mode               string  // "full" - full sync mode
    StartBlock         uint64  // Starting block number (0 = genesis)
    TargetBlock        uint64  // Target block number (0 = auto-detect)
    BatchSize          int     // Blocks per batch (default: 100)
    CheckpointInterval uint64  // Blocks between checkpoints (default: 1000)
    WorkerCount        int     // Worker pool size (default: 50)
}
```

## Error Handling

- **Download Errors:** Automatic retry with exponential backoff (max 3 retries)
- **Validation Errors:** Logged and tracked in metrics, batch fails
- **Write Errors:** Logged and tracked, can retry entire batch
- **Reorg Detection:** Automatic reorg handling with rollback
- **Peer Failures:** Automatic peer selection and failover

## Performance Considerations

### Throughput
- **Concurrent Downloads:** 50 workers downloading in parallel
- **Batch Processing:** 100 blocks per batch reduces overhead
- **Pipeline Architecture:** Overlapping download/validate/write operations
- **Worker Pools:** Efficient goroutine management

### Memory
- **Bounded Queues:** Prevents unbounded memory growth
- **Batch Limits:** Controls memory per batch
- **Stream Processing:** Blocks processed and released quickly

### Network
- **Request Batching:** Reduces network round trips
- **Peer Selection:** Uses best-scoring peers for reliability
- **Retry Logic:** Handles transient network failures

### Storage
- **Batch Writes:** Reduces storage operations
- **Pipeline Transactions:** Atomic batch commits
- **Checkpoint Strategy:** Balances durability and performance

## Monitoring

### Log Levels
- **INFO:** Progress updates, major events
- **WARN:** Reorgs, peer failures, retries
- **ERROR:** Critical failures, validation errors
- **DEBUG:** Detailed operation logs

### Metrics
Export Prometheus metrics at `/metrics` endpoint:
```go
import "github.com/prometheus/client_golang/prometheus/promhttp"

http.Handle("/metrics", promhttp.Handler())
http.ListenAndServe(":2112", nil)
```

### Health Checks
```go
// Check if syncer is running
if !syncer.IsRunning() {
    log.Warn("syncer not running")
}

// Check if synced to target
if syncer.IsSynced() {
    log.Info("sync complete")
}

// Get detailed stats
stats := syncer.GetStats()
log.Info("syncer stats", zap.Any("stats", stats))
```

## Production Deployment

### Recommended Settings
```go
config := &syncer.SyncConfig{
    Mode:               syncer.SyncModeFull,
    BatchSize:          100,
    WorkerCount:        50,
    CheckpointInterval: 1000,
}
```

### Resource Requirements
- **CPU:** 4+ cores recommended
- **Memory:** 4GB+ recommended
- **Storage:** Fast SSD for Pika storage
- **Network:** Stable connection to P2P network

### Best Practices
1. Monitor Prometheus metrics
2. Set up alerting for sync lag
3. Configure proper log rotation
4. Use persistent Pika storage
5. Implement proper shutdown handling
6. Test reorg handling in staging

## Testing

While no unit tests are currently provided, here's how to test:

```go
// Integration test example
func TestFullSync(t *testing.T) {
    // Set up test dependencies
    chainAdapter := setupTestChain()
    peerManager := setupTestPeers()
    pikaClient := setupTestPika()
    logger := setupTestLogger()
    
    // Create syncer
    config := syncer.DefaultSyncConfig()
    config.StartBlock = 0
    config.TargetBlock = 1000
    
    s, err := syncer.NewSyncer(config, chainAdapter, peerManager, pikaClient, logger)
    require.NoError(t, err)
    
    // Start sync
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()
    
    err = s.Start(ctx)
    require.NoError(t, err)
    
    // Wait for completion
    select {
    case <-s.Done():
        assert.True(t, s.IsSynced())
    case <-ctx.Done():
        t.Fatal("sync timeout")
    }
}
```

## Troubleshooting

### Sync Stuck
- Check peer connectivity
- Verify target block is set
- Check for validation errors in logs
- Review Prometheus metrics

### High Memory Usage
- Reduce batch size
- Reduce worker count
- Check for goroutine leaks

### Slow Sync Speed
- Increase worker count
- Increase batch size
- Check network latency
- Verify storage performance

### Validation Errors
- Check chain adapter configuration
- Verify consensus rules
- Check for corrupted peer data
- Review validator logs

## Future Enhancements

Potential improvements for future versions:
- Snap sync mode for faster initial sync
- Light client support
- Parallel validation
- Advanced peer selection algorithms
- Adaptive batch sizing
- Smart reorg detection
- State sync support

## License

This code is part of the evm_syncer project.
