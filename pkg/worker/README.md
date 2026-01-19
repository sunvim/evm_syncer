# Worker Pool Framework

A production-ready worker pool implementation for the EVM Syncer project with lifecycle management, metrics, and structured logging.

## Features

- **Task Interface**: Flexible task abstraction with lifecycle hooks
- **Configurable Pools**: Control worker count, queue size, and shutdown behavior
- **Graceful Shutdown**: Context-aware shutdown with timeout support
- **Prometheus Metrics**: Built-in metrics for monitoring pool health
- **Thread-Safe**: Proper synchronization for concurrent access
- **Structured Logging**: Integration with zap logger
- **Error Handling**: Comprehensive error handling with callback support

## Quick Start

```go
import (
    "context"
    "time"
    "github.com/sunvim/evm_syncer/pkg/worker"
)

// Create a pool
config := worker.PoolConfig{
    Name:            "my-pool",
    WorkerCount:     5,
    QueueSize:       100,
    ShutdownTimeout: 30 * time.Second,
}

pool, err := worker.NewPool(config)
if err != nil {
    panic(err)
}

// Start the pool
ctx := context.Background()
pool.Start(ctx)
defer pool.Shutdown(ctx)

// Submit a task
task := worker.NewFuncTask("my-task", func(ctx context.Context) error {
    // Your task logic here
    return nil
})

if err := pool.Submit(task); err != nil {
    log.Printf("Failed to submit task: %v", err)
}
```

## Task Interface

The `Task` interface defines the contract for work units:

```go
type Task interface {
    Execute(ctx context.Context) error
    OnSuccess(ctx context.Context)
    OnFailure(ctx context.Context, err error)
    Name() string
}
```

### Creating Custom Tasks

You can create custom tasks by implementing the interface:

```go
type BlockProcessingTask struct {
    *worker.BaseTask
    blockNumber uint64
}

func (t *BlockProcessingTask) Execute(ctx context.Context) error {
    // Process block
    return nil
}

func (t *BlockProcessingTask) OnSuccess(ctx context.Context) {
    log.Printf("Block %d processed successfully", t.blockNumber)
}
```

### Using FuncTask

For simple tasks, use `FuncTask`:

```go
task := worker.NewFuncTask("simple-task", func(ctx context.Context) error {
    return doWork()
}).WithOnSuccess(func(ctx context.Context) {
    log.Println("Success!")
}).WithOnFailure(func(ctx context.Context, err error) {
    log.Printf("Failed: %v", err)
})
```

## Pool Configuration

```go
type PoolConfig struct {
    Name            string        // Pool identifier (for metrics/logs)
    WorkerCount     int           // Number of concurrent workers
    QueueSize       int           // Max queued tasks (0 = unbuffered)
    ShutdownTimeout time.Duration // Max time for graceful shutdown
    Logger          *zap.Logger   // Optional custom logger
}
```

## Pool Lifecycle

### Starting

```go
pool.Start(ctx)
```

Starts worker goroutines and begins processing tasks.

### Submitting Tasks

```go
// Blocking submission (waits if queue is full)
err := pool.Submit(task)

// Non-blocking submission (returns false if queue is full)
success := pool.TrySubmit(task)
```

### Graceful Shutdown

```go
// Wait for all tasks to complete (respects ShutdownTimeout)
err := pool.Shutdown(ctx)
```

### Force Shutdown

```go
// Immediately stop processing (may lose queued tasks)
pool.ShutdownNow()
```

## Monitoring

### Pool Statistics

```go
stats := pool.Stats()
fmt.Printf("Active: %d, Queued: %d, Completed: %d, Failed: %d\n",
    stats.ActiveWorkers,
    stats.QueuedTasks,
    stats.CompletedTasks,
    stats.FailedTasks,
)
```

### Prometheus Metrics

The following metrics are automatically exported:

- `worker_pool_active_workers{pool="name"}` - Current active workers
- `worker_pool_queue_length{pool="name"}` - Current queue length
- `worker_pool_tasks_completed_total{pool="name"}` - Total completed tasks
- `worker_pool_tasks_failed_total{pool="name"}` - Total failed tasks
- `worker_pool_task_duration_seconds{pool="name",status="success|failure"}` - Task duration histogram

## Best Practices

### Context Cancellation

Always respect context cancellation in your tasks:

```go
task := worker.NewFuncTask("long-task", func(ctx context.Context) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(longOperation):
        return nil
    }
})
```

### Error Handling

Use `OnFailure` callbacks for retry logic and alerting:

```go
task.WithOnFailure(func(ctx context.Context, err error) {
    if isRetryable(err) {
        retryQueue.Add(task)
    } else {
        alerting.Send("Task failed permanently: %v", err)
    }
})
```

### Pool Sizing

- **CPU-bound tasks**: WorkerCount = number of CPU cores
- **I/O-bound tasks**: WorkerCount = 2-4x CPU cores
- **QueueSize**: 10-100x WorkerCount for burst handling

### Multiple Pools

Use separate pools for different task types:

```go
blockPool := worker.NewPool(worker.PoolConfig{
    Name: "blocks",
    WorkerCount: 5,
    QueueSize: 100,
})

txPool := worker.NewPool(worker.PoolConfig{
    Name: "transactions",
    WorkerCount: 20,
    QueueSize: 1000,
})
```

## Examples

See [example.go](./example.go) for comprehensive usage examples including:

- Basic usage
- Custom tasks
- Context cancellation
- Error handling
- Multiple pools
- Graceful shutdown

## Testing

Run the test suite:

```bash
go test -v ./pkg/worker/...
```

Run benchmarks:

```bash
go test -bench=. ./pkg/worker/...
```

## Thread Safety

All pool operations are thread-safe and can be called concurrently from multiple goroutines:

- `Submit()` - Safe for concurrent submission
- `TrySubmit()` - Safe for concurrent submission
- `Shutdown()` - Safe to call multiple times (subsequent calls are no-op)
- `Stats()` - Safe for concurrent reads

## Performance Considerations

- Use `TrySubmit()` when you can't afford to block
- Set appropriate `QueueSize` to balance memory vs. latency
- Monitor metrics to tune `WorkerCount`
- Use task pools to avoid overwhelming downstream systems

## License

Part of the EVM Syncer project.
