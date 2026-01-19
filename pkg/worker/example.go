package worker

import (
	"context"
	"fmt"
	"time"
)

// Example demonstrates how to use the worker pool framework.

// BlockProcessingTask is an example task that processes blockchain blocks.
type BlockProcessingTask struct {
	*BaseTask
	blockNumber uint64
	blockHash   string
}

// NewBlockProcessingTask creates a new block processing task.
func NewBlockProcessingTask(blockNumber uint64, blockHash string) *BlockProcessingTask {
	return &BlockProcessingTask{
		BaseTask:    NewBaseTask(fmt.Sprintf("process-block-%d", blockNumber)),
		blockNumber: blockNumber,
		blockHash:   blockHash,
	}
}

// Execute processes the blockchain block.
func (t *BlockProcessingTask) Execute(ctx context.Context) error {
	// Simulate block processing
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		// Block processing logic would go here
		// For example:
		// - Validate block
		// - Process transactions
		// - Update state
		// - Store in database
		return nil
	}
}

// OnSuccess is called after successful block processing.
func (t *BlockProcessingTask) OnSuccess(ctx context.Context) {
	// Log success, update metrics, emit events, etc.
	fmt.Printf("Successfully processed block %d (%s)\n", t.blockNumber, t.blockHash)
}

// OnFailure is called after block processing fails.
func (t *BlockProcessingTask) OnFailure(ctx context.Context, err error) {
	// Log failure, retry logic, alerting, etc.
	fmt.Printf("Failed to process block %d (%s): %v\n", t.blockNumber, t.blockHash, err)
}

// ExampleBasicUsage shows basic worker pool usage.
func ExampleBasicUsage() {
	// Create a pool with 5 workers and a queue size of 100
	config := PoolConfig{
		Name:            "block-processor",
		WorkerCount:     5,
		QueueSize:       100,
		ShutdownTimeout: 30 * time.Second,
	}

	pool, err := NewPool(config)
	if err != nil {
		panic(err)
	}

	// Start the pool
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	// Submit tasks
	for i := uint64(0); i < 10; i++ {
		task := NewBlockProcessingTask(i, fmt.Sprintf("0x%x", i))
		if err := pool.Submit(task); err != nil {
			fmt.Printf("Failed to submit task: %v\n", err)
		}
	}

	// Wait for tasks to complete
	time.Sleep(2 * time.Second)

	// Check statistics
	stats := pool.Stats()
	fmt.Printf("Pool Stats: Completed=%d, Failed=%d, Active=%d\n",
		stats.CompletedTasks, stats.FailedTasks, stats.ActiveWorkers)
}

// ExampleWithContext shows how to use context for cancellation.
func ExampleWithContext() {
	config := PoolConfig{
		Name:            "cancellable-processor",
		WorkerCount:     3,
		QueueSize:       50,
		ShutdownTimeout: 10 * time.Second,
	}

	pool, err := NewPool(config)
	if err != nil {
		panic(err)
	}

	// Create a cancellable context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool.Start(ctx)
	defer pool.Shutdown(context.Background())

	// Submit long-running tasks
	for i := 0; i < 10; i++ {
		task := NewFuncTask(fmt.Sprintf("task-%d", i), func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return nil
			}
		})
		pool.Submit(task)
	}

	// Context will cancel after 5 seconds
	<-ctx.Done()
	fmt.Println("Context cancelled, shutting down...")
}

// ExampleCustomTask shows how to create a custom task with complex logic.
func ExampleCustomTask() {
	config := PoolConfig{
		Name:            "custom-task-pool",
		WorkerCount:     4,
		QueueSize:       100,
		ShutdownTimeout: 15 * time.Second,
	}

	pool, err := NewPool(config)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	// Submit custom tasks
	for i := 0; i < 5; i++ {
		taskData := struct {
			txHash string
			from   string
			to     string
			amount uint64
		}{
			txHash: fmt.Sprintf("0x%x", i),
			from:   "0xFrom",
			to:     "0xTo",
			amount: uint64(i * 1000),
		}

		task := NewFuncTask(fmt.Sprintf("tx-%d", i), func(ctx context.Context) error {
			fmt.Printf("Processing transaction %s: %s -> %s (%d)\n",
				taskData.txHash, taskData.from, taskData.to, taskData.amount)
			return nil
		})

		pool.Submit(task)
	}

	time.Sleep(1 * time.Second)
}

// ExampleMultiplePools shows how to run multiple pools concurrently.
func ExampleMultiplePools() {
	// Create separate pools for different task types
	blockPool, _ := NewPool(PoolConfig{
		Name:            "block-pool",
		WorkerCount:     5,
		QueueSize:       100,
		ShutdownTimeout: 30 * time.Second,
	})

	txPool, _ := NewPool(PoolConfig{
		Name:            "transaction-pool",
		WorkerCount:     10,
		QueueSize:       1000,
		ShutdownTimeout: 30 * time.Second,
	})

	ctx := context.Background()
	blockPool.Start(ctx)
	txPool.Start(ctx)

	defer blockPool.Shutdown(ctx)
	defer txPool.Shutdown(ctx)

	// Submit to different pools
	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			// Every 10th task goes to block pool
			task := NewBlockProcessingTask(uint64(i), fmt.Sprintf("0x%x", i))
			blockPool.Submit(task)
		} else {
			// Other tasks go to transaction pool
			task := NewFuncTask(fmt.Sprintf("tx-%d", i), func(ctx context.Context) error {
				// Process transaction
				return nil
			})
			txPool.Submit(task)
		}
	}

	time.Sleep(2 * time.Second)

	// Compare statistics
	blockStats := blockPool.Stats()
	txStats := txPool.Stats()

	fmt.Printf("Block Pool - Completed: %d, Failed: %d\n",
		blockStats.CompletedTasks, blockStats.FailedTasks)
	fmt.Printf("TX Pool - Completed: %d, Failed: %d\n",
		txStats.CompletedTasks, txStats.FailedTasks)
}

// ExampleErrorHandling shows comprehensive error handling.
func ExampleErrorHandling() {
	config := PoolConfig{
		Name:            "error-handling-pool",
		WorkerCount:     3,
		QueueSize:       50,
		ShutdownTimeout: 10 * time.Second,
	}

	pool, err := NewPool(config)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	// Task that might fail
	task := NewFuncTask("risky-task", func(ctx context.Context) error {
		// Simulate random failure
		if time.Now().Unix()%2 == 0 {
			return fmt.Errorf("random failure occurred")
		}
		return nil
	}).WithOnSuccess(func(ctx context.Context) {
		fmt.Println("Task succeeded!")
	}).WithOnFailure(func(ctx context.Context, err error) {
		fmt.Printf("Task failed with error: %v\n", err)
		// Implement retry logic, alerting, etc.
	})

	pool.Submit(task)
	time.Sleep(200 * time.Millisecond)
}

// ExampleGracefulShutdown demonstrates proper shutdown handling.
func ExampleGracefulShutdown() {
	config := PoolConfig{
		Name:            "shutdown-example",
		WorkerCount:     5,
		QueueSize:       100,
		ShutdownTimeout: 30 * time.Second,
	}

	pool, err := NewPool(config)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	pool.Start(ctx)

	// Submit many tasks
	for i := 0; i < 50; i++ {
		task := NewFuncTask(fmt.Sprintf("task-%d", i), func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		pool.Submit(task)
	}

	fmt.Println("Shutting down pool gracefully...")

	// Graceful shutdown waits for all tasks to complete
	if err := pool.Shutdown(ctx); err != nil {
		fmt.Printf("Shutdown error: %v\n", err)
	}

	stats := pool.Stats()
	fmt.Printf("Final stats - Completed: %d, Failed: %d\n",
		stats.CompletedTasks, stats.FailedTasks)
}
