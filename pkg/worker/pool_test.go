package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sunvim/evm_syncer/pkg/worker"
)

// TestPoolBasicExecution tests basic task execution in the pool
func TestPoolBasicExecution(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "test-pool",
		WorkerCount:     3,
		QueueSize:       10,
		ShutdownTimeout: 5 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	var completed atomic.Int32
	task := worker.NewFuncTask("test-task", func(ctx context.Context) error {
		completed.Add(1)
		return nil
	})

	if err := pool.Submit(task); err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if completed.Load() != 1 {
		t.Errorf("expected 1 completed task, got %d", completed.Load())
	}
}

// TestPoolMultipleTasks tests concurrent execution of multiple tasks
func TestPoolMultipleTasks(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "multi-task-pool",
		WorkerCount:     5,
		QueueSize:       100,
		ShutdownTimeout: 10 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	taskCount := 50
	var completed atomic.Int32
	var mu sync.Mutex
	results := make(map[int]bool)

	for i := 0; i < taskCount; i++ {
		taskID := i
		task := worker.NewFuncTask(
			fmt.Sprintf("task-%d", taskID),
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				mu.Lock()
				results[taskID] = true
				mu.Unlock()
				completed.Add(1)
				return nil
			},
		)

		if err := pool.Submit(task); err != nil {
			t.Fatalf("failed to submit task %d: %v", taskID, err)
		}
	}

	// Wait for all tasks to complete
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for tasks: completed %d/%d", completed.Load(), taskCount)
		case <-ticker.C:
			if completed.Load() == int32(taskCount) {
				goto done
			}
		}
	}

done:
	if len(results) != taskCount {
		t.Errorf("expected %d results, got %d", taskCount, len(results))
	}
}

// TestPoolTaskFailure tests error handling
func TestPoolTaskFailure(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "failure-pool",
		WorkerCount:     2,
		QueueSize:       5,
		ShutdownTimeout: 5 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	var failureCallbackCalled atomic.Bool
	expectedErr := errors.New("task failure")

	task := worker.NewFuncTask("failing-task", func(ctx context.Context) error {
		return expectedErr
	}).WithOnFailure(func(ctx context.Context, err error) {
		if err == expectedErr {
			failureCallbackCalled.Store(true)
		}
	})

	if err := pool.Submit(task); err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !failureCallbackCalled.Load() {
		t.Error("failure callback was not called")
	}

	stats := pool.Stats()
	if stats.FailedTasks != 1 {
		t.Errorf("expected 1 failed task, got %d", stats.FailedTasks)
	}
}

// TestPoolGracefulShutdown tests graceful shutdown behavior
func TestPoolGracefulShutdown(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "shutdown-pool",
		WorkerCount:     2,
		QueueSize:       10,
		ShutdownTimeout: 5 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)

	var completed atomic.Int32
	taskCount := 5

	for i := 0; i < taskCount; i++ {
		task := worker.NewFuncTask(
			fmt.Sprintf("task-%d", i),
			func(ctx context.Context) error {
				time.Sleep(100 * time.Millisecond)
				completed.Add(1)
				return nil
			},
		)

		if err := pool.Submit(task); err != nil {
			t.Fatalf("failed to submit task: %v", err)
		}
	}

	// Shutdown and wait
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	if completed.Load() != int32(taskCount) {
		t.Errorf("not all tasks completed: %d/%d", completed.Load(), taskCount)
	}

	// Try to submit after shutdown
	task := worker.NewFuncTask("post-shutdown", func(ctx context.Context) error {
		return nil
	})

	err = pool.Submit(task)
	if err != worker.ErrPoolShuttingDown {
		t.Errorf("expected ErrPoolShuttingDown, got %v", err)
	}
}

// TestPoolContextCancellation tests that tasks respect context cancellation
func TestPoolContextCancellation(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "cancel-pool",
		WorkerCount:     2,
		QueueSize:       5,
		ShutdownTimeout: 5 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer pool.Shutdown(context.Background())

	var started atomic.Bool
	var cancelled atomic.Bool

	task := worker.NewFuncTask("cancellable-task", func(ctx context.Context) error {
		started.Store(true)
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	})

	if err := pool.Submit(task); err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	// Wait for task to start
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for cancellation to propagate
	time.Sleep(200 * time.Millisecond)

	if !started.Load() {
		t.Error("task never started")
	}

	if !cancelled.Load() {
		t.Error("task did not respect context cancellation")
	}
}

// TestPoolTrySubmit tests non-blocking submission
func TestPoolTrySubmit(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "try-submit-pool",
		WorkerCount:     1,
		QueueSize:       2,
		ShutdownTimeout: 5 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	// Fill the queue
	blockTask := worker.NewFuncTask("block", func(ctx context.Context) error {
		time.Sleep(2 * time.Second)
		return nil
	})

	// Submit blocking tasks
	pool.Submit(blockTask)
	pool.TrySubmit(blockTask)

	// Try to submit when queue is full
	quickTask := worker.NewFuncTask("quick", func(ctx context.Context) error {
		return nil
	})

	// This should fail because queue is full
	if pool.TrySubmit(quickTask) {
		t.Error("TrySubmit should have failed with full queue")
	}
}

// TestPoolStats tests statistics reporting
func TestPoolStats(t *testing.T) {
	config := worker.PoolConfig{
		Name:            "stats-pool",
		WorkerCount:     3,
		QueueSize:       10,
		ShutdownTimeout: 5 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	stats := pool.Stats()
	if stats.Name != "stats-pool" {
		t.Errorf("expected name 'stats-pool', got '%s'", stats.Name)
	}
	if stats.WorkerCount != 3 {
		t.Errorf("expected 3 workers, got %d", stats.WorkerCount)
	}
	if stats.State != "running" {
		t.Errorf("expected state 'running', got '%s'", stats.State)
	}

	// Submit some tasks
	for i := 0; i < 5; i++ {
		task := worker.NewFuncTask(fmt.Sprintf("task-%d", i), func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		})
		pool.Submit(task)
	}

	time.Sleep(300 * time.Millisecond)

	stats = pool.Stats()
	if stats.CompletedTasks < 5 {
		t.Errorf("expected at least 5 completed tasks, got %d", stats.CompletedTasks)
	}
}

// ExamplePool demonstrates basic usage of the worker pool
func ExamplePool() {
	// Create pool configuration
	config := worker.PoolConfig{
		Name:            "example-pool",
		WorkerCount:     5,
		QueueSize:       100,
		ShutdownTimeout: 10 * time.Second,
	}

	// Create the pool
	pool, err := worker.NewPool(config)
	if err != nil {
		panic(err)
	}

	// Start the pool
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	// Submit a simple task
	task := worker.NewFuncTask("hello-task", func(ctx context.Context) error {
		fmt.Println("Hello from worker pool!")
		return nil
	})

	if err := pool.Submit(task); err != nil {
		panic(err)
	}

	// Wait a bit for task to complete
	time.Sleep(100 * time.Millisecond)

	// Check statistics
	stats := pool.Stats()
	fmt.Printf("Completed: %d, Failed: %d\n", stats.CompletedTasks, stats.FailedTasks)
}

// BenchmarkPoolThroughput benchmarks task throughput
func BenchmarkPoolThroughput(b *testing.B) {
	config := worker.PoolConfig{
		Name:            "bench-pool",
		WorkerCount:     10,
		QueueSize:       1000,
		ShutdownTimeout: 30 * time.Second,
	}

	pool, err := worker.NewPool(config)
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Shutdown(ctx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		task := worker.NewFuncTask("bench-task", func(ctx context.Context) error {
			// Simulate some work
			time.Sleep(time.Microsecond)
			return nil
		})

		pool.Submit(task)
	}

	// Wait for all tasks to complete
	for pool.Stats().CompletedTasks+pool.Stats().FailedTasks < int64(b.N) {
		time.Sleep(10 * time.Millisecond)
	}
}
