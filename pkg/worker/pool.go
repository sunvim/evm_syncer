package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sunvim/evm_syncer/pkg/logger"
	"go.uber.org/zap"
)

var (
	// ErrPoolClosed is returned when submitting a task to a closed pool.
	ErrPoolClosed = errors.New("worker pool is closed")

	// ErrPoolShuttingDown is returned when submitting a task during shutdown.
	ErrPoolShuttingDown = errors.New("worker pool is shutting down")
)

// PoolConfig holds configuration for a worker pool.
type PoolConfig struct {
	// Name is the identifier for this pool (used in metrics and logs).
	Name string

	// WorkerCount is the number of concurrent workers.
	WorkerCount int

	// QueueSize is the maximum number of tasks that can be queued.
	// A size of 0 means unbuffered (blocking submission).
	QueueSize int

	// ShutdownTimeout is the maximum time to wait for graceful shutdown.
	// After this timeout, the pool will force-close.
	ShutdownTimeout time.Duration

	// Logger is an optional custom logger for this pool.
	// If nil, the global logger will be used.
	Logger *zap.Logger
}

// Pool is a worker pool that executes tasks concurrently.
// It provides graceful shutdown, metrics tracking, and error handling.
type Pool struct {
	config  PoolConfig
	logger  *zap.Logger
	tasks   chan Task
	metrics *poolMetrics

	wg      sync.WaitGroup
	state   atomic.Int32 // 0: running, 1: shutting down, 2: closed
	cancel  context.CancelFunc
	
	activeWorkers atomic.Int64
	queuedTasks   atomic.Int64
	completedTasks atomic.Int64
	failedTasks   atomic.Int64

	mu sync.RWMutex
}

const (
	stateRunning = iota
	stateShuttingDown
	stateClosed
)

// NewPool creates a new worker pool with the given configuration.
func NewPool(config PoolConfig) (*Pool, error) {
	if config.WorkerCount <= 0 {
		return nil, fmt.Errorf("worker count must be positive, got %d", config.WorkerCount)
	}

	if config.QueueSize < 0 {
		return nil, fmt.Errorf("queue size must be non-negative, got %d", config.QueueSize)
	}

	if config.Name == "" {
		config.Name = "default"
	}

	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 30 * time.Second
	}

	poolLogger := config.Logger
	if poolLogger == nil {
		poolLogger = logger.Get()
	}

	p := &Pool{
		config:  config,
		logger:  poolLogger.With(zap.String("pool", config.Name)),
		tasks:   make(chan Task, config.QueueSize),
		metrics: newPoolMetrics(config.Name),
	}

	p.state.Store(stateRunning)
	return p, nil
}

// Start starts the worker pool. This must be called before submitting tasks.
func (p *Pool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancel != nil {
		// Already started
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.logger.Info("starting worker pool",
		zap.Int("workers", p.config.WorkerCount),
		zap.Int("queue_size", p.config.QueueSize),
	)

	// Start worker goroutines
	for i := 0; i < p.config.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}

	// Start metrics updater
	go p.updateMetrics()

	p.logger.Info("worker pool started")
}

// Submit adds a task to the pool for execution.
// Returns an error if the pool is closed or shutting down.
// This call may block if the queue is full.
func (p *Pool) Submit(task Task) error {
	if p.state.Load() >= stateShuttingDown {
		return ErrPoolShuttingDown
	}

	p.queuedTasks.Add(1)
	defer p.queuedTasks.Add(-1)

	select {
	case p.tasks <- task:
		p.logger.Debug("task submitted", zap.String("task", task.Name()))
		return nil
	default:
		// Queue is full, block until space is available or pool shuts down
		if p.state.Load() >= stateShuttingDown {
			return ErrPoolShuttingDown
		}
		
		select {
		case p.tasks <- task:
			p.logger.Debug("task submitted", zap.String("task", task.Name()))
			return nil
		case <-time.After(5 * time.Second):
			if p.state.Load() >= stateShuttingDown {
				return ErrPoolShuttingDown
			}
			return fmt.Errorf("timeout submitting task: queue full")
		}
	}
}

// TrySubmit attempts to submit a task without blocking.
// Returns false if the queue is full or the pool is shutting down.
func (p *Pool) TrySubmit(task Task) bool {
	if p.state.Load() >= stateShuttingDown {
		return false
	}

	p.queuedTasks.Add(1)
	select {
	case p.tasks <- task:
		p.queuedTasks.Add(-1)
		p.logger.Debug("task submitted (non-blocking)", zap.String("task", task.Name()))
		return true
	default:
		p.queuedTasks.Add(-1)
		return false
	}
}

// Shutdown gracefully shuts down the pool, waiting for all tasks to complete.
// No new tasks can be submitted after this is called.
func (p *Pool) Shutdown(ctx context.Context) error {
	if !p.state.CompareAndSwap(stateRunning, stateShuttingDown) {
		return ErrPoolClosed
	}

	p.logger.Info("shutting down worker pool")

	// Close task channel to signal workers
	close(p.tasks)

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	timeout := p.config.ShutdownTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}

	select {
	case <-done:
		p.logger.Info("worker pool shutdown complete")
	case <-time.After(timeout):
		p.logger.Warn("worker pool shutdown timeout, forcing close",
			zap.Duration("timeout", timeout),
		)
		if p.cancel != nil {
			p.cancel()
		}
		// Wait a bit more for forced shutdown
		<-done
	case <-ctx.Done():
		p.logger.Warn("worker pool shutdown cancelled by context")
		if p.cancel != nil {
			p.cancel()
		}
		<-done
	}

	p.state.Store(stateClosed)
	return nil
}

// ShutdownNow immediately stops the pool without waiting for tasks to complete.
func (p *Pool) ShutdownNow() {
	if p.state.Load() >= stateClosed {
		return
	}

	p.logger.Warn("force shutting down worker pool")
	p.state.Store(stateClosed)
	
	if p.cancel != nil {
		p.cancel()
	}
	
	close(p.tasks)
}

// worker is the main worker goroutine that processes tasks.
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	workerLogger := p.logger.With(zap.Int("worker_id", id))
	workerLogger.Debug("worker started")

	for {
		select {
		case <-ctx.Done():
			workerLogger.Debug("worker stopped by context")
			return
		case task, ok := <-p.tasks:
			if !ok {
				workerLogger.Debug("worker stopped: task channel closed")
				return
			}

			p.executeTask(ctx, task, workerLogger)
		}
	}
}

// executeTask executes a single task with proper error handling and metrics.
func (p *Pool) executeTask(ctx context.Context, task Task, workerLogger *zap.Logger) {
	p.activeWorkers.Add(1)
	defer p.activeWorkers.Add(-1)

	taskLogger := workerLogger.With(zap.String("task", task.Name()))
	startTime := time.Now()

	taskLogger.Debug("executing task")

	// Execute the task
	err := task.Execute(ctx)
	duration := time.Since(startTime)

	if err != nil {
		// Task failed
		p.failedTasks.Add(1)
		p.metrics.recordTaskFailure(duration)
		
		taskLogger.Error("task failed",
			zap.Error(err),
			zap.Duration("duration", duration),
		)
		
		task.OnFailure(ctx, err)
	} else {
		// Task succeeded
		p.completedTasks.Add(1)
		p.metrics.recordTaskCompletion(duration)
		
		taskLogger.Debug("task completed",
			zap.Duration("duration", duration),
		)
		
		task.OnSuccess(ctx)
	}
}

// updateMetrics periodically updates the Prometheus metrics.
func (p *Pool) updateMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if p.state.Load() >= stateClosed {
			return
		}

		<-ticker.C
		
		p.metrics.setActiveWorkers(float64(p.activeWorkers.Load()))
		p.metrics.setQueueLength(float64(p.queuedTasks.Load()))
	}
}

// Stats returns current pool statistics.
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		Name:           p.config.Name,
		WorkerCount:    p.config.WorkerCount,
		QueueSize:      p.config.QueueSize,
		ActiveWorkers:  int(p.activeWorkers.Load()),
		QueuedTasks:    int(p.queuedTasks.Load()),
		CompletedTasks: p.completedTasks.Load(),
		FailedTasks:    p.failedTasks.Load(),
		State:          p.getStateName(),
	}
}

// PoolStats contains statistics about a worker pool.
type PoolStats struct {
	Name           string
	WorkerCount    int
	QueueSize      int
	ActiveWorkers  int
	QueuedTasks    int
	CompletedTasks int64
	FailedTasks    int64
	State          string
}

func (p *Pool) getStateName() string {
	switch p.state.Load() {
	case stateRunning:
		return "running"
	case stateShuttingDown:
		return "shutting_down"
	case stateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// IsRunning returns true if the pool is accepting new tasks.
func (p *Pool) IsRunning() bool {
	return p.state.Load() == stateRunning
}

// IsClosed returns true if the pool has been shut down.
func (p *Pool) IsClosed() bool {
	return p.state.Load() >= stateClosed
}
