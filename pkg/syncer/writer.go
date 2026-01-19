package syncer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"go.uber.org/zap"
)

const (
	// Default batch size for writing blocks
	DefaultBatchSize = 100
	// Default batch timeout
	DefaultBatchTimeout = 5 * time.Second
	// Write buffer size
	WriteBufferSize = 500
)

// WriteBatch represents a batch of blocks to write
type WriteBatch struct {
	Blocks   []*types.Block
	Receipts []types.Receipts
}

// Writer manages batched writes to Pika storage
type Writer struct {
	storage     *storage.BlockStorage
	logger      *zap.Logger
	batchSize   int
	batchTimeout time.Duration

	// Write pipeline
	writeCh chan *writeRequest
	batchCh chan *WriteBatch

	// Statistics
	writtenBlocks  atomic.Uint64
	writtenBatches atomic.Uint64
	writeErrors    atomic.Uint64

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

// writeRequest represents a single write request
type writeRequest struct {
	block    *types.Block
	receipts types.Receipts
	resultCh chan error
}

// NewWriter creates a new batch writer
func NewWriter(storage *storage.BlockStorage, logger *zap.Logger) *Writer {
	ctx, cancel := context.WithCancel(context.Background())

	return &Writer{
		storage:      storage,
		logger:       logger.With(zap.String("component", "writer")),
		batchSize:    DefaultBatchSize,
		batchTimeout: DefaultBatchTimeout,
		writeCh:      make(chan *writeRequest, WriteBufferSize),
		batchCh:      make(chan *WriteBatch, 10),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// SetBatchSize sets the batch size for writes
func (w *Writer) SetBatchSize(size int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if size > 0 {
		w.batchSize = size
		w.logger.Info("batch size updated", zap.Int("batch_size", size))
	}
}

// SetBatchTimeout sets the batch timeout
func (w *Writer) SetBatchTimeout(timeout time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if timeout > 0 {
		w.batchTimeout = timeout
		w.logger.Info("batch timeout updated", zap.Duration("timeout", timeout))
	}
}

// Start starts the writer pipeline
func (w *Writer) Start(ctx context.Context) {
	w.logger.Info("starting writer")

	// Start batcher goroutine
	w.wg.Add(1)
	go w.batcherLoop()

	// Start writer goroutine
	w.wg.Add(1)
	go w.writerLoop()
}

// Stop stops the writer pipeline
func (w *Writer) Stop(ctx context.Context) error {
	w.logger.Info("stopping writer")
	
	// Close write channel to signal batcher
	close(w.writeCh)
	
	// Wait for pipeline to drain with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("writer stopped gracefully")
		return nil
	case <-time.After(30 * time.Second):
		w.logger.Warn("writer stop timeout, forcing shutdown")
		w.cancel()
		<-done
		return fmt.Errorf("writer stop timeout")
	case <-ctx.Done():
		w.logger.Warn("writer stop cancelled")
		w.cancel()
		<-done
		return ctx.Err()
	}
}

// WriteBlock writes a single block asynchronously
func (w *Writer) WriteBlock(ctx context.Context, block *types.Block, receipts types.Receipts) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	req := &writeRequest{
		block:    block,
		receipts: receipts,
		resultCh: make(chan error, 1),
	}

	select {
	case w.writeCh <- req:
		// Wait for result
		select {
		case err := <-req.resultCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-w.ctx.Done():
			return fmt.Errorf("writer stopped")
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return fmt.Errorf("writer stopped")
	}
}

// WriteBatch writes a batch of blocks synchronously
func (w *Writer) WriteBatchBlocks(ctx context.Context, blocks []*types.Block, receipts []types.Receipts) error {
	if len(blocks) == 0 {
		return nil
	}

	if len(receipts) != len(blocks) {
		return fmt.Errorf("blocks and receipts length mismatch: %d != %d", len(blocks), len(receipts))
	}

	// Process in batches if needed
	for i := 0; i < len(blocks); i += w.batchSize {
		end := i + w.batchSize
		if end > len(blocks) {
			end = len(blocks)
		}

		batchBlocks := blocks[i:end]
		batchReceipts := receipts[i:end]

		if err := w.writeBatch(ctx, batchBlocks, batchReceipts); err != nil {
			return fmt.Errorf("failed to write batch at index %d: %w", i, err)
		}
	}

	return nil
}

// batcherLoop collects write requests into batches
func (w *Writer) batcherLoop() {
	defer w.wg.Done()
	defer close(w.batchCh)

	w.logger.Info("batcher started")

	var (
		batch     = &WriteBatch{}
		resultChs = make([]chan error, 0, w.batchSize)
		timer     = time.NewTimer(w.batchTimeout)
	)
	defer timer.Stop()

	flush := func() {
		if len(batch.Blocks) > 0 {
			// Send batch for writing
			select {
			case w.batchCh <- batch:
				w.logger.Debug("batch sent for writing",
					zap.Int("size", len(batch.Blocks)))
			case <-w.ctx.Done():
				// Notify all pending requests
				for _, ch := range resultChs {
					select {
					case ch <- fmt.Errorf("writer stopped"):
					default:
					}
				}
				return
			}

			// Create new batch
			batch = &WriteBatch{}
			resultChs = make([]chan error, 0, w.batchSize)
			timer.Reset(w.batchTimeout)
		}
	}

	for {
		select {
		case req, ok := <-w.writeCh:
			if !ok {
				// Channel closed, flush remaining batch
				flush()
				w.logger.Info("batcher stopped")
				return
			}

			// Add to batch
			batch.Blocks = append(batch.Blocks, req.block)
			batch.Receipts = append(batch.Receipts, req.receipts)
			resultChs = append(resultChs, req.resultCh)

			// Flush if batch is full
			if len(batch.Blocks) >= w.batchSize {
				flush()
			}

		case <-timer.C:
			// Timeout, flush current batch
			flush()
			timer.Reset(w.batchTimeout)

		case <-w.ctx.Done():
			w.logger.Info("batcher stopped by context")
			return
		}
	}
}

// writerLoop writes batches to storage
func (w *Writer) writerLoop() {
	defer w.wg.Done()

	w.logger.Info("writer started")

	for {
		select {
		case batch, ok := <-w.batchCh:
			if !ok {
				w.logger.Info("writer stopped")
				return
			}

			// Write batch
			if err := w.writeBatch(w.ctx, batch.Blocks, batch.Receipts); err != nil {
				w.logger.Error("failed to write batch",
					zap.Int("size", len(batch.Blocks)),
					zap.Error(err))
				w.writeErrors.Add(1)
			}

		case <-w.ctx.Done():
			w.logger.Info("writer stopped by context")
			return
		}
	}
}

// writeBatch writes a batch of blocks to storage
func (w *Writer) writeBatch(ctx context.Context, blocks []*types.Block, receipts []types.Receipts) error {
	if len(blocks) == 0 {
		return nil
	}

	startTime := time.Now()

	// Write blocks atomically
	if err := w.storage.BatchSaveBlocks(ctx, blocks, receipts); err != nil {
		return fmt.Errorf("failed to batch save blocks: %w", err)
	}

	// Update latest block pointer to the last block in batch
	lastBlock := blocks[len(blocks)-1]
	if err := w.storage.SaveLatestBlock(ctx, lastBlock.NumberU64()); err != nil {
		w.logger.Error("failed to update latest block",
			zap.Uint64("block", lastBlock.NumberU64()),
			zap.Error(err))
		// Don't fail the whole batch for this
	}

	duration := time.Since(startTime)
	blocksWritten := uint64(len(blocks))
	
	w.writtenBlocks.Add(blocksWritten)
	w.writtenBatches.Add(1)

	w.logger.Info("wrote batch",
		zap.Int("size", len(blocks)),
		zap.Uint64("from", blocks[0].NumberU64()),
		zap.Uint64("to", lastBlock.NumberU64()),
		zap.Duration("duration", duration),
		zap.Float64("blocks_per_sec", float64(blocksWritten)/duration.Seconds()))

	return nil
}

// HandleReorg handles a blockchain reorganization
func (w *Writer) HandleReorg(ctx context.Context, reorgBlock uint64, commonAncestor uint64) error {
	w.logger.Warn("handling reorg",
		zap.Uint64("reorg_block", reorgBlock),
		zap.Uint64("common_ancestor", commonAncestor))

	// Delete blocks from reorgBlock down to commonAncestor + 1
	for blockNum := reorgBlock; blockNum > commonAncestor; blockNum-- {
		if err := w.storage.DeleteBlock(ctx, blockNum); err != nil {
			w.logger.Error("failed to delete block during reorg",
				zap.Uint64("block", blockNum),
				zap.Error(err))
			return fmt.Errorf("failed to delete block %d: %w", blockNum, err)
		}

		w.logger.Debug("deleted block during reorg", zap.Uint64("block", blockNum))
	}

	// Update latest block pointer
	if err := w.storage.SaveLatestBlock(ctx, commonAncestor); err != nil {
		return fmt.Errorf("failed to update latest block: %w", err)
	}

	blocksDeleted := reorgBlock - commonAncestor
	w.logger.Info("reorg handled",
		zap.Uint64("blocks_deleted", blocksDeleted),
		zap.Uint64("new_head", commonAncestor))

	return nil
}

// Flush forces a flush of any pending writes
func (w *Writer) Flush(ctx context.Context) error {
	w.logger.Info("flushing pending writes")

	// Create a done channel
	done := make(chan struct{})
	
	// Submit a dummy request to ensure all previous requests are processed
	req := &writeRequest{
		block:    nil,
		receipts: nil,
		resultCh: make(chan error, 1),
	}

	// Send flush signal
	select {
	case w.writeCh <- req:
		close(done)
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return fmt.Errorf("flush timeout")
	}

	<-done
	w.logger.Info("flush completed")
	return nil
}

// GetStats returns writer statistics
func (w *Writer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"written_blocks":  w.writtenBlocks.Load(),
		"written_batches": w.writtenBatches.Load(),
		"write_errors":    w.writeErrors.Load(),
		"batch_size":      w.batchSize,
		"batch_timeout":   w.batchTimeout.String(),
		"queue_length":    len(w.writeCh),
	}
}

// GetWrittenBlocks returns the total number of blocks written
func (w *Writer) GetWrittenBlocks() uint64 {
	return w.writtenBlocks.Load()
}

// GetWriteErrors returns the total number of write errors
func (w *Writer) GetWriteErrors() uint64 {
	return w.writeErrors.Load()
}

// IsHealthy returns true if the writer is operating normally
func (w *Writer) IsHealthy() bool {
	// Check if write channel is not full
	queueFull := len(w.writeCh) >= cap(w.writeCh)
	
	// Check if error rate is acceptable (less than 5%)
	totalWrites := w.writtenBlocks.Load()
	errorRate := float64(0)
	if totalWrites > 0 {
		errorRate = float64(w.writeErrors.Load()) / float64(totalWrites)
	}

	healthy := !queueFull && errorRate < 0.05

	if !healthy {
		w.logger.Warn("writer health check failed",
			zap.Bool("queue_full", queueFull),
			zap.Float64("error_rate", errorRate))
	}

	return healthy
}

// ResetStats resets the writer statistics
func (w *Writer) ResetStats() {
	w.writtenBlocks.Store(0)
	w.writtenBatches.Store(0)
	w.writeErrors.Store(0)
	w.logger.Info("writer statistics reset")
}
