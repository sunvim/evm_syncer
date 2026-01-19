package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sunvim/evm_syncer/pkg/chain"
	"github.com/sunvim/evm_syncer/pkg/p2p"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"go.uber.org/zap"
)

const (
	// Sync modes
	SyncModeFull = "full"
	
	// Default sync parameters
	DefaultSyncBatchSize     = 100
	DefaultCheckpointInterval = 1000
	DefaultProgressInterval   = 10 * time.Second
)

var (
	ErrSyncerNotStarted = errors.New("syncer not started")
	ErrSyncerStopped    = errors.New("syncer stopped")
	ErrNoTargetBlock    = errors.New("no target block set")
)

// SyncConfig holds syncer configuration
type SyncConfig struct {
	Mode               string
	StartBlock         uint64
	TargetBlock        uint64
	BatchSize          int
	CheckpointInterval uint64
	WorkerCount        int
}

// DefaultSyncConfig returns default sync configuration
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		Mode:               SyncModeFull,
		StartBlock:         0,
		TargetBlock:        0,
		BatchSize:          DefaultSyncBatchSize,
		CheckpointInterval: DefaultCheckpointInterval,
		WorkerCount:        DefaultWorkerCount,
	}
}

// Syncer orchestrates the blockchain synchronization process
type Syncer struct {
	config       *SyncConfig
	chainAdapter chain.IChainAdapter
	peerManager  *p2p.PeerManager
	storage      *storage.BlockStorage
	logger       *zap.Logger

	// Components
	downloader *Downloader
	validator  *Validator
	writer     *Writer
	progress   *ProgressTracker
	metrics    *SyncMetrics

	// State
	state      atomic.Int32 // 0: stopped, 1: starting, 2: running, 3: paused, 4: stopping
	currentBlock atomic.Uint64
	targetBlock  atomic.Uint64

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex

	// Channels
	errorCh chan error
	doneCh  chan struct{}
}

const (
	stateStopped = iota
	stateStarting
	stateRunning
	statePaused
	stateStopping
)

// NewSyncer creates a new blockchain syncer
func NewSyncer(
	config *SyncConfig,
	chainAdapter chain.IChainAdapter,
	peerManager *p2p.PeerManager,
	pikaClient *storage.PikaClient,
	logger *zap.Logger,
) (*Syncer, error) {
	if config == nil {
		config = DefaultSyncConfig()
	}

	if chainAdapter == nil {
		return nil, fmt.Errorf("chain adapter is required")
	}

	if peerManager == nil {
		return nil, fmt.Errorf("peer manager is required")
	}

	if pikaClient == nil {
		return nil, fmt.Errorf("pika client is required")
	}

	blockStorage := storage.NewBlockStorage(pikaClient)

	// Create downloader
	downloader, err := NewDownloader(peerManager, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create downloader: %w", err)
	}

	// Create validator
	validator := NewValidator(chainAdapter, blockStorage, logger)

	// Create writer
	writer := NewWriter(blockStorage, logger)
	writer.SetBatchSize(config.BatchSize)

	// Create progress tracker
	progressTracker := NewProgressTracker(blockStorage, pikaClient, logger)

	// Create metrics
	metrics := NewSyncMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	s := &Syncer{
		config:       config,
		chainAdapter: chainAdapter,
		peerManager:  peerManager,
		storage:      blockStorage,
		logger:       logger.With(zap.String("component", "syncer")),
		downloader:   downloader,
		validator:    validator,
		writer:       writer,
		progress:     progressTracker,
		metrics:      metrics,
		ctx:          ctx,
		cancel:       cancel,
		errorCh:      make(chan error, 10),
		doneCh:       make(chan struct{}),
	}

	s.state.Store(stateStopped)
	return s, nil
}

// Start starts the syncer
func (s *Syncer) Start(ctx context.Context) error {
	if !s.state.CompareAndSwap(stateStopped, stateStarting) {
		return fmt.Errorf("syncer already started")
	}

	s.logger.Info("starting syncer", zap.String("mode", s.config.Mode))

	// Load progress
	progress, err := s.progress.LoadProgress(ctx)
	if err != nil {
		s.state.Store(stateStopped)
		return fmt.Errorf("failed to load progress: %w", err)
	}

	// Determine start block
	startBlock := s.config.StartBlock
	if progress.CurrentBlock > 0 {
		startBlock = progress.CurrentBlock
		s.logger.Info("resuming from checkpoint", zap.Uint64("block", startBlock))
	} else {
		// Try to load from storage
		latestBlock, err := s.storage.LoadLatestBlock(ctx)
		if err == nil && latestBlock > 0 {
			startBlock = latestBlock
			s.logger.Info("resuming from storage", zap.Uint64("block", startBlock))
		}
	}

	s.currentBlock.Store(startBlock)

	// Set target block
	targetBlock := s.config.TargetBlock
	if targetBlock == 0 {
		// Get from best peer
		peers := s.peerManager.GetAllPeers()
		for _, peer := range peers {
			if peer.IsConnected() && peer.Head() != (common.Hash{}) {
				// In a real scenario, we'd query the peer for the latest block number
				// For now, we'll use a placeholder
				s.logger.Warn("target block not set and cannot be determined from peers")
			}
		}
	}

	if targetBlock > 0 {
		s.targetBlock.Store(targetBlock)
		if err := s.progress.SetTargetBlock(ctx, targetBlock); err != nil {
			s.logger.Error("failed to set target block in progress", zap.Error(err))
		}
	}

	// Start components
	s.downloader.Start(ctx)
	s.writer.Start(ctx)

	// Start sync loop
	s.state.Store(stateRunning)
	s.wg.Add(1)
	go s.syncLoop()

	// Start progress reporter
	s.wg.Add(1)
	go s.progressReporter()

	s.logger.Info("syncer started",
		zap.Uint64("start_block", startBlock),
		zap.Uint64("target_block", targetBlock))

	return nil
}

// Stop stops the syncer
func (s *Syncer) Stop(ctx context.Context) error {
	if !s.state.CompareAndSwap(stateRunning, stateStopping) {
		if s.state.Load() == stateStopped {
			return nil
		}
		return fmt.Errorf("syncer not running")
	}

	s.logger.Info("stopping syncer")
	s.cancel()

	// Stop components
	if err := s.downloader.Stop(ctx); err != nil {
		s.logger.Error("error stopping downloader", zap.Error(err))
	}

	if err := s.writer.Stop(ctx); err != nil {
		s.logger.Error("error stopping writer", zap.Error(err))
	}

	// Wait for goroutines
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("syncer stopped gracefully")
	case <-time.After(30 * time.Second):
		s.logger.Warn("syncer stop timeout")
	case <-ctx.Done():
		s.logger.Warn("syncer stop cancelled")
	}

	s.state.Store(stateStopped)
	close(s.doneCh)
	return nil
}

// syncLoop is the main synchronization loop
func (s *Syncer) syncLoop() {
	defer s.wg.Done()

	s.logger.Info("sync loop started")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("sync loop stopped by context")
			return
		case <-ticker.C:
			if err := s.syncBatch(); err != nil {
				if errors.Is(err, ErrNoValidPeers) {
					s.logger.Warn("no valid peers, waiting...")
					time.Sleep(5 * time.Second)
					continue
				}

				s.logger.Error("sync batch failed", zap.Error(err))
				s.metrics.IncrementErrors()
				
				// Backoff on error
				time.Sleep(2 * time.Second)
			}

			// Check if we've reached target
			current := s.currentBlock.Load()
			target := s.targetBlock.Load()
			if target > 0 && current >= target {
				s.logger.Info("sync completed",
					zap.Uint64("current", current),
					zap.Uint64("target", target))
				return
			}
		}
	}
}

// syncBatch syncs a batch of blocks
func (s *Syncer) syncBatch() error {
	current := s.currentBlock.Load()
	target := s.targetBlock.Load()

	if target > 0 && current >= target {
		return nil
	}

	// Calculate batch range
	batchSize := uint64(s.config.BatchSize)
	fromBlock := current + 1
	toBlock := fromBlock + batchSize - 1

	if target > 0 && toBlock > target {
		toBlock = target
	}

	s.logger.Debug("syncing batch",
		zap.Uint64("from", fromBlock),
		zap.Uint64("to", toBlock))

	// Download headers
	headers, err := s.downloader.DownloadHeaders(s.ctx, fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("failed to download headers: %w", err)
	}

	if len(headers) == 0 {
		return fmt.Errorf("no headers downloaded")
	}

	s.metrics.IncrementDownloadedHeaders(uint64(len(headers)))

	// Check for reorg on first header
	if reorg, err := s.validator.DetectReorg(s.ctx, headers[0]); err != nil {
		s.logger.Error("failed to detect reorg", zap.Error(err))
	} else if reorg {
		s.logger.Warn("reorg detected, handling...")
		if err := s.handleReorg(headers[0]); err != nil {
			return fmt.Errorf("failed to handle reorg: %w", err)
		}
		s.progress.IncrementReorgs(s.ctx)
		return nil // Retry after reorg
	}

	// Download bodies
	bodies, err := s.downloader.DownloadBodies(s.ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to download bodies: %w", err)
	}

	s.metrics.IncrementDownloadedBodies(uint64(len(bodies)))

	// Download receipts
	receipts, err := s.downloader.DownloadReceipts(s.ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to download receipts: %w", err)
	}

	s.metrics.IncrementDownloadedReceipts(uint64(len(receipts)))

	// Construct blocks
	blocks := make([]*types.Block, len(headers))
	for i, header := range headers {
		if bodies[i] == nil {
			return fmt.Errorf("body %d is nil", i)
		}
		blocks[i] = types.NewBlockWithHeader(header).WithBody(bodies[i].Transactions, bodies[i].Uncles)
	}

	// Validate blocks
	if err := s.validator.ValidateChain(s.ctx, blocks, receipts); err != nil {
		s.metrics.IncrementValidationErrors()
		return fmt.Errorf("validation failed: %w", err)
	}

	s.metrics.IncrementValidatedBlocks(uint64(len(blocks)))

	// Write blocks
	if err := s.writer.WriteBatchBlocks(s.ctx, blocks, receipts); err != nil {
		return fmt.Errorf("failed to write blocks: %w", err)
	}

	s.metrics.IncrementWrittenBlocks(uint64(len(blocks)))

	// Update progress
	lastBlock := blocks[len(blocks)-1]
	s.currentBlock.Store(lastBlock.NumberU64())
	
	if err := s.progress.UpdateProgress(s.ctx, lastBlock.NumberU64(), lastBlock.Hash()); err != nil {
		s.logger.Error("failed to update progress", zap.Error(err))
	}

	return nil
}

// handleReorg handles a blockchain reorganization
func (s *Syncer) handleReorg(newHeader *types.Header) error {
	s.logger.Warn("handling reorg", zap.Uint64("block", newHeader.Number.Uint64()))

	current := s.currentBlock.Load()
	
	// Find common ancestor by walking back
	commonAncestor := current - 1
	for commonAncestor > 0 {
		_, err := s.storage.LoadBlockHeader(s.ctx, commonAncestor)
		if err != nil {
			return fmt.Errorf("failed to load ancestor header: %w", err)
		}

		// Check if this is on the new chain
		// In a real implementation, we'd verify this properly
		// For now, assume we need to go back a few blocks
		if commonAncestor < current-10 {
			break
		}
		commonAncestor--
	}

	// Handle reorg in writer
	if err := s.writer.HandleReorg(s.ctx, current, commonAncestor); err != nil {
		return fmt.Errorf("failed to handle reorg in writer: %w", err)
	}

	// Update current block
	s.currentBlock.Store(commonAncestor)
	
	s.logger.Info("reorg handled",
		zap.Uint64("common_ancestor", commonAncestor),
		zap.Uint64("blocks_reverted", current-commonAncestor))

	return nil
}

// progressReporter periodically logs sync progress
func (s *Syncer) progressReporter() {
	defer s.wg.Done()

	ticker := time.NewTicker(DefaultProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.logProgress()
		}
	}
}

// logProgress logs the current sync progress
func (s *Syncer) logProgress() {
	current := s.currentBlock.Load()
	target := s.targetBlock.Load()
	
	stats := s.progress.GetStats()
	
	if target > 0 {
		s.logger.Info("sync progress",
			zap.Uint64("current", current),
			zap.Uint64("target", target),
			zap.Float64("percent", s.progress.ProgressPercent()),
			zap.Float64("blocks_per_sec", s.progress.BlocksPerSecond()),
			zap.String("eta", s.progress.EstimatedTimeRemaining().Round(time.Second).String()),
			zap.Any("stats", stats))
	} else {
		s.logger.Info("sync progress",
			zap.Uint64("current", current),
			zap.Float64("blocks_per_sec", s.progress.BlocksPerSecond()),
			zap.Any("stats", stats))
	}

	s.metrics.UpdateProgress(current, target)
}

// GetProgress returns the current sync progress
func (s *Syncer) GetProgress() *SyncProgress {
	return s.progress.GetProgress()
}

// GetStats returns detailed syncer statistics
func (s *Syncer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"state":         s.getStateName(),
		"current_block": s.currentBlock.Load(),
		"target_block":  s.targetBlock.Load(),
		"progress":      s.progress.GetStats(),
		"downloader":    s.downloader.GetStats(),
		"validator":     s.validator.GetStats(),
		"writer":        s.writer.GetStats(),
		"metrics":       s.metrics.GetStats(),
	}
}

// SetTargetBlock sets the target block for syncing
func (s *Syncer) SetTargetBlock(target uint64) error {
	s.targetBlock.Store(target)
	return s.progress.SetTargetBlock(s.ctx, target)
}

// Pause pauses the syncer
func (s *Syncer) Pause() error {
	if !s.state.CompareAndSwap(stateRunning, statePaused) {
		return fmt.Errorf("syncer not running")
	}

	s.logger.Info("syncer paused")
	return nil
}

// Resume resumes the syncer
func (s *Syncer) Resume() error {
	if !s.state.CompareAndSwap(statePaused, stateRunning) {
		return fmt.Errorf("syncer not paused")
	}

	s.logger.Info("syncer resumed")
	return nil
}

// IsRunning returns true if the syncer is running
func (s *Syncer) IsRunning() bool {
	return s.state.Load() == stateRunning
}

// IsSynced returns true if the syncer has reached the target block
func (s *Syncer) IsSynced() bool {
	current := s.currentBlock.Load()
	target := s.targetBlock.Load()
	return target > 0 && current >= target
}

// Done returns a channel that is closed when the syncer stops
func (s *Syncer) Done() <-chan struct{} {
	return s.doneCh
}

// Errors returns a channel for receiving sync errors
func (s *Syncer) Errors() <-chan error {
	return s.errorCh
}

func (s *Syncer) getStateName() string {
	switch s.state.Load() {
	case stateStopped:
		return "stopped"
	case stateStarting:
		return "starting"
	case stateRunning:
		return "running"
	case statePaused:
		return "paused"
	case stateStopping:
		return "stopping"
	default:
		return "unknown"
	}
}
