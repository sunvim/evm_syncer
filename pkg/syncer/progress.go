package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"go.uber.org/zap"
)

const (
	// Key for storing sync progress in Pika
	KeySyncProgress = "sync:progress"
	// Key for storing checkpoint data
	KeySyncCheckpoint = "sync:checkpoint"
)

// SyncProgress tracks the current sync progress
type SyncProgress struct {
	CurrentBlock  uint64      `json:"current_block"`
	TargetBlock   uint64      `json:"target_block"`
	StartBlock    uint64      `json:"start_block"`
	StartTime     time.Time   `json:"start_time"`
	LastCheckpoint time.Time  `json:"last_checkpoint"`
	LastBlockHash common.Hash `json:"last_block_hash"`
	Reorgs        uint64      `json:"reorgs"`
	TotalBlocks   uint64      `json:"total_blocks"`
}

// ProgressTracker manages sync progress tracking
type ProgressTracker struct {
	storage  *storage.BlockStorage
	pika     *storage.PikaClient
	logger   *zap.Logger
	progress *SyncProgress
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(blockStorage *storage.BlockStorage, pika *storage.PikaClient, logger *zap.Logger) *ProgressTracker {
	return &ProgressTracker{
		storage: blockStorage,
		pika:    pika,
		logger:  logger.With(zap.String("component", "progress")),
		progress: &SyncProgress{
			StartTime: time.Now(),
		},
	}
}

// LoadProgress loads the sync progress from Pika
func (pt *ProgressTracker) LoadProgress(ctx context.Context) (*SyncProgress, error) {
	data, err := pt.pika.GetBytes(ctx, KeySyncProgress)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			// No progress saved yet, start fresh
			pt.logger.Info("no existing progress found, starting fresh")
			return &SyncProgress{
				StartTime: time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to load progress: %w", err)
	}

	var progress SyncProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, fmt.Errorf("failed to unmarshal progress: %w", err)
	}

	pt.progress = &progress
	pt.logger.Info("loaded sync progress",
		zap.Uint64("current_block", progress.CurrentBlock),
		zap.Uint64("target_block", progress.TargetBlock),
		zap.Uint64("total_blocks", progress.TotalBlocks))

	return &progress, nil
}

// SaveProgress saves the current sync progress to Pika
func (pt *ProgressTracker) SaveProgress(ctx context.Context, progress *SyncProgress) error {
	data, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("failed to marshal progress: %w", err)
	}

	if err := pt.pika.Set(ctx, KeySyncProgress, data, 0); err != nil {
		return fmt.Errorf("failed to save progress: %w", err)
	}

	pt.progress = progress
	return nil
}

// UpdateProgress updates the current block and saves periodically
func (pt *ProgressTracker) UpdateProgress(ctx context.Context, currentBlock uint64, blockHash common.Hash) error {
	if pt.progress == nil {
		pt.progress = &SyncProgress{
			StartTime:  time.Now(),
			StartBlock: currentBlock,
		}
	}

	pt.progress.CurrentBlock = currentBlock
	pt.progress.LastBlockHash = blockHash
	pt.progress.TotalBlocks++

	// Save checkpoint every 1000 blocks or every 30 seconds
	shouldSave := (currentBlock%1000 == 0) || 
		time.Since(pt.progress.LastCheckpoint) > 30*time.Second

	if shouldSave {
		pt.progress.LastCheckpoint = time.Now()
		if err := pt.SaveProgress(ctx, pt.progress); err != nil {
			pt.logger.Error("failed to save progress checkpoint",
				zap.Uint64("block", currentBlock),
				zap.Error(err))
			return err
		}

		pt.logger.Info("progress checkpoint",
			zap.Uint64("current_block", currentBlock),
			zap.Uint64("target_block", pt.progress.TargetBlock),
			zap.Float64("percent", pt.ProgressPercent()),
			zap.Float64("blocks_per_sec", pt.BlocksPerSecond()))
	}

	return nil
}

// SetTargetBlock sets the target block number
func (pt *ProgressTracker) SetTargetBlock(ctx context.Context, targetBlock uint64) error {
	if pt.progress == nil {
		pt.progress = &SyncProgress{
			StartTime: time.Now(),
		}
	}

	pt.progress.TargetBlock = targetBlock
	return pt.SaveProgress(ctx, pt.progress)
}

// IncrementReorgs increments the reorg counter
func (pt *ProgressTracker) IncrementReorgs(ctx context.Context) error {
	if pt.progress == nil {
		pt.progress = &SyncProgress{
			StartTime: time.Now(),
		}
	}

	pt.progress.Reorgs++
	pt.logger.Warn("reorg detected", zap.Uint64("total_reorgs", pt.progress.Reorgs))
	return pt.SaveProgress(ctx, pt.progress)
}

// GetProgress returns the current progress
func (pt *ProgressTracker) GetProgress() *SyncProgress {
	if pt.progress == nil {
		return &SyncProgress{
			StartTime: time.Now(),
		}
	}
	return pt.progress
}

// ProgressPercent returns the sync progress as a percentage
func (pt *ProgressTracker) ProgressPercent() float64 {
	if pt.progress == nil || pt.progress.TargetBlock == 0 {
		return 0
	}

	if pt.progress.CurrentBlock >= pt.progress.TargetBlock {
		return 100.0
	}

	total := pt.progress.TargetBlock - pt.progress.StartBlock
	if total == 0 {
		return 0
	}

	current := pt.progress.CurrentBlock - pt.progress.StartBlock
	return (float64(current) / float64(total)) * 100.0
}

// BlocksPerSecond calculates the average sync speed
func (pt *ProgressTracker) BlocksPerSecond() float64 {
	if pt.progress == nil {
		return 0
	}

	elapsed := time.Since(pt.progress.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}

	return float64(pt.progress.TotalBlocks) / elapsed
}

// EstimatedTimeRemaining estimates remaining sync time
func (pt *ProgressTracker) EstimatedTimeRemaining() time.Duration {
	if pt.progress == nil || pt.progress.TargetBlock == 0 {
		return 0
	}

	bps := pt.BlocksPerSecond()
	if bps == 0 {
		return 0
	}

	remaining := pt.progress.TargetBlock - pt.progress.CurrentBlock
	if remaining == 0 {
		return 0
	}

	seconds := float64(remaining) / bps
	return time.Duration(seconds) * time.Second
}

// Reset resets the progress tracker
func (pt *ProgressTracker) Reset(ctx context.Context) error {
	pt.progress = &SyncProgress{
		StartTime: time.Now(),
	}
	return pt.SaveProgress(ctx, pt.progress)
}

// GetStats returns detailed progress statistics
func (pt *ProgressTracker) GetStats() map[string]interface{} {
	if pt.progress == nil {
		return map[string]interface{}{
			"current_block": 0,
			"target_block":  0,
			"percent":       0.0,
		}
	}

	return map[string]interface{}{
		"current_block":      pt.progress.CurrentBlock,
		"target_block":       pt.progress.TargetBlock,
		"start_block":        pt.progress.StartBlock,
		"percent":            pt.ProgressPercent(),
		"blocks_per_second":  pt.BlocksPerSecond(),
		"time_remaining":     pt.EstimatedTimeRemaining().String(),
		"total_blocks":       pt.progress.TotalBlocks,
		"reorgs":             pt.progress.Reorgs,
		"start_time":         pt.progress.StartTime,
		"last_checkpoint":    pt.progress.LastCheckpoint,
		"last_block_hash":    pt.progress.LastBlockHash.Hex(),
	}
}
