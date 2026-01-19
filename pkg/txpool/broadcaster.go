package txpool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/redis/go-redis/v9"
	"github.com/sunvim/evm_syncer/pkg/p2p"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"go.uber.org/zap"
)

const (
	// Number of concurrent broadcast workers
	BroadcastWorkers = 10
	
	// Timeout for blocking pop from queue
	BroadcastQueueTimeout = 5 * time.Second
	
	// TTL for broadcasted transaction tracking (5 minutes)
	BroadcastedTTL = 5 * time.Minute
	
	// Retry delay when queue is empty
	EmptyQueueRetryDelay = 1 * time.Second
)

// PeerManager interface for broadcasting to peers
type PeerManager interface {
	GetAllPeers() []*p2p.Peer
	BroadcastMessage(code uint64, data interface{}) error
}

// Broadcaster monitors the broadcast queue and broadcasts transactions to peers
type Broadcaster struct {
	storage     *storage.TxPoolStorage
	pika        *storage.PikaClient
	peerManager PeerManager
	logger      *zap.Logger
	
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBroadcaster creates a new transaction broadcaster
func NewBroadcaster(
	pikaClient *storage.PikaClient,
	peerManager PeerManager,
	logger *zap.Logger,
) *Broadcaster {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Broadcaster{
		storage:     storage.NewTxPoolStorage(pikaClient),
		pika:        pikaClient,
		peerManager: peerManager,
		logger:      logger.With(zap.String("component", "broadcaster")),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the broadcaster workers
func (b *Broadcaster) Start() error {
	b.logger.Info("starting transaction broadcaster",
		zap.Int("workers", BroadcastWorkers))

	// Start worker pool
	for i := 0; i < BroadcastWorkers; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}

	return nil
}

// Stop stops the broadcaster gracefully
func (b *Broadcaster) Stop() error {
	b.logger.Info("stopping transaction broadcaster")
	b.cancel()
	b.wg.Wait()
	b.logger.Info("transaction broadcaster stopped")
	return nil
}

// worker is a goroutine that processes transactions from the broadcast queue
func (b *Broadcaster) worker(id int) {
	defer b.wg.Done()
	
	workerLogger := b.logger.With(zap.Int("worker_id", id))
	workerLogger.Info("broadcast worker started")

	for {
		select {
		case <-b.ctx.Done():
			workerLogger.Info("broadcast worker stopped")
			return
		default:
			if err := b.processNextTransaction(workerLogger); err != nil {
				if err == redis.Nil {
					// Queue is empty, wait before retrying
					select {
					case <-b.ctx.Done():
						return
					case <-time.After(EmptyQueueRetryDelay):
						continue
					}
				}
				workerLogger.Error("failed to process transaction", zap.Error(err))
			}
		}
	}
}

// processNextTransaction fetches and broadcasts the next transaction from the queue
func (b *Broadcaster) processNextTransaction(logger *zap.Logger) error {
	// Block and wait for transaction from queue (BRPop)
	result := b.pika.Client().BRPop(b.ctx, BroadcastQueueTimeout, storage.KeyPoolBroadcastQueue)
	
	// Handle timeout
	if err := result.Err(); err != nil {
		if err == redis.Nil {
			return redis.Nil
		}
		return fmt.Errorf("failed to pop from broadcast queue: %w", err)
	}

	// BRPop returns [key, value]
	vals := result.Val()
	if len(vals) != 2 {
		return fmt.Errorf("unexpected BRPop result length: %d", len(vals))
	}

	txHashStr := vals[1]
	txHash := common.HexToHash(txHashStr)

	// Check if already broadcasted recently (avoid duplicates)
	broadcastKey := fmt.Sprintf("%s%s", storage.KeyPrefixPoolBroadcasted, txHash.Hex())
	exists, err := b.pika.Exists(b.ctx, broadcastKey)
	if err != nil {
		logger.Error("failed to check broadcast status",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
	} else if exists > 0 {
		logger.Debug("transaction already broadcasted recently, skipping",
			zap.String("tx_hash", txHash.Hex()))
		return nil
	}

	// Get transaction from pending pool
	tx, metadata, err := b.storage.GetPendingTransaction(b.ctx, txHash)
	if err != nil {
		logger.Warn("failed to get transaction from pending pool",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return nil // Don't retry, transaction might be confirmed
	}

	// Broadcast to all peers
	if err := b.broadcastTransaction(tx, logger); err != nil {
		logger.Error("failed to broadcast transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return err
	}

	// Mark as broadcasted with TTL
	if err := b.pika.Set(b.ctx, broadcastKey, "1", BroadcastedTTL); err != nil {
		logger.Error("failed to mark transaction as broadcasted",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
	}

	// Update metadata
	if err := b.storage.MarkAsBroadcasted(b.ctx, txHash); err != nil {
		logger.Error("failed to update broadcast metadata",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
	}

	logger.Info("transaction broadcasted",
		zap.String("tx_hash", txHash.Hex()),
		zap.String("from", metadata.From.Hex()),
		zap.Uint64("nonce", metadata.Nonce),
		zap.Int("broadcast_count", metadata.BroadcastCount+1))

	return nil
}

// broadcastTransaction sends the transaction to all connected peers
func (b *Broadcaster) broadcastTransaction(tx *types.Transaction, logger *zap.Logger) error {
	peers := b.peerManager.GetAllPeers()
	if len(peers) == 0 {
		logger.Warn("no peers available for broadcast",
			zap.String("tx_hash", tx.Hash().Hex()))
		return fmt.Errorf("no peers available")
	}

	// Create transaction packet
	txPacket := p2p.TransactionsPacket{tx}
	
	// Encode the packet
	data, err := rlp.EncodeToBytes(txPacket)
	if err != nil {
		return fmt.Errorf("failed to encode transaction packet: %w", err)
	}

	// Broadcast to all peers concurrently
	var wg sync.WaitGroup
	successCount := 0
	failureCount := 0
	var mu sync.Mutex

	for _, peer := range peers {
		if !peer.IsConnected() {
			continue
		}

		wg.Add(1)
		go func(p *p2p.Peer) {
			defer wg.Done()

			if err := p.SendMessage(p2p.TransactionsMsg, data); err != nil {
				logger.Debug("failed to send transaction to peer",
					zap.String("tx_hash", tx.Hash().Hex()),
					zap.String("peer_id", p.ID()),
					zap.Error(err))
				mu.Lock()
				failureCount++
				mu.Unlock()
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	logger.Debug("broadcast completed",
		zap.String("tx_hash", tx.Hash().Hex()),
		zap.Int("success", successCount),
		zap.Int("failures", failureCount),
		zap.Int("total_peers", len(peers)))

	if successCount == 0 {
		return fmt.Errorf("failed to broadcast to any peer")
	}

	return nil
}

// EnqueueTransaction adds a transaction hash to the broadcast queue
func (b *Broadcaster) EnqueueTransaction(ctx context.Context, txHash common.Hash) error {
	return b.storage.EnqueueForBroadcast(ctx, txHash)
}

// GetQueueLength returns the current broadcast queue length
func (b *Broadcaster) GetQueueLength(ctx context.Context) (int64, error) {
	return b.storage.GetBroadcastQueueLength(ctx)
}

// GetStats returns broadcaster statistics
func (b *Broadcaster) GetStats(ctx context.Context) (map[string]interface{}, error) {
	queueLength, err := b.GetQueueLength(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue length: %w", err)
	}

	stats := map[string]interface{}{
		"queue_length": queueLength,
		"workers":      BroadcastWorkers,
	}

	return stats, nil
}
