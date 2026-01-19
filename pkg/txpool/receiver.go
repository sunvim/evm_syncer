package txpool

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sunvim/evm_syncer/pkg/p2p"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"go.uber.org/zap"
)

const (
	// Maximum number of transactions to process in a single batch
	MaxBatchSize = 100
	
	// Channel buffer size for incoming transactions
	TxChannelBuffer = 1000
)

var (
	// ErrDuplicateTransaction is returned when transaction already exists
	ErrDuplicateTransaction = errors.New("duplicate transaction")
)

// Receiver receives transactions from P2P network, validates, and stores them
type Receiver struct {
	storage     *storage.TxPoolStorage
	pika        *storage.PikaClient
	validator   *Validator
	logger      *zap.Logger
	
	// Transaction input channel
	txChan chan *ReceivedTransaction
	
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ReceivedTransaction represents a transaction received from a peer
type ReceivedTransaction struct {
	Tx     *types.Transaction
	PeerID string
}

// NewReceiver creates a new transaction receiver
func NewReceiver(
	pikaClient *storage.PikaClient,
	chainID *big.Int,
	logger *zap.Logger,
) *Receiver {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Receiver{
		storage:   storage.NewTxPoolStorage(pikaClient),
		pika:      pikaClient,
		validator: NewValidator(chainID, logger),
		logger:    logger.With(zap.String("component", "receiver")),
		txChan:    make(chan *ReceivedTransaction, TxChannelBuffer),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start starts the receiver workers
func (r *Receiver) Start() error {
	r.logger.Info("starting transaction receiver")

	// Start processing goroutine
	r.wg.Add(1)
	go r.processTransactions()

	return nil
}

// Stop stops the receiver gracefully
func (r *Receiver) Stop() error {
	r.logger.Info("stopping transaction receiver")
	r.cancel()
	close(r.txChan)
	r.wg.Wait()
	r.logger.Info("transaction receiver stopped")
	return nil
}

// HandleTransaction receives a transaction from a P2P peer
func (r *Receiver) HandleTransaction(tx *types.Transaction, peerID string) error {
	if tx == nil {
		return ErrNilTransaction
	}

	select {
	case r.txChan <- &ReceivedTransaction{Tx: tx, PeerID: peerID}:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		r.logger.Warn("transaction channel full, dropping transaction",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.String("peer_id", peerID))
		return fmt.Errorf("transaction channel full")
	}
}

// HandleTransactionBatch receives multiple transactions from a P2P peer
func (r *Receiver) HandleTransactionBatch(txs []*types.Transaction, peerID string) error {
	if len(txs) == 0 {
		return nil
	}

	if len(txs) > MaxBatchSize {
		r.logger.Warn("transaction batch too large",
			zap.Int("size", len(txs)),
			zap.Int("max_size", MaxBatchSize),
			zap.String("peer_id", peerID))
		txs = txs[:MaxBatchSize]
	}

	for _, tx := range txs {
		if err := r.HandleTransaction(tx, peerID); err != nil {
			r.logger.Debug("failed to queue transaction",
				zap.String("tx_hash", tx.Hash().Hex()),
				zap.Error(err))
		}
	}

	return nil
}

// processTransactions processes transactions from the input channel
func (r *Receiver) processTransactions() {
	defer r.wg.Done()
	
	r.logger.Info("transaction processor started")

	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("transaction processor stopped")
			return
		case rxTx, ok := <-r.txChan:
			if !ok {
				return
			}
			r.processTransaction(rxTx)
		}
	}
}

// processTransaction validates and stores a single transaction
func (r *Receiver) processTransaction(rxTx *ReceivedTransaction) {
	tx := rxTx.Tx
	peerID := rxTx.PeerID
	txHash := tx.Hash()

	txLogger := r.logger.With(
		zap.String("tx_hash", txHash.Hex()),
		zap.String("peer_id", peerID),
	)

	// Check if transaction already exists in pending pool
	isPending, err := r.storage.IsPending(r.ctx, txHash)
	if err != nil {
		txLogger.Error("failed to check pending status", zap.Error(err))
		return
	}

	if isPending {
		txLogger.Debug("transaction already in pending pool, skipping")
		return
	}

	// Check if transaction already broadcasted
	isBroadcasted, err := r.storage.IsBroadcasted(r.ctx, txHash)
	if err != nil {
		txLogger.Error("failed to check broadcasted status", zap.Error(err))
		return
	}

	if isBroadcasted {
		txLogger.Debug("transaction already broadcasted, skipping")
		return
	}

	// Validate transaction
	from, err := r.validator.ValidateTransaction(tx)
	if err != nil {
		txLogger.Warn("transaction validation failed", zap.Error(err))
		return
	}

	// Add to pending pool
	if err := r.storage.AddPendingTransaction(r.ctx, tx, from); err != nil {
		txLogger.Error("failed to add transaction to pending pool", zap.Error(err))
		return
	}

	txLogger.Info("transaction received and validated",
		zap.String("from", from.Hex()),
		zap.Uint64("nonce", tx.Nonce()),
		zap.Uint64("gas", tx.Gas()),
		zap.String("gas_price", tx.GasPrice().String()),
	)

	// Enqueue for gossip forwarding (re-broadcast to other peers)
	if err := r.storage.EnqueueForBroadcast(r.ctx, txHash); err != nil {
		txLogger.Error("failed to enqueue transaction for broadcast", zap.Error(err))
		return
	}

	txLogger.Debug("transaction enqueued for gossip forwarding")
}

// HandleP2PMessage handles incoming P2P messages containing transactions
func (r *Receiver) HandleP2PMessage(msg p2p.Packet, peerID string) error {
	switch msg.Code {
	case p2p.TransactionsMsg:
		// Handle transaction broadcast
		txPacket, ok := msg.Data.(p2p.TransactionsPacket)
		if !ok {
			return fmt.Errorf("invalid transactions packet type")
		}

		return r.HandleTransactionBatch(txPacket, peerID)

	default:
		return fmt.Errorf("unsupported message code: %d", msg.Code)
	}
}

// GetChannelStats returns statistics about the receiver channel
func (r *Receiver) GetChannelStats() map[string]interface{} {
	return map[string]interface{}{
		"channel_len": len(r.txChan),
		"channel_cap": cap(r.txChan),
		"channel_usage": float64(len(r.txChan)) / float64(cap(r.txChan)) * 100,
	}
}

// GetStats returns receiver statistics
func (r *Receiver) GetStats(ctx context.Context) (map[string]interface{}, error) {
	poolStats, err := r.storage.GetPoolStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool stats: %w", err)
	}

	channelStats := r.GetChannelStats()
	
	stats := map[string]interface{}{
		"channel":    channelStats,
		"pool_stats": poolStats,
	}

	return stats, nil
}

// ProcessReceivedTransactions processes a slice of transactions from P2P
// This is a synchronous method useful for testing or direct processing
func (r *Receiver) ProcessReceivedTransactions(txs []*types.Transaction, peerID string) []error {
	if len(txs) == 0 {
		return nil
	}

	errors := make([]error, len(txs))
	
	for i, tx := range txs {
		txHash := tx.Hash()

		// Check duplicates
		isPending, err := r.storage.IsPending(r.ctx, txHash)
		if err != nil {
			errors[i] = fmt.Errorf("pending check failed: %w", err)
			continue
		}
		if isPending {
			errors[i] = ErrDuplicateTransaction
			continue
		}

		isBroadcasted, err := r.storage.IsBroadcasted(r.ctx, txHash)
		if err != nil {
			errors[i] = fmt.Errorf("broadcast check failed: %w", err)
			continue
		}
		if isBroadcasted {
			errors[i] = ErrDuplicateTransaction
			continue
		}

		// Validate
		from, err := r.validator.ValidateTransaction(tx)
		if err != nil {
			errors[i] = fmt.Errorf("validation failed: %w", err)
			continue
		}

		// Store
		if err := r.storage.AddPendingTransaction(r.ctx, tx, from); err != nil {
			errors[i] = fmt.Errorf("storage failed: %w", err)
			continue
		}

		// Enqueue for broadcast
		if err := r.storage.EnqueueForBroadcast(r.ctx, txHash); err != nil {
			r.logger.Warn("failed to enqueue for broadcast",
				zap.String("tx_hash", txHash.Hex()),
				zap.Error(err))
		}

		r.logger.Info("transaction processed",
			zap.String("tx_hash", txHash.Hex()),
			zap.String("from", from.Hex()),
			zap.String("peer_id", peerID))
	}

	return errors
}

// RemoveConfirmedTransaction removes a transaction after it's confirmed in a block
func (r *Receiver) RemoveConfirmedTransaction(ctx context.Context, txHash common.Hash) error {
	return r.storage.ConfirmTransaction(ctx, txHash)
}

// RemoveConfirmedTransactionBatch removes multiple confirmed transactions
func (r *Receiver) RemoveConfirmedTransactionBatch(ctx context.Context, txHashes []common.Hash) error {
	return r.storage.BatchConfirmTransactions(ctx, txHashes)
}
