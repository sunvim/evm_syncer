package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
)

// Key prefixes for transaction pool data
const (
	KeyPrefixPoolPending      = "pool:pending:"      // Pending transactions
	KeyPrefixPoolBroadcasted  = "pool:broadcasted:"  // Broadcasted transactions
	KeyPoolBroadcastQueue     = "pool:broadcast:queue" // Broadcast queue (list)
	KeyPoolPendingSet         = "pool:pending:set"   // Set of all pending tx hashes
	KeyPoolBroadcastedSet     = "pool:broadcasted:set" // Set of all broadcasted tx hashes
)

// TxPoolMetadata contains metadata about a transaction in the pool
type TxPoolMetadata struct {
	Hash             common.Hash `json:"hash"`
	From             common.Address `json:"from"`
	To               *common.Address `json:"to,omitempty"`
	Nonce            uint64 `json:"nonce"`
	Value            string `json:"value"` // String representation to handle big numbers
	GasPrice         string `json:"gas_price"`
	Gas              uint64 `json:"gas"`
	AddedAt          time.Time `json:"added_at"`
	BroadcastedAt    *time.Time `json:"broadcasted_at,omitempty"`
	BroadcastCount   int `json:"broadcast_count"`
	LastBroadcastAt  *time.Time `json:"last_broadcast_at,omitempty"`
}

// TxPoolStorage handles storage and retrieval of transaction pool data
type TxPoolStorage struct {
	client *PikaClient
	logger *zap.Logger
}

// NewTxPoolStorage creates a new transaction pool storage instance
func NewTxPoolStorage(client *PikaClient) *TxPoolStorage {
	return &TxPoolStorage{
		client: client,
		logger: client.logger.With(zap.String("storage", "txpool")),
	}
}

// AddPendingTransaction adds a transaction to the pending pool
func (ts *TxPoolStorage) AddPendingTransaction(ctx context.Context, tx *types.Transaction, from common.Address) error {
	if tx == nil {
		return fmt.Errorf("transaction cannot be nil")
	}

	// Encode transaction using RLP
	txData, err := rlp.EncodeToBytes(tx)
	if err != nil {
		ts.logger.Error("failed to encode transaction",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Error(err))
		return fmt.Errorf("encode transaction: %w", err)
	}

	// Create metadata
	metadata := TxPoolMetadata{
		Hash:           tx.Hash(),
		From:           from,
		To:             tx.To(),
		Nonce:          tx.Nonce(),
		Value:          tx.Value().String(),
		GasPrice:       tx.GasPrice().String(),
		Gas:            tx.Gas(),
		AddedAt:        time.Now(),
		BroadcastCount: 0,
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}

	// Store transaction and metadata using pipeline
	pipe := ts.client.Pipeline()
	txKey := ts.pendingTxKey(tx.Hash())
	
	// Store transaction data with metadata as hash fields
	pipe.HSet(ctx, txKey, "tx", txData)
	pipe.HSet(ctx, txKey, "metadata", metadataJSON)
	
	// Add to pending set
	pipe.SAdd(ctx, KeyPoolPendingSet, tx.Hash().Hex())

	if _, err := pipe.Exec(ctx); err != nil {
		ts.logger.Error("failed to add pending transaction",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Error(err))
		return fmt.Errorf("execute pipeline: %w", err)
	}

	ts.logger.Debug("added pending transaction",
		zap.String("tx_hash", tx.Hash().Hex()),
		zap.String("from", from.Hex()),
		zap.Uint64("nonce", tx.Nonce()))

	return nil
}

// GetPendingTransaction retrieves a pending transaction by hash
func (ts *TxPoolStorage) GetPendingTransaction(ctx context.Context, txHash common.Hash) (*types.Transaction, *TxPoolMetadata, error) {
	txKey := ts.pendingTxKey(txHash)
	
	// Get transaction data and metadata
	data, err := ts.client.HGetAll(ctx, txKey)
	if err != nil {
		return nil, nil, fmt.Errorf("get pending transaction: %w", err)
	}

	if len(data) == 0 {
		return nil, nil, fmt.Errorf("transaction not found: %s", txHash.Hex())
	}

	// Decode transaction
	var tx types.Transaction
	if err := rlp.DecodeBytes([]byte(data["tx"]), &tx); err != nil {
		ts.logger.Error("failed to decode transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return nil, nil, fmt.Errorf("decode transaction: %w", err)
	}

	// Decode metadata
	var metadata TxPoolMetadata
	if err := json.Unmarshal([]byte(data["metadata"]), &metadata); err != nil {
		ts.logger.Error("failed to decode metadata",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return nil, nil, fmt.Errorf("decode metadata: %w", err)
	}

	return &tx, &metadata, nil
}

// RemovePendingTransaction removes a transaction from the pending pool
func (ts *TxPoolStorage) RemovePendingTransaction(ctx context.Context, txHash common.Hash) error {
	pipe := ts.client.Pipeline()
	
	txKey := ts.pendingTxKey(txHash)
	pipe.Del(ctx, txKey)
	pipe.SRem(ctx, KeyPoolPendingSet, txHash.Hex())

	if _, err := pipe.Exec(ctx); err != nil {
		ts.logger.Error("failed to remove pending transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return fmt.Errorf("execute pipeline: %w", err)
	}

	ts.logger.Debug("removed pending transaction",
		zap.String("tx_hash", txHash.Hex()))

	return nil
}

// GetAllPendingTransactions retrieves all pending transaction hashes
func (ts *TxPoolStorage) GetAllPendingTransactions(ctx context.Context) ([]common.Hash, error) {
	// Use SMEMBERS to get all pending transaction hashes
	result := ts.client.Client().SMembers(ctx, KeyPoolPendingSet)
	if err := result.Err(); err != nil {
		ts.logger.Error("failed to get pending transactions", zap.Error(err))
		return nil, fmt.Errorf("get pending transactions: %w", err)
	}

	hashes := make([]common.Hash, 0, len(result.Val()))
	for _, hashStr := range result.Val() {
		hashes = append(hashes, common.HexToHash(hashStr))
	}

	return hashes, nil
}

// GetPendingTransactionCount returns the number of pending transactions
func (ts *TxPoolStorage) GetPendingTransactionCount(ctx context.Context) (int64, error) {
	result := ts.client.Client().SCard(ctx, KeyPoolPendingSet)
	if err := result.Err(); err != nil {
		return 0, fmt.Errorf("get pending count: %w", err)
	}
	return result.Val(), nil
}

// EnqueueForBroadcast adds a transaction hash to the broadcast queue
func (ts *TxPoolStorage) EnqueueForBroadcast(ctx context.Context, txHash common.Hash) error {
	if err := ts.client.RPush(ctx, KeyPoolBroadcastQueue, txHash.Hex()); err != nil {
		ts.logger.Error("failed to enqueue transaction for broadcast",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return fmt.Errorf("enqueue for broadcast: %w", err)
	}

	ts.logger.Debug("enqueued transaction for broadcast",
		zap.String("tx_hash", txHash.Hex()))

	return nil
}

// DequeueFromBroadcast removes and returns a transaction hash from the broadcast queue
func (ts *TxPoolStorage) DequeueFromBroadcast(ctx context.Context) (common.Hash, error) {
	hashStr, err := ts.client.LPop(ctx, KeyPoolBroadcastQueue)
	if err != nil {
		return common.Hash{}, fmt.Errorf("dequeue from broadcast: %w", err)
	}

	return common.HexToHash(hashStr), nil
}

// GetBroadcastQueueLength returns the length of the broadcast queue
func (ts *TxPoolStorage) GetBroadcastQueueLength(ctx context.Context) (int64, error) {
	length, err := ts.client.LLen(ctx, KeyPoolBroadcastQueue)
	if err != nil {
		return 0, fmt.Errorf("get broadcast queue length: %w", err)
	}
	return length, nil
}

// MarkAsBroadcasted marks a transaction as broadcasted
func (ts *TxPoolStorage) MarkAsBroadcasted(ctx context.Context, txHash common.Hash) error {
	// Get current metadata from pending
	txKey := ts.pendingTxKey(txHash)
	data, err := ts.client.HGetAll(ctx, txKey)
	if err != nil {
		return fmt.Errorf("get pending transaction: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("transaction not found in pending pool: %s", txHash.Hex())
	}

	// Update metadata
	var metadata TxPoolMetadata
	if err := json.Unmarshal([]byte(data["metadata"]), &metadata); err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}

	now := time.Now()
	metadata.BroadcastCount++
	metadata.LastBroadcastAt = &now
	if metadata.BroadcastedAt == nil {
		metadata.BroadcastedAt = &now
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}

	// Move to broadcasted set and update metadata
	pipe := ts.client.Pipeline()
	
	// Update metadata in pending
	pipe.HSet(ctx, txKey, "metadata", metadataJSON)
	
	// Copy to broadcasted
	broadcastedKey := ts.broadcastedTxKey(txHash)
	pipe.HSet(ctx, broadcastedKey, "tx", data["tx"])
	pipe.HSet(ctx, broadcastedKey, "metadata", metadataJSON)
	pipe.SAdd(ctx, KeyPoolBroadcastedSet, txHash.Hex())

	if _, err := pipe.Exec(ctx); err != nil {
		ts.logger.Error("failed to mark transaction as broadcasted",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return fmt.Errorf("execute pipeline: %w", err)
	}

	ts.logger.Debug("marked transaction as broadcasted",
		zap.String("tx_hash", txHash.Hex()),
		zap.Int("broadcast_count", metadata.BroadcastCount))

	return nil
}

// GetBroadcastedTransaction retrieves a broadcasted transaction by hash
func (ts *TxPoolStorage) GetBroadcastedTransaction(ctx context.Context, txHash common.Hash) (*types.Transaction, *TxPoolMetadata, error) {
	txKey := ts.broadcastedTxKey(txHash)
	
	data, err := ts.client.HGetAll(ctx, txKey)
	if err != nil {
		return nil, nil, fmt.Errorf("get broadcasted transaction: %w", err)
	}

	if len(data) == 0 {
		return nil, nil, fmt.Errorf("transaction not found in broadcasted pool: %s", txHash.Hex())
	}

	// Decode transaction
	var tx types.Transaction
	if err := rlp.DecodeBytes([]byte(data["tx"]), &tx); err != nil {
		ts.logger.Error("failed to decode transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return nil, nil, fmt.Errorf("decode transaction: %w", err)
	}

	// Decode metadata
	var metadata TxPoolMetadata
	if err := json.Unmarshal([]byte(data["metadata"]), &metadata); err != nil {
		ts.logger.Error("failed to decode metadata",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return nil, nil, fmt.Errorf("decode metadata: %w", err)
	}

	return &tx, &metadata, nil
}

// RemoveBroadcastedTransaction removes a transaction from the broadcasted pool
func (ts *TxPoolStorage) RemoveBroadcastedTransaction(ctx context.Context, txHash common.Hash) error {
	pipe := ts.client.Pipeline()
	
	txKey := ts.broadcastedTxKey(txHash)
	pipe.Del(ctx, txKey)
	pipe.SRem(ctx, KeyPoolBroadcastedSet, txHash.Hex())

	if _, err := pipe.Exec(ctx); err != nil {
		ts.logger.Error("failed to remove broadcasted transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return fmt.Errorf("execute pipeline: %w", err)
	}

	ts.logger.Debug("removed broadcasted transaction",
		zap.String("tx_hash", txHash.Hex()))

	return nil
}

// GetAllBroadcastedTransactions retrieves all broadcasted transaction hashes
func (ts *TxPoolStorage) GetAllBroadcastedTransactions(ctx context.Context) ([]common.Hash, error) {
	result := ts.client.Client().SMembers(ctx, KeyPoolBroadcastedSet)
	if err := result.Err(); err != nil {
		ts.logger.Error("failed to get broadcasted transactions", zap.Error(err))
		return nil, fmt.Errorf("get broadcasted transactions: %w", err)
	}

	hashes := make([]common.Hash, 0, len(result.Val()))
	for _, hashStr := range result.Val() {
		hashes = append(hashes, common.HexToHash(hashStr))
	}

	return hashes, nil
}

// GetBroadcastedTransactionCount returns the number of broadcasted transactions
func (ts *TxPoolStorage) GetBroadcastedTransactionCount(ctx context.Context) (int64, error) {
	result := ts.client.Client().SCard(ctx, KeyPoolBroadcastedSet)
	if err := result.Err(); err != nil {
		return 0, fmt.Errorf("get broadcasted count: %w", err)
	}
	return result.Val(), nil
}

// IsPending checks if a transaction is in the pending pool
func (ts *TxPoolStorage) IsPending(ctx context.Context, txHash common.Hash) (bool, error) {
	isMember, err := ts.client.SIsMember(ctx, KeyPoolPendingSet, txHash.Hex())
	if err != nil {
		return false, fmt.Errorf("check pending status: %w", err)
	}
	return isMember, nil
}

// IsBroadcasted checks if a transaction has been broadcasted
func (ts *TxPoolStorage) IsBroadcasted(ctx context.Context, txHash common.Hash) (bool, error) {
	isMember, err := ts.client.SIsMember(ctx, KeyPoolBroadcastedSet, txHash.Hex())
	if err != nil {
		return false, fmt.Errorf("check broadcasted status: %w", err)
	}
	return isMember, nil
}

// ConfirmTransaction removes a transaction from both pending and broadcasted pools
// This should be called when a transaction is confirmed in a block
func (ts *TxPoolStorage) ConfirmTransaction(ctx context.Context, txHash common.Hash) error {
	pipe := ts.client.Pipeline()
	
	// Remove from pending
	pendingKey := ts.pendingTxKey(txHash)
	pipe.Del(ctx, pendingKey)
	pipe.SRem(ctx, KeyPoolPendingSet, txHash.Hex())
	
	// Remove from broadcasted
	broadcastedKey := ts.broadcastedTxKey(txHash)
	pipe.Del(ctx, broadcastedKey)
	pipe.SRem(ctx, KeyPoolBroadcastedSet, txHash.Hex())

	if _, err := pipe.Exec(ctx); err != nil {
		ts.logger.Error("failed to confirm transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return fmt.Errorf("execute pipeline: %w", err)
	}

	ts.logger.Info("confirmed transaction",
		zap.String("tx_hash", txHash.Hex()))

	return nil
}

// BatchConfirmTransactions removes multiple transactions from pools
// This is useful for confirming all transactions in a block at once
func (ts *TxPoolStorage) BatchConfirmTransactions(ctx context.Context, txHashes []common.Hash) error {
	if len(txHashes) == 0 {
		return nil
	}

	pipe := ts.client.Pipeline()
	
	for _, txHash := range txHashes {
		pendingKey := ts.pendingTxKey(txHash)
		broadcastedKey := ts.broadcastedTxKey(txHash)
		
		pipe.Del(ctx, pendingKey)
		pipe.Del(ctx, broadcastedKey)
		pipe.SRem(ctx, KeyPoolPendingSet, txHash.Hex())
		pipe.SRem(ctx, KeyPoolBroadcastedSet, txHash.Hex())
	}

	if _, err := pipe.Exec(ctx); err != nil {
		ts.logger.Error("failed to batch confirm transactions",
			zap.Int("count", len(txHashes)),
			zap.Error(err))
		return fmt.Errorf("execute batch pipeline: %w", err)
	}

	ts.logger.Info("batch confirmed transactions",
		zap.Int("count", len(txHashes)))

	return nil
}

// CleanupOldTransactions removes transactions older than the specified duration
func (ts *TxPoolStorage) CleanupOldTransactions(ctx context.Context, maxAge time.Duration) (int, error) {
	cutoffTime := time.Now().Add(-maxAge)
	
	// Get all pending transactions
	pendingHashes, err := ts.GetAllPendingTransactions(ctx)
	if err != nil {
		return 0, fmt.Errorf("get pending transactions: %w", err)
	}

	removedCount := 0
	for _, txHash := range pendingHashes {
		_, metadata, err := ts.GetPendingTransaction(ctx, txHash)
		if err != nil {
			ts.logger.Warn("failed to get transaction metadata during cleanup",
				zap.String("tx_hash", txHash.Hex()),
				zap.Error(err))
			continue
		}

		if metadata.AddedAt.Before(cutoffTime) {
			if err := ts.RemovePendingTransaction(ctx, txHash); err != nil {
				ts.logger.Warn("failed to remove old transaction",
					zap.String("tx_hash", txHash.Hex()),
					zap.Error(err))
				continue
			}
			removedCount++
		}
	}

	// Also cleanup broadcasted transactions
	broadcastedHashes, err := ts.GetAllBroadcastedTransactions(ctx)
	if err != nil {
		return removedCount, fmt.Errorf("get broadcasted transactions: %w", err)
	}

	for _, txHash := range broadcastedHashes {
		_, metadata, err := ts.GetBroadcastedTransaction(ctx, txHash)
		if err != nil {
			ts.logger.Warn("failed to get broadcasted transaction metadata during cleanup",
				zap.String("tx_hash", txHash.Hex()),
				zap.Error(err))
			continue
		}

		if metadata.AddedAt.Before(cutoffTime) {
			if err := ts.RemoveBroadcastedTransaction(ctx, txHash); err != nil {
				ts.logger.Warn("failed to remove old broadcasted transaction",
					zap.String("tx_hash", txHash.Hex()),
					zap.Error(err))
				continue
			}
			removedCount++
		}
	}

	if removedCount > 0 {
		ts.logger.Info("cleaned up old transactions",
			zap.Int("removed_count", removedCount),
			zap.Duration("max_age", maxAge))
	}

	return removedCount, nil
}

// GetPoolStats returns statistics about the transaction pool
func (ts *TxPoolStorage) GetPoolStats(ctx context.Context) (map[string]interface{}, error) {
	pendingCount, err := ts.GetPendingTransactionCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pending count: %w", err)
	}

	broadcastedCount, err := ts.GetBroadcastedTransactionCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("get broadcasted count: %w", err)
	}

	queueLength, err := ts.GetBroadcastQueueLength(ctx)
	if err != nil {
		return nil, fmt.Errorf("get queue length: %w", err)
	}

	stats := map[string]interface{}{
		"pending_count":      pendingCount,
		"broadcasted_count":  broadcastedCount,
		"broadcast_queue_length": queueLength,
		"total_count":        pendingCount + broadcastedCount,
	}

	return stats, nil
}

// Helper functions to generate keys

func (ts *TxPoolStorage) pendingTxKey(txHash common.Hash) string {
	return fmt.Sprintf("%s%s", KeyPrefixPoolPending, txHash.Hex())
}

func (ts *TxPoolStorage) broadcastedTxKey(txHash common.Hash) string {
	return fmt.Sprintf("%s%s", KeyPrefixPoolBroadcasted, txHash.Hex())
}
