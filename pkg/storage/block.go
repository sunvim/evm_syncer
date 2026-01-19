package storage

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
)

// Key prefixes for different block data types
const (
	KeyPrefixBlockHeader  = "blk:hdr:"      // Block header
	KeyPrefixBlockBody    = "blk:body:"     // Block body (transactions + uncles)
	KeyPrefixBlockReceipt = "blk:rcpt:"     // Block receipts
	KeyPrefixTransaction  = "tx:"           // Individual transaction
	KeyIndexLatestBlock   = "idx:latest"    // Latest synced block number
	KeyPrefixBlockHash    = "blk:hash:"     // Block number to hash mapping
	KeyPrefixHashToNumber = "blk:num:"      // Block hash to number mapping
)

// BlockStorage handles storage and retrieval of blockchain data
type BlockStorage struct {
	client *PikaClient
	logger *zap.Logger
}

// NewBlockStorage creates a new block storage instance
func NewBlockStorage(client *PikaClient) *BlockStorage {
	return &BlockStorage{
		client: client,
		logger: client.logger.With(zap.String("storage", "block")),
	}
}

// SaveBlockHeader stores a block header using RLP encoding
func (bs *BlockStorage) SaveBlockHeader(ctx context.Context, header *types.Header) error {
	if header == nil {
		return fmt.Errorf("header cannot be nil")
	}

	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		bs.logger.Error("failed to encode block header",
			zap.Uint64("block_number", header.Number.Uint64()),
			zap.String("block_hash", header.Hash().Hex()),
			zap.Error(err))
		return fmt.Errorf("encode header: %w", err)
	}

	key := bs.blockHeaderKey(header.Number.Uint64())
	if err := bs.client.Set(ctx, key, data, 0); err != nil {
		return fmt.Errorf("save header: %w", err)
	}

	// Save block hash to number mapping
	hashKey := bs.hashToNumberKey(header.Hash())
	if err := bs.client.Set(ctx, hashKey, header.Number.Uint64(), 0); err != nil {
		return fmt.Errorf("save hash mapping: %w", err)
	}

	// Save block number to hash mapping
	numHashKey := bs.blockHashKey(header.Number.Uint64())
	if err := bs.client.Set(ctx, numHashKey, header.Hash().Hex(), 0); err != nil {
		return fmt.Errorf("save number mapping: %w", err)
	}

	bs.logger.Debug("saved block header",
		zap.Uint64("block_number", header.Number.Uint64()),
		zap.String("block_hash", header.Hash().Hex()))

	return nil
}

// LoadBlockHeader retrieves a block header by block number
func (bs *BlockStorage) LoadBlockHeader(ctx context.Context, blockNumber uint64) (*types.Header, error) {
	key := bs.blockHeaderKey(blockNumber)
	data, err := bs.client.GetBytes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load header: %w", err)
	}

	var header types.Header
	if err := rlp.DecodeBytes(data, &header); err != nil {
		bs.logger.Error("failed to decode block header",
			zap.Uint64("block_number", blockNumber),
			zap.Error(err))
		return nil, fmt.Errorf("decode header: %w", err)
	}

	return &header, nil
}

// LoadBlockHeaderByHash retrieves a block header by block hash
func (bs *BlockStorage) LoadBlockHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	// First get the block number from hash
	numKey := bs.hashToNumberKey(hash)
	numStr, err := bs.client.Get(ctx, numKey)
	if err != nil {
		return nil, fmt.Errorf("load block number: %w", err)
	}

	blockNum, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse block number: %w", err)
	}

	return bs.LoadBlockHeader(ctx, blockNum)
}

// SaveBlockBody stores a block body (transactions and uncles)
func (bs *BlockStorage) SaveBlockBody(ctx context.Context, blockNumber uint64, body *types.Body) error {
	if body == nil {
		return fmt.Errorf("body cannot be nil")
	}

	data, err := rlp.EncodeToBytes(body)
	if err != nil {
		bs.logger.Error("failed to encode block body",
			zap.Uint64("block_number", blockNumber),
			zap.Error(err))
		return fmt.Errorf("encode body: %w", err)
	}

	key := bs.blockBodyKey(blockNumber)
	if err := bs.client.Set(ctx, key, data, 0); err != nil {
		return fmt.Errorf("save body: %w", err)
	}

	bs.logger.Debug("saved block body",
		zap.Uint64("block_number", blockNumber),
		zap.Int("tx_count", len(body.Transactions)))

	return nil
}

// LoadBlockBody retrieves a block body by block number
func (bs *BlockStorage) LoadBlockBody(ctx context.Context, blockNumber uint64) (*types.Body, error) {
	key := bs.blockBodyKey(blockNumber)
	data, err := bs.client.GetBytes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load body: %w", err)
	}

	var body types.Body
	if err := rlp.DecodeBytes(data, &body); err != nil {
		bs.logger.Error("failed to decode block body",
			zap.Uint64("block_number", blockNumber),
			zap.Error(err))
		return nil, fmt.Errorf("decode body: %w", err)
	}

	return &body, nil
}

// SaveBlockReceipts stores block receipts
func (bs *BlockStorage) SaveBlockReceipts(ctx context.Context, blockNumber uint64, receipts types.Receipts) error {
	if receipts == nil {
		return fmt.Errorf("receipts cannot be nil")
	}

	data, err := rlp.EncodeToBytes(receipts)
	if err != nil {
		bs.logger.Error("failed to encode block receipts",
			zap.Uint64("block_number", blockNumber),
			zap.Error(err))
		return fmt.Errorf("encode receipts: %w", err)
	}

	key := bs.blockReceiptKey(blockNumber)
	if err := bs.client.Set(ctx, key, data, 0); err != nil {
		return fmt.Errorf("save receipts: %w", err)
	}

	bs.logger.Debug("saved block receipts",
		zap.Uint64("block_number", blockNumber),
		zap.Int("receipt_count", len(receipts)))

	return nil
}

// LoadBlockReceipts retrieves block receipts by block number
func (bs *BlockStorage) LoadBlockReceipts(ctx context.Context, blockNumber uint64) (types.Receipts, error) {
	key := bs.blockReceiptKey(blockNumber)
	data, err := bs.client.GetBytes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load receipts: %w", err)
	}

	var receipts types.Receipts
	if err := rlp.DecodeBytes(data, &receipts); err != nil {
		bs.logger.Error("failed to decode block receipts",
			zap.Uint64("block_number", blockNumber),
			zap.Error(err))
		return nil, fmt.Errorf("decode receipts: %w", err)
	}

	return receipts, nil
}

// SaveBlock stores a complete block (header, body, and receipts) atomically using pipeline
func (bs *BlockStorage) SaveBlock(ctx context.Context, block *types.Block, receipts types.Receipts) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	blockNumber := block.NumberU64()
	
	// Encode all data first
	headerData, err := rlp.EncodeToBytes(block.Header())
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	body := &types.Body{
		Transactions: block.Transactions(),
		Uncles:       block.Uncles(),
	}
	bodyData, err := rlp.EncodeToBytes(body)
	if err != nil {
		return fmt.Errorf("encode body: %w", err)
	}

	var receiptsData []byte
	if receipts != nil && len(receipts) > 0 {
		receiptsData, err = rlp.EncodeToBytes(receipts)
		if err != nil {
			return fmt.Errorf("encode receipts: %w", err)
		}
	}

	// Use pipeline for atomic batch write
	pipe := bs.client.Pipeline()
	
	pipe.Set(ctx, bs.blockHeaderKey(blockNumber), headerData, 0)
	pipe.Set(ctx, bs.blockBodyKey(blockNumber), bodyData, 0)
	pipe.Set(ctx, bs.hashToNumberKey(block.Hash()), blockNumber, 0)
	pipe.Set(ctx, bs.blockHashKey(blockNumber), block.Hash().Hex(), 0)
	
	if receiptsData != nil {
		pipe.Set(ctx, bs.blockReceiptKey(blockNumber), receiptsData, 0)
	}

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		bs.logger.Error("failed to save block",
			zap.Uint64("block_number", blockNumber),
			zap.String("block_hash", block.Hash().Hex()),
			zap.Error(err))
		return fmt.Errorf("execute pipeline: %w", err)
	}

	bs.logger.Info("saved complete block",
		zap.Uint64("block_number", blockNumber),
		zap.String("block_hash", block.Hash().Hex()),
		zap.Int("tx_count", len(block.Transactions())),
		zap.Int("receipt_count", len(receipts)))

	return nil
}

// LoadBlock retrieves a complete block (header and body)
func (bs *BlockStorage) LoadBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	header, err := bs.LoadBlockHeader(ctx, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("load header: %w", err)
	}

	body, err := bs.LoadBlockBody(ctx, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("load body: %w", err)
	}

	block := types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
	return block, nil
}

// SaveTransaction stores an individual transaction by hash
func (bs *BlockStorage) SaveTransaction(ctx context.Context, tx *types.Transaction) error {
	if tx == nil {
		return fmt.Errorf("transaction cannot be nil")
	}

	data, err := rlp.EncodeToBytes(tx)
	if err != nil {
		bs.logger.Error("failed to encode transaction",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Error(err))
		return fmt.Errorf("encode transaction: %w", err)
	}

	key := bs.transactionKey(tx.Hash())
	if err := bs.client.Set(ctx, key, data, 0); err != nil {
		return fmt.Errorf("save transaction: %w", err)
	}

	bs.logger.Debug("saved transaction",
		zap.String("tx_hash", tx.Hash().Hex()))

	return nil
}

// LoadTransaction retrieves a transaction by hash
func (bs *BlockStorage) LoadTransaction(ctx context.Context, txHash common.Hash) (*types.Transaction, error) {
	key := bs.transactionKey(txHash)
	data, err := bs.client.GetBytes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load transaction: %w", err)
	}

	var tx types.Transaction
	if err := rlp.DecodeBytes(data, &tx); err != nil {
		bs.logger.Error("failed to decode transaction",
			zap.String("tx_hash", txHash.Hex()),
			zap.Error(err))
		return nil, fmt.Errorf("decode transaction: %w", err)
	}

	return &tx, nil
}

// SaveLatestBlock updates the latest synced block number
func (bs *BlockStorage) SaveLatestBlock(ctx context.Context, blockNumber uint64) error {
	if err := bs.client.Set(ctx, KeyIndexLatestBlock, blockNumber, 0); err != nil {
		return fmt.Errorf("save latest block: %w", err)
	}

	bs.logger.Info("updated latest block",
		zap.Uint64("block_number", blockNumber))

	return nil
}

// LoadLatestBlock retrieves the latest synced block number
func (bs *BlockStorage) LoadLatestBlock(ctx context.Context) (uint64, error) {
	val, err := bs.client.Get(ctx, KeyIndexLatestBlock)
	if err != nil {
		return 0, fmt.Errorf("load latest block: %w", err)
	}

	blockNum, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse block number: %w", err)
	}

	return blockNum, nil
}

// DeleteBlock removes all data associated with a block
func (bs *BlockStorage) DeleteBlock(ctx context.Context, blockNumber uint64) error {
	// Load header first to get the hash
	header, err := bs.LoadBlockHeader(ctx, blockNumber)
	if err != nil {
		// If header doesn't exist, nothing to delete
		return nil
	}

	pipe := bs.client.Pipeline()
	pipe.Del(ctx, bs.blockHeaderKey(blockNumber))
	pipe.Del(ctx, bs.blockBodyKey(blockNumber))
	pipe.Del(ctx, bs.blockReceiptKey(blockNumber))
	pipe.Del(ctx, bs.hashToNumberKey(header.Hash()))
	pipe.Del(ctx, bs.blockHashKey(blockNumber))

	if _, err := pipe.Exec(ctx); err != nil {
		bs.logger.Error("failed to delete block",
			zap.Uint64("block_number", blockNumber),
			zap.Error(err))
		return fmt.Errorf("execute delete pipeline: %w", err)
	}

	bs.logger.Info("deleted block",
		zap.Uint64("block_number", blockNumber),
		zap.String("block_hash", header.Hash().Hex()))

	return nil
}

// BlockExists checks if a block exists in storage
func (bs *BlockStorage) BlockExists(ctx context.Context, blockNumber uint64) (bool, error) {
	key := bs.blockHeaderKey(blockNumber)
	count, err := bs.client.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check block existence: %w", err)
	}
	return count > 0, nil
}

// GetBlockHash retrieves the block hash for a given block number
func (bs *BlockStorage) GetBlockHash(ctx context.Context, blockNumber uint64) (common.Hash, error) {
	key := bs.blockHashKey(blockNumber)
	hashStr, err := bs.client.Get(ctx, key)
	if err != nil {
		return common.Hash{}, fmt.Errorf("get block hash: %w", err)
	}
	return common.HexToHash(hashStr), nil
}

// BatchSaveBlocks saves multiple blocks atomically using transaction pipeline
func (bs *BlockStorage) BatchSaveBlocks(ctx context.Context, blocks []*types.Block, receipts []types.Receipts) error {
	if len(blocks) != len(receipts) {
		return fmt.Errorf("blocks and receipts length mismatch: %d != %d", len(blocks), len(receipts))
	}

	pipe := bs.client.TxPipeline()

	for i, block := range blocks {
		if block == nil {
			continue
		}

		blockNumber := block.NumberU64()

		// Encode header
		headerData, err := rlp.EncodeToBytes(block.Header())
		if err != nil {
			return fmt.Errorf("encode header for block %d: %w", blockNumber, err)
		}

		// Encode body
		body := &types.Body{
			Transactions: block.Transactions(),
			Uncles:       block.Uncles(),
		}
		bodyData, err := rlp.EncodeToBytes(body)
		if err != nil {
			return fmt.Errorf("encode body for block %d: %w", blockNumber, err)
		}

		// Encode receipts
		var receiptsData []byte
		if receipts[i] != nil && len(receipts[i]) > 0 {
			receiptsData, err = rlp.EncodeToBytes(receipts[i])
			if err != nil {
				return fmt.Errorf("encode receipts for block %d: %w", blockNumber, err)
			}
		}

		// Add to pipeline
		pipe.Set(ctx, bs.blockHeaderKey(blockNumber), headerData, 0)
		pipe.Set(ctx, bs.blockBodyKey(blockNumber), bodyData, 0)
		pipe.Set(ctx, bs.hashToNumberKey(block.Hash()), blockNumber, 0)
		pipe.Set(ctx, bs.blockHashKey(blockNumber), block.Hash().Hex(), 0)
		
		if receiptsData != nil {
			pipe.Set(ctx, bs.blockReceiptKey(blockNumber), receiptsData, 0)
		}
	}

	// Execute transaction pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		bs.logger.Error("failed to batch save blocks",
			zap.Int("block_count", len(blocks)),
			zap.Error(err))
		return fmt.Errorf("execute batch pipeline: %w", err)
	}

	bs.logger.Info("batch saved blocks",
		zap.Int("block_count", len(blocks)))

	return nil
}

// Helper functions to generate keys

func (bs *BlockStorage) blockHeaderKey(blockNumber uint64) string {
	return fmt.Sprintf("%s%d", KeyPrefixBlockHeader, blockNumber)
}

func (bs *BlockStorage) blockBodyKey(blockNumber uint64) string {
	return fmt.Sprintf("%s%d", KeyPrefixBlockBody, blockNumber)
}

func (bs *BlockStorage) blockReceiptKey(blockNumber uint64) string {
	return fmt.Sprintf("%s%d", KeyPrefixBlockReceipt, blockNumber)
}

func (bs *BlockStorage) transactionKey(txHash common.Hash) string {
	return fmt.Sprintf("%s%s", KeyPrefixTransaction, txHash.Hex())
}

func (bs *BlockStorage) blockHashKey(blockNumber uint64) string {
	return fmt.Sprintf("%s%d", KeyPrefixBlockHash, blockNumber)
}

func (bs *BlockStorage) hashToNumberKey(hash common.Hash) string {
	return fmt.Sprintf("%s%s", KeyPrefixHashToNumber, hash.Hex())
}
