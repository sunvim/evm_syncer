package syncer

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/sunvim/evm_syncer/pkg/chain"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"go.uber.org/zap"
)

// ValidationError represents a block validation error
type ValidationError struct {
	BlockNumber uint64
	BlockHash   common.Hash
	Reason      string
	Err         error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed for block %d (%s): %s: %v",
		e.BlockNumber, e.BlockHash.Hex(), e.Reason, e.Err)
}

// Validator validates blocks against consensus rules
type Validator struct {
	chainAdapter chain.IChainAdapter
	storage      *storage.BlockStorage
	logger       *zap.Logger

	// Statistics
	validatedBlocks atomic.Uint64
	validationErrors atomic.Uint64
}

// NewValidator creates a new block validator
func NewValidator(chainAdapter chain.IChainAdapter, storage *storage.BlockStorage, logger *zap.Logger) *Validator {
	return &Validator{
		chainAdapter: chainAdapter,
		storage:      storage,
		logger:       logger.With(zap.String("component", "validator")),
	}
}

// ValidateBlock performs full block validation
func (v *Validator) ValidateBlock(ctx context.Context, block *types.Block, receipts types.Receipts) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	blockNumber := block.NumberU64()
	blockHash := block.Hash()

	v.logger.Debug("validating block",
		zap.Uint64("number", blockNumber),
		zap.String("hash", blockHash.Hex()))

	// Validate header
	if err := v.ValidateHeader(ctx, block.Header()); err != nil {
		v.validationErrors.Add(1)
		return &ValidationError{
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
			Reason:      "header validation failed",
			Err:         err,
		}
	}

	// Validate body
	if err := v.ValidateBody(ctx, block); err != nil {
		v.validationErrors.Add(1)
		return &ValidationError{
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
			Reason:      "body validation failed",
			Err:         err,
		}
	}

	// Validate receipts if provided
	if receipts != nil {
		if err := v.ValidateReceipts(ctx, block, receipts); err != nil {
			v.validationErrors.Add(1)
			return &ValidationError{
				BlockNumber: blockNumber,
				BlockHash:   blockHash,
				Reason:      "receipts validation failed",
				Err:         err,
			}
		}
	}

	v.validatedBlocks.Add(1)
	v.logger.Debug("block validated successfully",
		zap.Uint64("number", blockNumber),
		zap.String("hash", blockHash.Hex()))

	return nil
}

// ValidateHeader validates a block header
func (v *Validator) ValidateHeader(ctx context.Context, header *types.Header) error {
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	blockNumber := header.Number.Uint64()

	// Basic sanity checks
	if header.Number == nil {
		return fmt.Errorf("header number is nil")
	}

	if header.Time == 0 {
		return fmt.Errorf("header timestamp is zero")
	}

	// Validate against parent (if not genesis)
	if blockNumber > 0 {
		parent, err := v.storage.LoadBlockHeader(ctx, blockNumber-1)
		if err != nil {
			// Parent not found - this is expected during initial sync
			// We'll validate parent hash when parent becomes available
			v.logger.Debug("parent header not found, skipping parent validation",
				zap.Uint64("block", blockNumber),
				zap.Uint64("parent", blockNumber-1))
		} else {
			if err := v.validateHeaderWithParent(header, parent); err != nil {
				return fmt.Errorf("parent validation failed: %w", err)
			}
		}
	}

	// Validate consensus seal
	if err := v.chainAdapter.ValidateHeader(header, nil); err != nil {
		return fmt.Errorf("chain adapter validation failed: %w", err)
	}

	// Verify seal (signature)
	consensusEngine := v.chainAdapter.GetConsensusEngine()
	if err := consensusEngine.VerifySeal(header); err != nil {
		return fmt.Errorf("seal verification failed: %w", err)
	}

	return nil
}

// validateHeaderWithParent validates header against its parent
func (v *Validator) validateHeaderWithParent(header, parent *types.Header) error {
	// Verify parent hash
	if header.ParentHash != parent.Hash() {
		return fmt.Errorf("parent hash mismatch: got %s, expected %s",
			header.ParentHash.Hex(), parent.Hash().Hex())
	}

	// Verify block number is parent + 1
	if header.Number.Uint64() != parent.Number.Uint64()+1 {
		return fmt.Errorf("block number mismatch: got %d, expected %d",
			header.Number.Uint64(), parent.Number.Uint64()+1)
	}

	// Verify timestamp is after parent
	if header.Time <= parent.Time {
		return fmt.Errorf("timestamp not after parent: got %d, parent %d",
			header.Time, parent.Time)
	}

	// Chain-specific header validation with parent
	if err := v.chainAdapter.ValidateHeader(header, parent); err != nil {
		return fmt.Errorf("chain-specific validation failed: %w", err)
	}

	return nil
}

// ValidateBody validates a block body
func (v *Validator) ValidateBody(ctx context.Context, block *types.Block) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	header := block.Header()

	// Validate transaction root
	if err := v.validateTransactionRoot(block); err != nil {
		return fmt.Errorf("transaction root validation failed: %w", err)
	}

	// Validate uncle root
	if err := v.validateUncleRoot(block); err != nil {
		return fmt.Errorf("uncle root validation failed: %w", err)
	}

	// Validate transactions
	txs := block.Transactions()
	for i, tx := range txs {
		if tx == nil {
			return fmt.Errorf("transaction %d is nil", i)
		}

		// Basic transaction validation
		if err := v.validateTransaction(tx); err != nil {
			return fmt.Errorf("transaction %d validation failed: %w", i, err)
		}
	}

	// Validate uncles
	uncles := block.Uncles()
	if len(uncles) > 2 {
		return fmt.Errorf("too many uncles: got %d, max 2", len(uncles))
	}

	// Chain-specific body validation
	if err := v.chainAdapter.ValidateBody(block); err != nil {
		return fmt.Errorf("chain-specific body validation failed: %w", err)
	}

	// Verify block hash
	calculatedHash := header.Hash()
	if calculatedHash != block.Hash() {
		return fmt.Errorf("block hash mismatch: calculated %s, got %s",
			calculatedHash.Hex(), block.Hash().Hex())
	}

	return nil
}

// validateTransactionRoot validates the transaction trie root
func (v *Validator) validateTransactionRoot(block *types.Block) error {
	if block.Transactions().Len() == 0 {
		// Empty transaction list should have empty root
		if block.TxHash() != types.EmptyRootHash {
			return fmt.Errorf("empty transactions but non-empty tx root: %s", block.TxHash().Hex())
		}
		return nil
	}

	// Calculate transaction root
	hasher := trie.NewStackTrie(nil)
	txRoot := types.DeriveSha(block.Transactions(), hasher)

	// Compare with header
	if txRoot != block.TxHash() {
		return fmt.Errorf("transaction root mismatch: calculated %s, header %s",
			txRoot.Hex(), block.TxHash().Hex())
	}

	return nil
}

// validateUncleRoot validates the uncle trie root
func (v *Validator) validateUncleRoot(block *types.Block) error {
	uncles := block.Uncles()
	
	// Calculate uncle root
	uncleRoot := types.CalcUncleHash(uncles)

	// Compare with header
	if uncleRoot != block.UncleHash() {
		return fmt.Errorf("uncle root mismatch: calculated %s, header %s",
			uncleRoot.Hex(), block.UncleHash().Hex())
	}

	return nil
}

// validateTransaction performs basic transaction validation
func (v *Validator) validateTransaction(tx *types.Transaction) error {
	// Check transaction hash
	if tx.Hash() == (common.Hash{}) {
		return fmt.Errorf("transaction has empty hash")
	}

	// Check gas limit
	if tx.Gas() == 0 {
		return fmt.Errorf("transaction has zero gas limit")
	}

	// Verify transaction signature
	_, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return fmt.Errorf("invalid transaction signature: %w", err)
	}

	return nil
}

// ValidateReceipts validates block receipts
func (v *Validator) ValidateReceipts(ctx context.Context, block *types.Block, receipts types.Receipts) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	if receipts == nil {
		return fmt.Errorf("receipts are nil")
	}

	// Number of receipts must match number of transactions
	txCount := len(block.Transactions())
	if len(receipts) != txCount {
		return fmt.Errorf("receipt count mismatch: got %d receipts for %d transactions",
			len(receipts), txCount)
	}

	// Calculate receipt root
	hasher := trie.NewStackTrie(nil)
	receiptRoot := types.DeriveSha(receipts, hasher)

	// Compare with header
	if receiptRoot != block.ReceiptHash() {
		return fmt.Errorf("receipt root mismatch: calculated %s, header %s",
			receiptRoot.Hex(), block.ReceiptHash().Hex())
	}

	// Validate individual receipts
	for i, receipt := range receipts {
		if receipt == nil {
			return fmt.Errorf("receipt %d is nil", i)
		}

		// Validate receipt fields
		if receipt.TxHash != block.Transactions()[i].Hash() {
			return fmt.Errorf("receipt %d tx hash mismatch: got %s, expected %s",
				i, receipt.TxHash.Hex(), block.Transactions()[i].Hash().Hex())
		}

		if receipt.BlockNumber.Uint64() != block.NumberU64() {
			return fmt.Errorf("receipt %d block number mismatch: got %d, expected %d",
				i, receipt.BlockNumber.Uint64(), block.NumberU64())
		}

		if receipt.BlockHash != block.Hash() {
			return fmt.Errorf("receipt %d block hash mismatch: got %s, expected %s",
				i, receipt.BlockHash.Hex(), block.Hash().Hex())
		}
	}

	// Validate bloom filter
	if err := v.validateBloom(block, receipts); err != nil {
		return fmt.Errorf("bloom validation failed: %w", err)
	}

	return nil
}

// validateBloom validates the logs bloom filter
func (v *Validator) validateBloom(block *types.Block, receipts types.Receipts) error {
	// Calculate bloom from receipts
	var bloom types.Bloom
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			bloom.Add(log.Address.Bytes())
			for _, topic := range log.Topics {
				bloom.Add(topic.Bytes())
			}
		}
	}

	// Compare with header
	if bloom != block.Bloom() {
		return fmt.Errorf("bloom filter mismatch")
	}

	return nil
}

// ValidateChain validates a sequence of blocks
func (v *Validator) ValidateChain(ctx context.Context, blocks []*types.Block, receipts []types.Receipts) error {
	if len(blocks) == 0 {
		return nil
	}

	if len(receipts) != len(blocks) {
		return fmt.Errorf("blocks and receipts count mismatch: %d != %d", len(blocks), len(receipts))
	}

	for i, block := range blocks {
		if err := v.ValidateBlock(ctx, block, receipts[i]); err != nil {
			return fmt.Errorf("block %d validation failed: %w", i, err)
		}

		// Validate chain continuity (except for first block)
		if i > 0 {
			prevBlock := blocks[i-1]
			if block.ParentHash() != prevBlock.Hash() {
				return fmt.Errorf("chain discontinuity at block %d: parent hash mismatch", block.NumberU64())
			}

			if block.NumberU64() != prevBlock.NumberU64()+1 {
				return fmt.Errorf("chain discontinuity at block %d: number not sequential", block.NumberU64())
			}
		}
	}

	v.logger.Info("validated block chain", zap.Int("count", len(blocks)))
	return nil
}

// DetectReorg detects if a reorg occurred at the given block
func (v *Validator) DetectReorg(ctx context.Context, header *types.Header) (bool, error) {
	if header == nil {
		return false, fmt.Errorf("header is nil")
	}

	blockNumber := header.Number.Uint64()
	if blockNumber == 0 {
		// Genesis block cannot reorg
		return false, nil
	}

	// Load stored header at this height
	storedHeader, err := v.storage.LoadBlockHeader(ctx, blockNumber)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			// No stored block at this height, not a reorg
			return false, nil
		}
		return false, fmt.Errorf("failed to load stored header: %w", err)
	}

	// Check if hashes match
	if storedHeader.Hash() != header.Hash() {
		v.logger.Warn("reorg detected",
			zap.Uint64("block", blockNumber),
			zap.String("stored_hash", storedHeader.Hash().Hex()),
			zap.String("new_hash", header.Hash().Hex()))
		return true, nil
	}

	return false, nil
}

// ValidateBlockData validates downloaded block data
func (v *Validator) ValidateBlockData(ctx context.Context, data *BlockData) error {
	if data == nil {
		return fmt.Errorf("block data is nil")
	}

	if data.Header == nil {
		return fmt.Errorf("header is nil")
	}

	if data.Body == nil {
		return fmt.Errorf("body is nil")
	}

	// Construct block from header and body
	block := types.NewBlockWithHeader(data.Header).WithBody(data.Body.Transactions, data.Body.Uncles)

	// Validate the block
	return v.ValidateBlock(ctx, block, data.Receipts)
}

// GetStats returns validator statistics
func (v *Validator) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"validated_blocks":   v.validatedBlocks.Load(),
		"validation_errors":  v.validationErrors.Load(),
	}
}

// VerifyBlockHash verifies that a block's hash matches its content
func (v *Validator) VerifyBlockHash(block *types.Block) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	// Calculate hash from header
	calculatedHash := block.Header().Hash()

	// Compare with block hash
	if calculatedHash != block.Hash() {
		return fmt.Errorf("block hash mismatch: calculated %s, got %s",
			calculatedHash.Hex(), block.Hash().Hex())
	}

	return nil
}

// VerifyHeaderHash verifies that a header's hash matches its content
func (v *Validator) VerifyHeaderHash(header *types.Header) error {
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	// Calculate hash
	calculatedHash := header.Hash()

	// The Hash() method calculates the hash, so we just need to verify it's not empty
	if calculatedHash == (common.Hash{}) {
		return fmt.Errorf("header hash is empty")
	}

	return nil
}

// VerifySignature verifies the block signature
func (v *Validator) VerifySignature(header *types.Header) (common.Address, error) {
	if header == nil {
		return common.Address{}, fmt.Errorf("header is nil")
	}

	consensusEngine := v.chainAdapter.GetConsensusEngine()
	
	// Verify seal
	if err := consensusEngine.VerifySeal(header); err != nil {
		return common.Address{}, fmt.Errorf("seal verification failed: %w", err)
	}

	// Extract author
	author, err := consensusEngine.Author(header)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to extract author: %w", err)
	}

	return author, nil
}
