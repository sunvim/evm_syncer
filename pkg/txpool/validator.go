package txpool

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
)

const (
	// MaxTransactionSize is the maximum allowed transaction size (128KB)
	MaxTransactionSize = 128 * 1024
	
	// MaxGasLimit is the maximum allowed gas limit per transaction
	MaxGasLimit = 30_000_000
)

var (
	// ErrInvalidSignature is returned when transaction signature verification fails
	ErrInvalidSignature = errors.New("invalid transaction signature")
	
	// ErrTxTooLarge is returned when transaction exceeds size limit
	ErrTxTooLarge = errors.New("transaction size exceeds limit")
	
	// ErrGasLimitExceeded is returned when gas limit is too high
	ErrGasLimitExceeded = errors.New("gas limit exceeds maximum")
	
	// ErrChainIDMismatch is returned when transaction chain ID doesn't match
	ErrChainIDMismatch = errors.New("chain ID mismatch")
	
	// ErrInvalidGasPrice is returned when gas price is zero or negative
	ErrInvalidGasPrice = errors.New("invalid gas price")
	
	// ErrNilTransaction is returned when transaction is nil
	ErrNilTransaction = errors.New("transaction is nil")
)

// Validator validates transactions according to network rules
type Validator struct {
	chainID *big.Int
	signer  types.Signer
	logger  *zap.Logger
}

// NewValidator creates a new transaction validator
func NewValidator(chainID *big.Int, logger *zap.Logger) *Validator {
	return &Validator{
		chainID: chainID,
		signer:  types.NewLondonSigner(chainID),
		logger:  logger.With(zap.String("component", "validator")),
	}
}

// ValidateTransaction performs comprehensive validation of a transaction
func (v *Validator) ValidateTransaction(tx *types.Transaction) (common.Address, error) {
	if tx == nil {
		return common.Address{}, ErrNilTransaction
	}

	// Validate transaction size
	if err := v.validateSize(tx); err != nil {
		return common.Address{}, err
	}

	// Validate gas limit
	if err := v.validateGasLimit(tx); err != nil {
		return common.Address{}, err
	}

	// Validate gas price
	if err := v.validateGasPrice(tx); err != nil {
		return common.Address{}, err
	}

	// Validate chain ID
	if err := v.validateChainID(tx); err != nil {
		return common.Address{}, err
	}

	// Validate signature and recover sender
	from, err := v.validateSignature(tx)
	if err != nil {
		return common.Address{}, err
	}

	v.logger.Debug("transaction validated",
		zap.String("tx_hash", tx.Hash().Hex()),
		zap.String("from", from.Hex()),
		zap.Uint64("nonce", tx.Nonce()),
		zap.Uint64("gas", tx.Gas()),
	)

	return from, nil
}

// validateSize checks if transaction size is within limits
func (v *Validator) validateSize(tx *types.Transaction) error {
	// Encode transaction to get actual size
	encoded, err := rlp.EncodeToBytes(tx)
	if err != nil {
		return fmt.Errorf("failed to encode transaction: %w", err)
	}

	size := len(encoded)
	if size > MaxTransactionSize {
		v.logger.Warn("transaction too large",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Int("size", size),
			zap.Int("max_size", MaxTransactionSize),
		)
		return ErrTxTooLarge
	}

	return nil
}

// validateGasLimit checks if gas limit is within acceptable range
func (v *Validator) validateGasLimit(tx *types.Transaction) error {
	gasLimit := tx.Gas()
	
	if gasLimit == 0 {
		v.logger.Warn("transaction has zero gas limit",
			zap.String("tx_hash", tx.Hash().Hex()),
		)
		return fmt.Errorf("gas limit cannot be zero")
	}

	if gasLimit > MaxGasLimit {
		v.logger.Warn("gas limit exceeds maximum",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Uint64("gas_limit", gasLimit),
			zap.Uint64("max_gas_limit", MaxGasLimit),
		)
		return ErrGasLimitExceeded
	}

	return nil
}

// validateGasPrice checks if gas price is valid
func (v *Validator) validateGasPrice(tx *types.Transaction) error {
	gasPrice := tx.GasPrice()
	if gasPrice == nil || gasPrice.Sign() <= 0 {
		v.logger.Warn("invalid gas price",
			zap.String("tx_hash", tx.Hash().Hex()),
		)
		return ErrInvalidGasPrice
	}

	return nil
}

// validateChainID verifies transaction chain ID matches network
func (v *Validator) validateChainID(tx *types.Transaction) error {
	txChainID := tx.ChainId()
	
	if txChainID == nil {
		v.logger.Warn("transaction missing chain ID",
			zap.String("tx_hash", tx.Hash().Hex()),
		)
		return fmt.Errorf("transaction missing chain ID")
	}

	if txChainID.Cmp(v.chainID) != 0 {
		v.logger.Warn("chain ID mismatch",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.String("tx_chain_id", txChainID.String()),
			zap.String("network_chain_id", v.chainID.String()),
		)
		return ErrChainIDMismatch
	}

	return nil
}

// validateSignature verifies transaction signature and recovers sender address
func (v *Validator) validateSignature(tx *types.Transaction) (common.Address, error) {
	// Recover sender from signature
	from, err := types.Sender(v.signer, tx)
	if err != nil {
		v.logger.Warn("failed to recover sender",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Error(err),
		)
		return common.Address{}, fmt.Errorf("%w: %v", ErrInvalidSignature, err)
	}

	// Validate that we got a non-zero address
	if from == (common.Address{}) {
		v.logger.Warn("recovered zero address",
			zap.String("tx_hash", tx.Hash().Hex()),
		)
		return common.Address{}, ErrInvalidSignature
	}

	return from, nil
}

// ValidateBatch validates multiple transactions in batch
func (v *Validator) ValidateBatch(txs []*types.Transaction) ([]common.Address, []error) {
	if len(txs) == 0 {
		return nil, nil
	}

	senders := make([]common.Address, len(txs))
	errors := make([]error, len(txs))

	for i, tx := range txs {
		sender, err := v.ValidateTransaction(tx)
		senders[i] = sender
		errors[i] = err
	}

	return senders, errors
}

// QuickValidate performs basic validation without full signature verification
// This is useful for fast filtering before more expensive validation
func (v *Validator) QuickValidate(tx *types.Transaction) error {
	if tx == nil {
		return ErrNilTransaction
	}

	// Quick size check
	if err := v.validateSize(tx); err != nil {
		return err
	}

	// Quick gas limit check
	if err := v.validateGasLimit(tx); err != nil {
		return err
	}

	// Quick gas price check
	if err := v.validateGasPrice(tx); err != nil {
		return err
	}

	// Quick chain ID check
	if err := v.validateChainID(tx); err != nil {
		return err
	}

	return nil
}
