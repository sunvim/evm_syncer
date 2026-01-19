package txpool

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sunvim/evm_syncer/pkg/logger"
	"github.com/sunvim/evm_syncer/pkg/p2p"
	"github.com/sunvim/evm_syncer/pkg/storage"
)

// Helper function to create a test transaction
func createTestTransaction(t *testing.T, key *ecdsa.PrivateKey, chainID *big.Int, nonce uint64) *types.Transaction {
	tx := types.NewTransaction(
		nonce,
		common.HexToAddress("0x1234567890123456789012345678901234567890"),
		big.NewInt(1000000000000000000), // 1 ETH
		21000,                           // gas limit
		big.NewInt(20000000000),         // gas price (20 gwei)
		nil,
	)

	signer := types.NewLondonSigner(chainID)
	signedTx, err := types.SignTx(tx, signer, key)
	if err != nil {
		t.Fatalf("failed to sign transaction: %v", err)
	}

	return signedTx
}

// TestValidator tests transaction validation
func TestValidator(t *testing.T) {
	// Initialize logger
	if err := logger.Init("info", "console"); err != nil {
		t.Fatalf("failed to initialize logger: %v", err)
	}

	chainID := big.NewInt(1)
	validator := NewValidator(chainID, logger.Get())

	// Generate test key
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Test valid transaction
	t.Run("ValidTransaction", func(t *testing.T) {
		tx := createTestTransaction(t, key, chainID, 0)
		from, err := validator.ValidateTransaction(tx)
		if err != nil {
			t.Errorf("expected valid transaction, got error: %v", err)
		}

		expectedFrom := crypto.PubkeyToAddress(key.PublicKey)
		if from != expectedFrom {
			t.Errorf("expected from %s, got %s", expectedFrom.Hex(), from.Hex())
		}
	})

	// Test chain ID mismatch
	t.Run("ChainIDMismatch", func(t *testing.T) {
		wrongChainID := big.NewInt(999)
		tx := createTestTransaction(t, key, wrongChainID, 0)
		_, err := validator.ValidateTransaction(tx)
		if err != ErrChainIDMismatch {
			t.Errorf("expected ErrChainIDMismatch, got: %v", err)
		}
	})

	// Test nil transaction
	t.Run("NilTransaction", func(t *testing.T) {
		_, err := validator.ValidateTransaction(nil)
		if err != ErrNilTransaction {
			t.Errorf("expected ErrNilTransaction, got: %v", err)
		}
	})

	// Test gas limit exceeded
	t.Run("GasLimitExceeded", func(t *testing.T) {
		tx := types.NewTransaction(
			0,
			common.HexToAddress("0x1234567890123456789012345678901234567890"),
			big.NewInt(1000),
			MaxGasLimit+1, // Exceed max gas limit
			big.NewInt(20000000000),
			nil,
		)

		signer := types.NewLondonSigner(chainID)
		signedTx, err := types.SignTx(tx, signer, key)
		if err != nil {
			t.Fatalf("failed to sign transaction: %v", err)
		}

		_, err = validator.ValidateTransaction(signedTx)
		if err != ErrGasLimitExceeded {
			t.Errorf("expected ErrGasLimitExceeded, got: %v", err)
		}
	})
}

// TestValidatorQuickValidate tests quick validation
func TestValidatorQuickValidate(t *testing.T) {
	if err := logger.Init("info", "console"); err != nil {
		t.Fatalf("failed to initialize logger: %v", err)
	}

	chainID := big.NewInt(1)
	validator := NewValidator(chainID, logger.Get())

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	t.Run("QuickValidateSuccess", func(t *testing.T) {
		tx := createTestTransaction(t, key, chainID, 0)
		err := validator.QuickValidate(tx)
		if err != nil {
			t.Errorf("expected quick validation to pass, got error: %v", err)
		}
	})

	t.Run("QuickValidateChainIDMismatch", func(t *testing.T) {
		wrongChainID := big.NewInt(999)
		tx := createTestTransaction(t, key, wrongChainID, 0)
		err := validator.QuickValidate(tx)
		if err != ErrChainIDMismatch {
			t.Errorf("expected ErrChainIDMismatch, got: %v", err)
		}
	})
}

// TestValidatorBatch tests batch validation
func TestValidatorBatch(t *testing.T) {
	if err := logger.Init("info", "console"); err != nil {
		t.Fatalf("failed to initialize logger: %v", err)
	}

	chainID := big.NewInt(1)
	validator := NewValidator(chainID, logger.Get())

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create batch of transactions
	txs := make([]*types.Transaction, 3)
	for i := range txs {
		txs[i] = createTestTransaction(t, key, chainID, uint64(i))
	}

	senders, errors := validator.ValidateBatch(txs)

	if len(senders) != len(txs) {
		t.Errorf("expected %d senders, got %d", len(txs), len(senders))
	}

	if len(errors) != len(txs) {
		t.Errorf("expected %d errors, got %d", len(txs), len(errors))
	}

	expectedFrom := crypto.PubkeyToAddress(key.PublicKey)
	for i, sender := range senders {
		if errors[i] != nil {
			t.Errorf("unexpected error at index %d: %v", i, errors[i])
		}
		if sender != expectedFrom {
			t.Errorf("expected sender %s at index %d, got %s", expectedFrom.Hex(), i, sender.Hex())
		}
	}
}

// Mock PeerManager for testing
type mockPeerManager struct {
	peers         []*p2p.Peer
	broadcastFunc func(code uint64, data interface{}) error
}

func (m *mockPeerManager) GetAllPeers() []*p2p.Peer {
	return m.peers
}

func (m *mockPeerManager) BroadcastMessage(code uint64, data interface{}) error {
	if m.broadcastFunc != nil {
		return m.broadcastFunc(code, data)
	}
	return nil
}

// TestReceiverDuplicateDetection tests duplicate transaction detection
func TestReceiverDuplicateDetection(t *testing.T) {
	// Skip if no Pika connection available
	t.Skip("Skipping integration test - requires Pika connection")

	if err := logger.Init("info", "console"); err != nil {
		t.Fatalf("failed to initialize logger: %v", err)
	}

	// Setup Pika client
	pikaClient, err := storage.NewPikaClient(storage.DefaultPikaConfig())
	if err != nil {
		t.Fatalf("failed to create pika client: %v", err)
	}
	defer pikaClient.Close()

	chainID := big.NewInt(1)
	receiver := NewReceiver(pikaClient, chainID, logger.Get())

	if err := receiver.Start(); err != nil {
		t.Fatalf("failed to start receiver: %v", err)
	}
	defer receiver.Stop()

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	tx := createTestTransaction(t, key, chainID, 0)

	// First transaction should succeed
	err = receiver.HandleTransaction(tx, "peer1")
	if err != nil {
		t.Errorf("first transaction should succeed: %v", err)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Second identical transaction should be detected as duplicate
	// Note: This will be logged but not return an error through HandleTransaction
	err = receiver.HandleTransaction(tx, "peer2")
	if err != nil {
		t.Errorf("HandleTransaction should not error on duplicate: %v", err)
	}
}

// TestBroadcasterQueueProcessing tests broadcaster queue processing
func TestBroadcasterQueueProcessing(t *testing.T) {
	// Skip if no Pika connection available
	t.Skip("Skipping integration test - requires Pika connection")

	if err := logger.Init("info", "console"); err != nil {
		t.Fatalf("failed to initialize logger: %v", err)
	}

	pikaClient, err := storage.NewPikaClient(storage.DefaultPikaConfig())
	if err != nil {
		t.Fatalf("failed to create pika client: %v", err)
	}
	defer pikaClient.Close()

	mockPM := &mockPeerManager{
		peers: []*p2p.Peer{},
		broadcastFunc: func(code uint64, data interface{}) error {
			return nil
		},
	}

	broadcaster := NewBroadcaster(pikaClient, mockPM, logger.Get())

	if err := broadcaster.Start(); err != nil {
		t.Fatalf("failed to start broadcaster: %v", err)
	}
	defer broadcaster.Stop()

	// Create and store a test transaction
	ctx := context.Background()
	txStorage := storage.NewTxPoolStorage(pikaClient)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	chainID := big.NewInt(1)
	tx := createTestTransaction(t, key, chainID, 0)
	from := crypto.PubkeyToAddress(key.PublicKey)

	// Add to pending
	if err := txStorage.AddPendingTransaction(ctx, tx, from); err != nil {
		t.Fatalf("failed to add pending transaction: %v", err)
	}

	// Enqueue for broadcast
	if err := broadcaster.EnqueueTransaction(ctx, tx.Hash()); err != nil {
		t.Fatalf("failed to enqueue transaction: %v", err)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Check queue length
	queueLen, err := broadcaster.GetQueueLength(ctx)
	if err != nil {
		t.Errorf("failed to get queue length: %v", err)
	}

	t.Logf("Queue length after processing: %d", queueLen)
}

// Benchmark transaction validation
func BenchmarkValidateTransaction(b *testing.B) {
	if err := logger.Init("error", "console"); err != nil {
		b.Fatalf("failed to initialize logger: %v", err)
	}

	chainID := big.NewInt(1)
	validator := NewValidator(chainID, logger.Get())

	key, err := crypto.GenerateKey()
	if err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}

	tx := createTestTransaction(&testing.T{}, key, chainID, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = validator.ValidateTransaction(tx)
	}
}

// Benchmark quick validation
func BenchmarkQuickValidate(b *testing.B) {
	if err := logger.Init("error", "console"); err != nil {
		b.Fatalf("failed to initialize logger: %v", err)
	}

	chainID := big.NewInt(1)
	validator := NewValidator(chainID, logger.Get())

	key, err := crypto.GenerateKey()
	if err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}

	tx := createTestTransaction(&testing.T{}, key, chainID, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validator.QuickValidate(tx)
	}
}
