package bsc

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/sunvim/evm_syncer/pkg/chain"
)

// BSCAdapter implements the IChainAdapter interface for Binance Smart Chain
type BSCAdapter struct {
	config          *chain.ChainConfig
	bootnodes       []*enode.Node
	consensusEngine *ParliaConsensus
}

// NewBSCAdapter creates a new BSC chain adapter
func NewBSCAdapter(config *chain.ChainConfig) (*BSCAdapter, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid BSC config: %w", err)
	}

	bootnodes, err := config.ParseBootnodes()
	if err != nil {
		return nil, fmt.Errorf("failed to parse bootnodes: %w", err)
	}

	adapter := &BSCAdapter{
		config:          config,
		bootnodes:       bootnodes,
		consensusEngine: NewParliaConsensus(),
	}

	return adapter, nil
}

// ChainName returns the name of the chain
func (b *BSCAdapter) ChainName() string {
	return b.config.Name
}

// NetworkID returns the network ID
func (b *BSCAdapter) NetworkID() uint64 {
	return b.config.NetworkID
}

// GenesisHash returns the genesis block hash
func (b *BSCAdapter) GenesisHash() common.Hash {
	return b.config.GenesisHash
}

// GetBootnodes returns the list of bootnodes
func (b *BSCAdapter) GetBootnodes() []*enode.Node {
	return b.bootnodes
}

// GetConsensusEngine returns the consensus engine
func (b *BSCAdapter) GetConsensusEngine() chain.IConsensusEngine {
	return b.consensusEngine
}

// ValidateHeader validates a block header against its parent
func (b *BSCAdapter) ValidateHeader(header, parent *types.Header) error {
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	if parent != nil {
		// Check parent hash
		if header.ParentHash != parent.Hash() {
			return fmt.Errorf("invalid parent hash: got %s, want %s",
				header.ParentHash.Hex(), parent.Hash().Hex())
		}

		// Check block number
		if header.Number.Uint64() != parent.Number.Uint64()+1 {
			return fmt.Errorf("invalid block number: got %d, want %d",
				header.Number.Uint64(), parent.Number.Uint64()+1)
		}

		// Check timestamp
		if header.Time <= parent.Time {
			return fmt.Errorf("invalid timestamp: got %d, parent %d",
				header.Time, parent.Time)
		}
	}

	// Verify seal (signature)
	if err := b.consensusEngine.VerifySeal(header); err != nil {
		return fmt.Errorf("failed to verify seal: %w", err)
	}

	// Verify difficulty
	if err := b.consensusEngine.VerifyDifficulty(header); err != nil {
		return fmt.Errorf("failed to verify difficulty: %w", err)
	}

	return nil
}

// ValidateBody validates a block body
func (b *BSCAdapter) ValidateBody(block *types.Block) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}

	header := block.Header()

	// Validate transactions root
	txHash := types.DeriveSha(block.Transactions(), trie.NewStackTrie(nil))
	if txHash != header.TxHash {
		return fmt.Errorf("invalid transaction root: got %s, want %s",
			header.TxHash.Hex(), txHash.Hex())
	}

	// Validate uncle hash (BSC doesn't use uncles)
	uncleHash := types.CalcUncleHash(block.Uncles())
	if uncleHash != header.UncleHash {
		return fmt.Errorf("invalid uncle hash: got %s, want %s",
			header.UncleHash.Hex(), uncleHash.Hex())
	}

	// Check for duplicate transactions
	txSet := make(map[common.Hash]struct{})
	for _, tx := range block.Transactions() {
		hash := tx.Hash()
		if _, exists := txSet[hash]; exists {
			return fmt.Errorf("duplicate transaction %s", hash.Hex())
		}
		txSet[hash] = struct{}{}
	}

	return nil
}
