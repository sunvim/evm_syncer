package chain

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// IChainAdapter defines the interface for chain-specific implementations
type IChainAdapter interface {
	// ChainName returns the name of the chain (e.g., "bsc", "ethereum")
	ChainName() string

	// NetworkID returns the network ID of the chain
	NetworkID() uint64

	// GenesisHash returns the genesis block hash
	GenesisHash() common.Hash

	// GetBootnodes returns the list of bootnodes for initial peer discovery
	GetBootnodes() []*enode.Node

	// GetConsensusEngine returns the consensus engine for this chain
	GetConsensusEngine() IConsensusEngine

	// ValidateHeader validates a block header against its parent
	ValidateHeader(header, parent *types.Header) error

	// ValidateBody validates a block body
	ValidateBody(block *types.Block) error
}

// IConsensusEngine defines the interface for consensus mechanisms
type IConsensusEngine interface {
	// VerifySeal verifies the signature/seal of a block header
	VerifySeal(header *types.Header) error

	// Author extracts the author (signer) address from a block header
	Author(header *types.Header) (common.Address, error)

	// VerifyDifficulty verifies the difficulty field of a block header
	VerifyDifficulty(header *types.Header) error
}
