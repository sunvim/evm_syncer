package chain

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// ChainConfig holds chain-specific configuration
type ChainConfig struct {
	Name        string
	NetworkID   uint64
	GenesisHash common.Hash
	Bootnodes   []string
}

// ParseBootnodes parses bootnode strings into enode.Node objects
func (c *ChainConfig) ParseBootnodes() ([]*enode.Node, error) {
	nodes := make([]*enode.Node, 0, len(c.Bootnodes))
	for i, bootnode := range c.Bootnodes {
		node, err := enode.Parse(enode.ValidSchemes, bootnode)
		if err != nil {
			return nil, fmt.Errorf("invalid bootnode %d (%s): %w", i, bootnode, err)
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// Validate validates the chain configuration
func (c *ChainConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("chain name is required")
	}
	if c.NetworkID == 0 {
		return fmt.Errorf("network ID is required")
	}
	if c.GenesisHash == (common.Hash{}) {
		return fmt.Errorf("genesis hash is required")
	}
	if len(c.Bootnodes) == 0 {
		return fmt.Errorf("at least one bootnode is required")
	}
	return nil
}
