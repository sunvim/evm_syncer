package bsc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/sunvim/evm_syncer/pkg/chain"
)

// DefaultBSCConfig returns the default configuration for BSC mainnet
func DefaultBSCConfig() *chain.ChainConfig {
	return &chain.ChainConfig{
		Name:      "bsc",
		NetworkID: 56,
		GenesisHash: common.HexToHash(
			"0x0d21840abff46b96c84b2ac9e10e4f5cdaeb5693cb665db62a2f3b02d2d57b5b",
		),
		Bootnodes: []string{
			"enode://f3cfd69f2808ef64c41f83c76b5d60f8d5f32e98b9c512ba83c991138b4fafe3c60824c538711f5aee4e9a4b4b06f6e6c59c39f1538efae0f1c7ca8ef75ad9b5@3.0.211.106:30311",
			"enode://8b8eb4e64f8c8f3c3b9e0f4e0b7b2f2d8f1d7e0c7f8f1f6f5f4f3f2f1f0f9f8@54.178.30.104:30311",
		},
	}
}
