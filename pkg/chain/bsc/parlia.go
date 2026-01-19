package bsc

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	// ExtraVanity is the fixed length of extra-data prefix
	ExtraVanity = 32
	// ExtraSeal is the fixed length of extra-data suffix containing the signature
	ExtraSeal = 65
)

// ParliaConsensus implements the Parlia consensus engine (simplified version)
type ParliaConsensus struct{}

// NewParliaConsensus creates a new Parlia consensus engine
func NewParliaConsensus() *ParliaConsensus {
	return &ParliaConsensus{}
}

// VerifySeal verifies the signature/seal of a block header
func (p *ParliaConsensus) VerifySeal(header *types.Header) error {
	if header == nil {
		return errors.New("header is nil")
	}

	// Check extra data length
	if len(header.Extra) < ExtraVanity+ExtraSeal {
		return fmt.Errorf("invalid extra data length: got %d, want at least %d",
			len(header.Extra), ExtraVanity+ExtraSeal)
	}

	// Extract signature from extra data (last 65 bytes)
	signature := header.Extra[len(header.Extra)-ExtraSeal:]

	// Recover signer address from signature
	signer, err := p.ecrecover(header, signature)
	if err != nil {
		return fmt.Errorf("failed to recover signer: %w", err)
	}

	// Check if signer is valid (non-zero address)
	if signer == (common.Address{}) {
		return errors.New("invalid signer: zero address")
	}

	// In a full implementation, we would verify that the signer is in the validator set
	// For this simplified version, we just check that the signature is valid

	return nil
}

// Author extracts the author (signer) address from a block header
func (p *ParliaConsensus) Author(header *types.Header) (common.Address, error) {
	if header == nil {
		return common.Address{}, errors.New("header is nil")
	}

	// Check extra data length
	if len(header.Extra) < ExtraVanity+ExtraSeal {
		return common.Address{}, fmt.Errorf("invalid extra data length: got %d, want at least %d",
			len(header.Extra), ExtraVanity+ExtraSeal)
	}

	// Extract signature from extra data (last 65 bytes)
	signature := header.Extra[len(header.Extra)-ExtraSeal:]

	// Recover signer address from signature
	return p.ecrecover(header, signature)
}

// VerifyDifficulty verifies the difficulty field of a block header
// In Parlia, difficulty is either 1 (not in-turn) or 2 (in-turn)
func (p *ParliaConsensus) VerifyDifficulty(header *types.Header) error {
	if header == nil {
		return errors.New("header is nil")
	}

	difficulty := header.Difficulty.Uint64()
	if difficulty != 1 && difficulty != 2 {
		return fmt.Errorf("invalid difficulty: got %d, want 1 or 2", difficulty)
	}

	return nil
}

// ecrecover recovers the signer address from the header and signature
func (p *ParliaConsensus) ecrecover(header *types.Header, signature []byte) (common.Address, error) {
	// Create a copy of the header without the seal
	h := types.CopyHeader(header)
	h.Extra = h.Extra[:len(h.Extra)-ExtraSeal]

	// Compute the hash of the header
	hash := rlpHash(h)

	// Recover public key from signature
	pubkey, err := crypto.SigToPub(hash.Bytes(), signature)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to recover public key: %w", err)
	}

	// Convert public key to address
	signer := crypto.PubkeyToAddress(*pubkey)
	return signer, nil
}

// rlpHash computes the RLP hash of a header
func rlpHash(header *types.Header) common.Hash {
	// Use the types package to compute the hash
	return header.Hash()
}
