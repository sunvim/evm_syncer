package p2p

import (
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

// Protocol message codes for eth/66 and eth/67
const (
	StatusMsg             = 0x00
	NewBlockHashesMsg     = 0x01
	TransactionsMsg       = 0x02
	GetBlockHeadersMsg    = 0x03
	BlockHeadersMsg       = 0x04
	GetBlockBodiesMsg     = 0x05
	BlockBodiesMsg        = 0x06
	NewBlockMsg           = 0x07
	GetReceiptsMsg        = 0x09
	ReceiptsMsg           = 0x0a
	GetPooledTransactions = 0x09
	PooledTransactions    = 0x0a
)

// Protocol versions
const (
	ETH66 = 66
	ETH67 = 67
)

// StatusData represents the status message exchanged during handshake
type StatusData struct {
	ProtocolVersion uint32
	NetworkID       uint64
	TD              []byte // Total difficulty
	Head            common.Hash
	Genesis         common.Hash
	ForkID          ForkID
}

// ForkID represents EIP-2124 fork identifier
type ForkID struct {
	Hash [4]byte // CRC32 checksum of the genesis hash and fork blocks
	Next uint64  // Next fork block number
}

// GetBlockHeadersPacket represents a request for block headers (eth/66+)
type GetBlockHeadersPacket struct {
	RequestId uint64
	GetBlockHeadersRequest
}

// GetBlockHeadersRequest represents the request data
type GetBlockHeadersRequest struct {
	Origin  HashOrNumber // Block hash or number from which to retrieve headers
	Amount  uint64       // Maximum number of headers to retrieve
	Skip    uint64       // Blocks to skip between consecutive headers
	Reverse bool         // Query direction (false = rising towards latest, true = falling towards genesis)
}

// HashOrNumber represents either a block hash or a block number
type HashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block number from which to retrieve headers (excludes Hash)
}

// EncodeRLP is a specialized encoder for HashOrNumber to implement RLP encoding
func (hn *HashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash != (common.Hash{}) {
		return rlp.Encode(w, hn.Hash)
	}
	return rlp.Encode(w, hn.Number)
}

// DecodeRLP is a specialized decoder for HashOrNumber to implement RLP decoding
func (hn *HashOrNumber) DecodeRLP(s *rlp.Stream) error {
	_, size, err := s.Kind()
	if err != nil {
		return err
	}
	if size == 32 {
		return s.Decode(&hn.Hash)
	}
	return s.Decode(&hn.Number)
}

// BlockHeadersPacket represents a response with block headers (eth/66+)
type BlockHeadersPacket struct {
	RequestId uint64
	BlockHeadersRequest
}

// BlockHeadersRequest represents the response data
type BlockHeadersRequest []*types.Header

// GetBlockBodiesPacket represents a request for block bodies (eth/66+)
type GetBlockBodiesPacket struct {
	RequestId uint64
	GetBlockBodiesRequest
}

// GetBlockBodiesRequest represents the request data
type GetBlockBodiesRequest []common.Hash

// BlockBodiesPacket represents a response with block bodies (eth/66+)
type BlockBodiesPacket struct {
	RequestId uint64
	BlockBodiesResponse
}

// BlockBodiesResponse represents the response data
type BlockBodiesResponse []*BlockBody

// BlockBody represents a block body without the header
type BlockBody struct {
	Transactions []*types.Transaction
	Uncles       []*types.Header
}

// GetReceiptsPacket represents a request for receipts (eth/66+)
type GetReceiptsPacket struct {
	RequestId uint64
	GetReceiptsRequest
}

// GetReceiptsRequest represents the request data
type GetReceiptsRequest []common.Hash

// ReceiptsPacket represents a response with receipts (eth/66+)
type ReceiptsPacket struct {
	RequestId uint64
	ReceiptsResponse
}

// ReceiptsResponse represents the response data
type ReceiptsResponse [][]*types.Receipt

// TransactionsPacket represents a transactions broadcast message
type TransactionsPacket []*types.Transaction

// NewBlockHashesPacket represents a block hash announcement
type NewBlockHashesPacket []struct {
	Hash   common.Hash
	Number uint64
}

// NewBlockPacket represents a complete block announcement
type NewBlockPacket struct {
	Block *types.Block
	TD    []byte // Total difficulty
}

// Packet represents a generic protocol packet
type Packet struct {
	Code uint64
	Data interface{}
}

// DecodePacket decodes a packet based on protocol version and message code
func DecodePacket(code uint64, data []byte, version uint32) (interface{}, error) {
	switch code {
	case StatusMsg:
		var msg StatusData
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode status message: %w", err)
		}
		return &msg, nil

	case GetBlockHeadersMsg:
		if version >= ETH66 {
			var msg GetBlockHeadersPacket
			if err := rlp.DecodeBytes(data, &msg); err != nil {
				return nil, fmt.Errorf("failed to decode get block headers packet: %w", err)
			}
			return &msg, nil
		}
		var msg GetBlockHeadersRequest
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode get block headers request: %w", err)
		}
		return &msg, nil

	case BlockHeadersMsg:
		if version >= ETH66 {
			var msg BlockHeadersPacket
			if err := rlp.DecodeBytes(data, &msg); err != nil {
				return nil, fmt.Errorf("failed to decode block headers packet: %w", err)
			}
			return &msg, nil
		}
		var msg BlockHeadersRequest
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode block headers request: %w", err)
		}
		return &msg, nil

	case GetBlockBodiesMsg:
		if version >= ETH66 {
			var msg GetBlockBodiesPacket
			if err := rlp.DecodeBytes(data, &msg); err != nil {
				return nil, fmt.Errorf("failed to decode get block bodies packet: %w", err)
			}
			return &msg, nil
		}
		var msg GetBlockBodiesRequest
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode get block bodies request: %w", err)
		}
		return &msg, nil

	case BlockBodiesMsg:
		if version >= ETH66 {
			var msg BlockBodiesPacket
			if err := rlp.DecodeBytes(data, &msg); err != nil {
				return nil, fmt.Errorf("failed to decode block bodies packet: %w", err)
			}
			return &msg, nil
		}
		var msg BlockBodiesResponse
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode block bodies response: %w", err)
		}
		return &msg, nil

	case GetReceiptsMsg:
		if version >= ETH66 {
			var msg GetReceiptsPacket
			if err := rlp.DecodeBytes(data, &msg); err != nil {
				return nil, fmt.Errorf("failed to decode get receipts packet: %w", err)
			}
			return &msg, nil
		}
		var msg GetReceiptsRequest
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode get receipts request: %w", err)
		}
		return &msg, nil

	case ReceiptsMsg:
		if version >= ETH66 {
			var msg ReceiptsPacket
			if err := rlp.DecodeBytes(data, &msg); err != nil {
				return nil, fmt.Errorf("failed to decode receipts packet: %w", err)
			}
			return &msg, nil
		}
		var msg ReceiptsResponse
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode receipts response: %w", err)
		}
		return &msg, nil

	case TransactionsMsg:
		var msg TransactionsPacket
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode transactions packet: %w", err)
		}
		return &msg, nil

	case NewBlockHashesMsg:
		var msg NewBlockHashesPacket
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode new block hashes packet: %w", err)
		}
		return &msg, nil

	case NewBlockMsg:
		var msg NewBlockPacket
		if err := rlp.DecodeBytes(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode new block packet: %w", err)
		}
		return &msg, nil

	default:
		return nil, fmt.Errorf("unknown message code: %d", code)
	}
}

// EncodePacket encodes a packet based on protocol version
func EncodePacket(msg interface{}, version uint32) ([]byte, error) {
	data, err := rlp.EncodeToBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode packet: %w", err)
	}
	return data, nil
}
