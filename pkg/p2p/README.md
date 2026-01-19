# P2P Network Layer

This package implements a production-ready P2P network layer for the EVM blockchain syncer project.

## Features

- **Protocol Support**: Full implementation of eth/66 and eth/67 protocols
- **Peer Management**: Automatic peer pool management (30-50 active peers)
- **Peer Scoring**: Dynamic peer scoring based on response speed and data validity
- **Discovery**: DHT Kademlia-based node discovery with BSC bootnode support
- **Message Handling**: Complete protocol message handlers for all eth message types
- **Health Checks**: Automatic peer health monitoring and bad peer disconnection
- **Context Support**: Full context support for graceful shutdown
- **Structured Logging**: Comprehensive structured logging with zap

## Architecture

### Components

1. **messages.go** - Protocol message definitions
   - Status, GetBlockHeaders, BlockHeaders
   - GetBlockBodies, BlockBodies
   - GetReceipts, Receipts
   - Transactions, NewBlock, NewBlockHashes
   - Support for both eth/66 and eth/67 formats

2. **peer.go** - Peer connection management
   - Handshake protocol
   - Request/response tracking
   - Peer scoring mechanism
   - Connection state management
   - Statistics collection

3. **peer_manager.go** - Peer pool management
   - Maintains 30-50 active peers
   - Health check loops
   - Peer scoring and disconnection
   - Broadcasting capabilities
   - Peer filtering and selection

4. **protocol_handler.go** - Message handlers
   - Handles all protocol messages
   - Request/response correlation
   - Callback interfaces for block, transaction, and receipt handling
   - Message validation and peer scoring

5. **discovery.go** - Node discovery
   - DHT Kademlia implementation
   - BSC bootnode support
   - Random node lookup
   - Node caching

6. **server.go** - P2P server
   - Main server implementation
   - Protocol initialization
   - Peer connection management
   - Status synchronization
   - Broadcast APIs

## Usage

### Basic Server Setup

```go
import (
    "github.com/sunvim/evm_syncer/pkg/p2p"
    "github.com/ethereum/go-ethereum/crypto"
)

// Generate private key
privateKey, err := crypto.GenerateKey()
if err != nil {
    log.Fatal(err)
}

// Create server config
config := &p2p.ServerConfig{
    PrivateKey:    privateKey,
    ListenAddr:    "0.0.0.0:30303",
    NetworkID:     56, // BSC mainnet
    Genesis:       genesisHash,
    Head:          headHash,
    TD:            totalDifficulty,
    MaxPeers:      50,
    BootstrapURLs: nil, // Uses BSC bootnodes by default
    
    // Protocol callbacks
    BlockHandler:   myBlockHandler,
    TxHandler:      myTxHandler,
    ReceiptHandler: myReceiptHandler,
}

// Create and start server
server, err := p2p.NewServer(config, logger)
if err != nil {
    log.Fatal(err)
}

if err := server.Start(); err != nil {
    log.Fatal(err)
}
defer server.Stop()
```

### Implementing Handlers

```go
type MyBlockHandler struct{}

func (h *MyBlockHandler) HandleNewBlock(peer *p2p.Peer, block *types.Block, td []byte) error {
    // Process new block
    return nil
}

func (h *MyBlockHandler) HandleNewBlockHashes(peer *p2p.Peer, hashes []struct {
    Hash   [32]byte
    Number uint64
}) error {
    // Process block hash announcements
    return nil
}

func (h *MyBlockHandler) HandleBlockHeaders(peer *p2p.Peer, requestID uint64, headers []*types.Header) error {
    // Process block headers response
    return nil
}

func (h *MyBlockHandler) HandleBlockBodies(peer *p2p.Peer, requestID uint64, bodies []*p2p.BlockBody) error {
    // Process block bodies response
    return nil
}
```

### Requesting Data

```go
// Request block headers
ctx := context.Background()
origin := p2p.HashOrNumber{Number: 1000}
headers, err := server.RequestBlockHeaders(ctx, origin, 100, 0, false)
if err != nil {
    log.Error(err)
}

// Request block bodies
hashes := []common.Hash{hash1, hash2, hash3}
bodies, err := server.RequestBlockBodies(ctx, hashes)
if err != nil {
    log.Error(err)
}

// Request receipts
receipts, err := server.RequestReceipts(ctx, hashes)
if err != nil {
    log.Error(err)
}
```

### Broadcasting

```go
// Broadcast new block
block := types.NewBlock(...)
td := big.NewInt(...)
server.BroadcastBlock(block, td.Bytes())

// Broadcast transactions
txs := []*types.Transaction{tx1, tx2}
server.BroadcastTransactions(txs)

// Broadcast block hashes
hashes := p2p.NewBlockHashesPacket{
    {Hash: hash1, Number: 1000},
    {Hash: hash2, Number: 1001},
}
server.BroadcastBlockHashes(hashes)
```

### Monitoring

```go
// Get peer count
count := server.GetPeerCount()

// Get peer statistics
stats := server.GetPeerStats()
fmt.Printf("Peers: %v\n", stats)

// Get connected peers
peers := server.GetPeers()
for _, peer := range peers {
    fmt.Printf("Peer: %s, Score: %d\n", peer.ID(), peer.Score())
}
```

## Peer Scoring

Peers are scored based on:
- **Response Speed**: Faster responses increase score
- **Response Rate**: High response rate increases score
- **Data Validity**: Invalid data decreases score
- **Timeout**: Request timeouts decrease score

Score ranges:
- Initial score: 50
- Maximum score: 100
- Disconnect threshold: 10

## Configuration

### Network Parameters

- **Min Peers**: 30
- **Max Peers**: 50
- **Max Pending Peers**: 50
- **Dial Timeout**: 30 seconds
- **Max Response Time**: 30 seconds

### Health Checks

- **Peer Check Interval**: 10 seconds
- **Health Check Interval**: 30 seconds
- **Discovery Refresh**: 30 seconds

### BSC Bootnodes

The package includes built-in BSC mainnet bootnodes. You can also provide custom bootnodes via the `BootstrapURLs` configuration parameter.

## Thread Safety

All components are thread-safe and can be safely accessed from multiple goroutines:
- Peer manager uses RW mutexes for concurrent access
- Server uses context for graceful shutdown
- All state modifications are properly synchronized

## Error Handling

The implementation includes comprehensive error handling:
- All errors are properly wrapped with context
- Network errors trigger peer scoring adjustments
- Protocol violations lead to peer disconnection
- All operations respect context cancellation

## Logging

All components use structured logging (zap):
- Debug: Detailed protocol messages
- Info: Peer connections, discovery events
- Warn: Protocol violations, timeouts
- Error: Critical failures

## Testing

To test the P2P layer:

```bash
# Build the package
go build ./pkg/p2p/...

# Run tests (when available)
go test ./pkg/p2p/...
```

## Dependencies

- `github.com/ethereum/go-ethereum/p2p` - Core P2P functionality
- `github.com/ethereum/go-ethereum/core/types` - Ethereum types
- `github.com/ethereum/go-ethereum/rlp` - RLP encoding
- `github.com/ethereum/go-ethereum/crypto` - Cryptography
- `go.uber.org/zap` - Structured logging

## License

This implementation follows the same license as the parent project.
