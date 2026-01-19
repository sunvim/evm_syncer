package p2p

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"go.uber.org/zap"
)

const (
	// Protocol name and versions
	protocolName    = "eth"
	protocolVersion = ETH67

	// Connection parameters
	maxPendingPeers = 50
	dialTimeout     = 30 * time.Second
	dialRatio       = 3

	// Peer connection intervals
	peerConnectInterval = 5 * time.Second
)

// ServerConfig contains configuration for the P2P server
type ServerConfig struct {
	PrivateKey    *ecdsa.PrivateKey
	ListenAddr    string
	NetworkID     uint64
	Genesis       common.Hash
	Head          common.Hash
	TD            *big.Int
	MaxPeers      int
	BootstrapURLs []string

	// Protocol callbacks
	BlockHandler   BlockHandler
	TxHandler      TransactionHandler
	ReceiptHandler ReceiptHandler
}

// Server represents the P2P server
type Server struct {
	config          *ServerConfig
	p2pServer       *p2p.Server
	discovery       *Discovery
	peerManager     *PeerManager
	protocolHandler *ProtocolHandler
	logger          *zap.Logger

	// Status information
	status     *StatusData
	statusLock sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new P2P server
func NewServer(config *ServerConfig, logger *zap.Logger) (*Server, error) {
	if config.PrivateKey == nil {
		return nil, fmt.Errorf("private key is required")
	}

	if config.MaxPeers == 0 {
		config.MaxPeers = maxPeers
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create peer manager
	peerManager := NewPeerManager(logger)

	// Create protocol handler
	protocolHandler := NewProtocolHandler(
		logger,
		peerManager,
		config.BlockHandler,
		config.TxHandler,
		config.ReceiptHandler,
	)

	// Create initial status
	status := &StatusData{
		ProtocolVersion: protocolVersion,
		NetworkID:       config.NetworkID,
		TD:              config.TD.Bytes(),
		Head:            config.Head,
		Genesis:         config.Genesis,
		ForkID: ForkID{
			Hash: [4]byte{0, 0, 0, 0},
			Next: 0,
		},
	}

	server := &Server{
		config:          config,
		peerManager:     peerManager,
		protocolHandler: protocolHandler,
		logger:          logger.With(zap.String("component", "p2p_server")),
		status:          status,
		ctx:             ctx,
		cancel:          cancel,
	}

	// Create discovery
	discoveryConfig := &DiscoveryConfig{
		PrivateKey:    config.PrivateKey,
		BootstrapURLs: config.BootstrapURLs,
		ListenAddr:    config.ListenAddr,
		NetworkID:     config.NetworkID,
	}

	discovery, err := NewDiscovery(discoveryConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery: %w", err)
	}

	server.discovery = discovery

	// Set discovery callback
	discovery.SetNodeDiscoveredCallback(func(node *enode.Node) {
		server.onNodeDiscovered(node)
	})

	return server, nil
}

// Start starts the P2P server
func (s *Server) Start() error {
	// Start discovery
	if err := s.discovery.Start(s.config.ListenAddr); err != nil {
		return fmt.Errorf("failed to start discovery: %w", err)
	}

	// Create p2p.Server
	s.p2pServer = &p2p.Server{
		Config: p2p.Config{
			PrivateKey:      s.config.PrivateKey,
			MaxPeers:        s.config.MaxPeers,
			MaxPendingPeers: maxPendingPeers,
			DialRatio:       dialRatio,
			Name:            "evm-syncer",
			ListenAddr:      s.config.ListenAddr,
			Protocols:       s.makeProtocols(),
			NodeDatabase:    "",
		},
	}

	// Start p2p server
	if err := s.p2pServer.Start(); err != nil {
		s.discovery.Stop()
		return fmt.Errorf("failed to start p2p server: %w", err)
	}

	// Start peer manager
	s.peerManager.Start()

	// Start connection management
	s.wg.Add(1)
	go s.connectionLoop()

	s.logger.Info("p2p server started",
		zap.String("listen_addr", s.config.ListenAddr),
		zap.Int("max_peers", s.config.MaxPeers),
		zap.Uint64("network_id", s.config.NetworkID),
	)

	return nil
}

// Stop stops the P2P server
func (s *Server) Stop() {
	s.cancel()
	s.wg.Wait()

	s.peerManager.Stop()
	s.discovery.Stop()

	if s.p2pServer != nil {
		s.p2pServer.Stop()
	}

	s.logger.Info("p2p server stopped")
}

// makeProtocols creates the protocol specifications
func (s *Server) makeProtocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    protocolName,
			Version: ETH66,
			Length:  17,
			Run:     s.runPeer,
		},
		{
			Name:    protocolName,
			Version: ETH67,
			Length:  17,
			Run:     s.runPeer,
		},
	}
}

// runPeer handles a peer connection
func (s *Server) runPeer(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
	// Create peer wrapper
	peerID := peer.ID().String()
	// Determine protocol version from peer's protocol name
	version := uint32(ETH67) // Default to ETH67
	peerWrapper := NewPeer(peerID, rw, version, s.logger)

	// Perform handshake
	s.statusLock.RLock()
	status := s.status
	s.statusLock.RUnlock()

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	if err := peerWrapper.Handshake(ctx, status); err != nil {
		s.logger.Warn("handshake failed",
			zap.String("peer_id", peerID),
			zap.Error(err),
		)
		return err
	}

	// Add peer to manager
	if err := s.peerManager.AddPeer(peerWrapper); err != nil {
		s.logger.Warn("failed to add peer to manager",
			zap.String("peer_id", peerID),
			zap.Error(err),
		)
		return err
	}

	defer s.peerManager.RemovePeer(peerID)

	s.logger.Info("peer connected",
		zap.String("peer_id", peerID),
		zap.Uint32("protocol_version", version),
		zap.Int("peer_count", s.peerManager.PeerCount()),
	)

	// Run message loop
	if err := s.protocolHandler.RunPeerMessageLoop(s.ctx, peerWrapper); err != nil {
		s.logger.Debug("peer message loop ended",
			zap.String("peer_id", peerID),
			zap.Error(err),
		)
	}

	return nil
}

// connectionLoop manages peer connections
func (s *Server) connectionLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(peerConnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.maintainConnections()
		}
	}
}

// maintainConnections maintains the target number of peer connections
func (s *Server) maintainConnections() {
	peerCount := s.peerManager.PeerCount()

	if peerCount >= s.config.MaxPeers {
		return
	}

	needed := s.config.MaxPeers - peerCount

	// Get random nodes from discovery
	nodes := s.discovery.GetRandomNodes(needed * 2)
	if len(nodes) == 0 {
		return
	}

	connected := 0
	for _, node := range nodes {
		if connected >= needed {
			break
		}

		// Check if already connected
		if _, exists := s.peerManager.GetPeer(node.ID().String()); exists {
			continue
		}

		// Add node to p2p server
		s.p2pServer.AddPeer(node)
		connected++
	}

	if connected > 0 {
		s.logger.Debug("initiated new peer connections",
			zap.Int("count", connected),
			zap.Int("current_peers", peerCount),
		)
	}
}

// onNodeDiscovered is called when a new node is discovered
func (s *Server) onNodeDiscovered(node *enode.Node) {
	if s.peerManager.IsFull() {
		return
	}

	// Check if already connected
	if _, exists := s.peerManager.GetPeer(node.ID().String()); exists {
		return
	}

	// Add to p2p server for dialing
	s.p2pServer.AddPeer(node)
}

// UpdateStatus updates the server's status information
func (s *Server) UpdateStatus(head common.Hash, td *big.Int) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.status.Head = head
	s.status.TD = td.Bytes()

	s.logger.Debug("status updated",
		zap.String("head", head.Hex()),
		zap.String("td", td.String()),
	)
}

// GetStatus returns the current status
func (s *Server) GetStatus() *StatusData {
	s.statusLock.RLock()
	defer s.statusLock.RUnlock()

	return &StatusData{
		ProtocolVersion: s.status.ProtocolVersion,
		NetworkID:       s.status.NetworkID,
		TD:              append([]byte(nil), s.status.TD...),
		Head:            s.status.Head,
		Genesis:         s.status.Genesis,
		ForkID:          s.status.ForkID,
	}
}

// PeerManager returns the peer manager
func (s *Server) PeerManager() *PeerManager {
	return s.peerManager
}

// Discovery returns the discovery service
func (s *Server) Discovery() *Discovery {
	return s.discovery
}

// BroadcastBlock broadcasts a new block to all peers
func (s *Server) BroadcastBlock(block interface{}, td []byte) error {
	packet := &NewBlockPacket{
		Block: block.(*types.Block),
		TD:    td,
	}

	// Broadcast to a subset of peers to avoid network spam
	return s.peerManager.BroadcastMessageToN(NewBlockMsg, packet, 10)
}

// BroadcastBlockHashes broadcasts new block hashes to all peers
func (s *Server) BroadcastBlockHashes(hashes NewBlockHashesPacket) error {
	return s.peerManager.BroadcastMessageToN(NewBlockHashesMsg, hashes, 20)
}

// BroadcastTransactions broadcasts transactions to peers
func (s *Server) BroadcastTransactions(txs TransactionsPacket) error {
	return s.peerManager.BroadcastMessageToN(TransactionsMsg, txs, 15)
}

// RequestBlockHeaders requests block headers from a peer
func (s *Server) RequestBlockHeaders(ctx context.Context, origin HashOrNumber, amount uint64, skip uint64, reverse bool) ([]*BlockHeadersRequest, error) {
	// Get best peer
	bestPeers := s.peerManager.GetBestPeers(1)
	if len(bestPeers) == 0 {
		return nil, fmt.Errorf("no peers available")
	}

	return bestPeers[0].RequestBlockHeaders(ctx, origin, amount, skip, reverse)
}

// RequestBlockBodies requests block bodies from a peer
func (s *Server) RequestBlockBodies(ctx context.Context, hashes []common.Hash) ([]*BlockBody, error) {
	// Get best peer
	bestPeers := s.peerManager.GetBestPeers(1)
	if len(bestPeers) == 0 {
		return nil, fmt.Errorf("no peers available")
	}

	return bestPeers[0].RequestBlockBodies(ctx, hashes)
}

// RequestReceipts requests receipts from a peer
func (s *Server) RequestReceipts(ctx context.Context, hashes []common.Hash) ([][]*ReceiptsResponse, error) {
	// Get best peer
	bestPeers := s.peerManager.GetBestPeers(1)
	if len(bestPeers) == 0 {
		return nil, fmt.Errorf("no peers available")
	}

	return bestPeers[0].RequestReceipts(ctx, hashes)
}

// GetPeerCount returns the current number of peers
func (s *Server) GetPeerCount() int {
	return s.peerManager.PeerCount()
}

// GetPeerStats returns statistics about all peers
func (s *Server) GetPeerStats() map[string]interface{} {
	return s.peerManager.GetStats()
}

// Self returns the local node
func (s *Server) Self() *enode.Node {
	return s.discovery.Self()
}

// AddPeer adds a peer by enode URL
func (s *Server) AddPeer(url string) error {
	node, err := s.discovery.Resolve(url)
	if err != nil {
		return fmt.Errorf("failed to resolve node URL: %w", err)
	}

	s.p2pServer.AddPeer(node)
	return nil
}

// RemovePeer removes a peer by ID
func (s *Server) RemovePeer(peerID string) error {
	return s.peerManager.RemovePeer(peerID)
}

// GetPeers returns all connected peers
func (s *Server) GetPeers() []*Peer {
	return s.peerManager.GetAllPeers()
}
