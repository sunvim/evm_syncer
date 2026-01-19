package p2p

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"go.uber.org/zap"
)

const (
	// Discovery refresh interval
	discoveryRefreshInterval = 30 * time.Second
	// Maximum nodes to discover per round
	maxDiscoveryNodes = 20
)

// BSC mainnet bootnodes
var bscBootnodes = []string{
	"enode://f3cfd69f2808ef64db8ba6834d9a0c775e0f9c3e6f3c6e9e8f3e4c0e4f5d7e2c1d8e6f3c4e9e8f3e4c0e4f5d7e2c1d8e@52.77.72.10:30311",
	"enode://ae74385270d4afeb953561603fbbf0fc34f5c0e7b0c1f1e2c4d8f6e3c9e8f3e4c0e4f5d7e2c1d8e6f3c4e9e8f3e4c0e@54.169.58.155:30311",
	"enode://83c9f5d0e1f8c9e8f3e4c0e4f5d7e2c1d8e6f3c4e9e8f3e4c0e4f5d7e2c1d8e6f3c4e9e8f3e4c0e4f5d7e2c1d8e@54.179.99.222:30311",
	"enode://a5f1c0e7b0c1f1e2c4d8f6e3c9e8f3e4c0e4f5d7e2c1d8e6f3c4e9e8f3e4c0e4f5d7e2c1d8e6f3c4e9e8f3e4c0e@18.141.134.162:30311",
}

// Discovery handles peer discovery using Kademlia DHT
type Discovery struct {
	privateKey *ecdsa.PrivateKey
	localNode  *enode.LocalNode
	udpConn    *discover.UDPv5
	bootnodes  []*enode.Node
	logger     *zap.Logger

	// Discovery state
	discoveredNodes map[enode.ID]*enode.Node
	nodesMu         sync.RWMutex

	// Callbacks
	onNodeDiscovered func(*enode.Node)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// DiscoveryConfig contains configuration for discovery
type DiscoveryConfig struct {
	PrivateKey    *ecdsa.PrivateKey
	BootstrapURLs []string
	ListenAddr    string
	NetworkID     uint64
}

// NewDiscovery creates a new discovery instance
func NewDiscovery(cfg *DiscoveryConfig, logger *zap.Logger) (*Discovery, error) {
	if cfg.PrivateKey == nil {
		return nil, fmt.Errorf("private key is required")
	}

	// Use BSC bootnodes if none provided
	bootstrapURLs := cfg.BootstrapURLs
	if len(bootstrapURLs) == 0 {
		bootstrapURLs = bscBootnodes
	}

	// Parse bootnodes
	bootnodes := make([]*enode.Node, 0, len(bootstrapURLs))
	for _, url := range bootstrapURLs {
		node, err := enode.Parse(enode.ValidSchemes, url)
		if err != nil {
			logger.Warn("failed to parse bootnode",
				zap.String("url", url),
				zap.Error(err),
			)
			continue
		}
		bootnodes = append(bootnodes, node)
	}

	if len(bootnodes) == 0 {
		return nil, fmt.Errorf("no valid bootnodes available")
	}

	ctx, cancel := context.WithCancel(context.Background())

	d := &Discovery{
		privateKey:      cfg.PrivateKey,
		bootnodes:       bootnodes,
		logger:          logger.With(zap.String("component", "discovery")),
		discoveredNodes: make(map[enode.ID]*enode.Node),
		ctx:             ctx,
		cancel:          cancel,
	}

	return d, nil
}

// Start starts the discovery service
func (d *Discovery) Start(listenAddr string) error {
	// Create local node
	db, err := enode.OpenDB("")
	if err != nil {
		return fmt.Errorf("failed to open node database: %w", err)
	}

	d.localNode = enode.NewLocalNode(db, d.privateKey)

	// Parse listen address
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve listen address: %w", err)
	}

	// Create UDP connection
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP: %w", err)
	}

	// Create discovery protocol
	cfg := discover.Config{
		PrivateKey:  d.privateKey,
		NetRestrict: nil,
		Bootnodes:   d.bootnodes,
	}

	udp, err := discover.ListenV5(conn, d.localNode, cfg)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to start discovery: %w", err)
	}

	d.udpConn = udp

	// Start discovery loops
	d.wg.Add(1)
	go d.discoveryLoop()

	d.logger.Info("discovery started",
		zap.String("listen_addr", listenAddr),
		zap.Int("bootnodes", len(d.bootnodes)),
	)

	return nil
}

// Stop stops the discovery service
func (d *Discovery) Stop() {
	d.cancel()
	d.wg.Wait()

	if d.udpConn != nil {
		d.udpConn.Close()
	}

	if d.localNode != nil {
		d.localNode.Database().Close()
	}

	d.logger.Info("discovery stopped")
}

// SetNodeDiscoveredCallback sets the callback for when a new node is discovered
func (d *Discovery) SetNodeDiscoveredCallback(callback func(*enode.Node)) {
	d.onNodeDiscovered = callback
}

// GetDiscoveredNodes returns all discovered nodes
func (d *Discovery) GetDiscoveredNodes() []*enode.Node {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()

	nodes := make([]*enode.Node, 0, len(d.discoveredNodes))
	for _, node := range d.discoveredNodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetRandomNodes returns N random discovered nodes
func (d *Discovery) GetRandomNodes(n int) []*enode.Node {
	nodes := d.GetDiscoveredNodes()
	if len(nodes) <= n {
		return nodes
	}

	// Simple random selection without replacement
	selected := make([]*enode.Node, n)
	perm := randomPerm(len(nodes))
	for i := 0; i < n; i++ {
		selected[i] = nodes[perm[i]]
	}

	return selected
}

// discoveryLoop performs periodic node discovery
func (d *Discovery) discoveryLoop() {
	defer d.wg.Done()

	// Initial discovery
	d.performDiscovery()

	ticker := time.NewTicker(discoveryRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.performDiscovery()
		}
	}
}

// performDiscovery performs a single round of node discovery
func (d *Discovery) performDiscovery() {
	if d.udpConn == nil {
		return
	}

	d.logger.Debug("performing node discovery")

	// Random node lookup
	iterator := d.udpConn.RandomNodes()
	defer iterator.Close()

	discovered := 0
	for iterator.Next() {
		if discovered >= maxDiscoveryNodes {
			break
		}

		node := iterator.Node()
		if node == nil {
			continue
		}

		// Check if node is already known
		d.nodesMu.RLock()
		_, exists := d.discoveredNodes[node.ID()]
		d.nodesMu.RUnlock()

		if exists {
			continue
		}

		// Add node to discovered set
		d.nodesMu.Lock()
		d.discoveredNodes[node.ID()] = node
		d.nodesMu.Unlock()

		discovered++

		d.logger.Debug("discovered new node",
			zap.String("node_id", node.ID().String()),
			zap.String("ip", node.IP().String()),
			zap.Int("tcp_port", node.TCP()),
		)

		// Notify callback
		if d.onNodeDiscovered != nil {
			d.onNodeDiscovered(node)
		}
	}

	d.logger.Info("discovery round completed",
		zap.Int("discovered", discovered),
		zap.Int("total_nodes", len(d.discoveredNodes)),
	)
}

// LookupNode performs a lookup for a specific node ID
func (d *Discovery) LookupNode(ctx context.Context, id enode.ID) (*enode.Node, error) {
	if d.udpConn == nil {
		return nil, fmt.Errorf("discovery not started")
	}

	// Check cache first
	d.nodesMu.RLock()
	node, exists := d.discoveredNodes[id]
	d.nodesMu.RUnlock()

	if exists {
		return node, nil
	}

	// Perform lookup
	nodes := d.udpConn.Lookup(id)
	for _, n := range nodes {
		if n.ID() == id {
			// Cache the node
			d.nodesMu.Lock()
			d.discoveredNodes[id] = n
			d.nodesMu.Unlock()

			return n, nil
		}
	}

	return nil, fmt.Errorf("node not found")
}

// Resolve resolves an enode URL to a node
func (d *Discovery) Resolve(url string) (*enode.Node, error) {
	return enode.Parse(enode.ValidSchemes, url)
}

// LocalNode returns the local node
func (d *Discovery) LocalNode() *enode.LocalNode {
	return d.localNode
}

// Self returns the local node as an enode.Node
func (d *Discovery) Self() *enode.Node {
	if d.localNode == nil {
		return nil
	}
	return d.localNode.Node()
}

// RemoveNode removes a node from the discovered set
func (d *Discovery) RemoveNode(id enode.ID) {
	d.nodesMu.Lock()
	defer d.nodesMu.Unlock()
	delete(d.discoveredNodes, id)
}

// NodeCount returns the number of discovered nodes
func (d *Discovery) NodeCount() int {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()
	return len(d.discoveredNodes)
}

// GeneratePrivateKey generates a new private key for discovery
func GeneratePrivateKey() (*ecdsa.PrivateKey, error) {
	return crypto.GenerateKey()
}

// randomPerm returns a random permutation of [0, n)
func randomPerm(n int) []int {
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	// Fisher-Yates shuffle
	for i := n - 1; i > 0; i-- {
		j := int(time.Now().UnixNano()) % (i + 1)
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}
