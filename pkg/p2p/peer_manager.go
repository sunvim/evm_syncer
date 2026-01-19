package p2p

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// Target number of active peers
	minPeers = 30
	maxPeers = 50

	// Peer management intervals
	peerCheckInterval = 10 * time.Second
	peerHealthCheck   = 30 * time.Second

	// Scoring thresholds
	disconnectScore = 10
	goodPeerScore   = 70
)

var (
	// ErrPeerPoolFull is returned when trying to add a peer to a full pool
	ErrPeerPoolFull = errors.New("peer pool is full")
	// ErrPeerExists is returned when trying to add a peer that already exists
	ErrPeerExists = errors.New("peer already exists")
	// ErrPeerNotFound is returned when a peer is not found
	ErrPeerNotFound = errors.New("peer not found")
)

// PeerManager manages the pool of connected peers
type PeerManager struct {
	peers   map[string]*Peer
	peersMu sync.RWMutex

	maxPeers int
	minPeers int

	logger *zap.Logger

	// Channels for peer events
	peerAddCh    chan *Peer
	peerRemoveCh chan string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPeerManager creates a new peer manager
func NewPeerManager(logger *zap.Logger) *PeerManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &PeerManager{
		peers:        make(map[string]*Peer),
		maxPeers:     maxPeers,
		minPeers:     minPeers,
		logger:       logger.With(zap.String("component", "peer_manager")),
		peerAddCh:    make(chan *Peer, 10),
		peerRemoveCh: make(chan string, 10),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start starts the peer manager
func (pm *PeerManager) Start() {
	pm.wg.Add(2)
	go pm.managementLoop()
	go pm.healthCheckLoop()

	pm.logger.Info("peer manager started")
}

// Stop stops the peer manager
func (pm *PeerManager) Stop() {
	pm.cancel()
	pm.wg.Wait()

	// Disconnect all peers
	pm.peersMu.Lock()
	for _, peer := range pm.peers {
		peer.Disconnect()
	}
	pm.peers = make(map[string]*Peer)
	pm.peersMu.Unlock()

	pm.logger.Info("peer manager stopped")
}

// AddPeer adds a new peer to the pool
func (pm *PeerManager) AddPeer(peer *Peer) error {
	pm.peersMu.Lock()
	defer pm.peersMu.Unlock()

	if len(pm.peers) >= pm.maxPeers {
		return ErrPeerPoolFull
	}

	if _, exists := pm.peers[peer.ID()]; exists {
		return ErrPeerExists
	}

	pm.peers[peer.ID()] = peer
	pm.logger.Info("peer added",
		zap.String("peer_id", peer.ID()),
		zap.Int("peer_count", len(pm.peers)),
	)

	return nil
}

// RemovePeer removes a peer from the pool
func (pm *PeerManager) RemovePeer(peerID string) error {
	pm.peersMu.Lock()
	defer pm.peersMu.Unlock()

	peer, exists := pm.peers[peerID]
	if !exists {
		return ErrPeerNotFound
	}

	peer.Disconnect()
	delete(pm.peers, peerID)

	pm.logger.Info("peer removed",
		zap.String("peer_id", peerID),
		zap.Int("peer_count", len(pm.peers)),
	)

	return nil
}

// GetPeer returns a peer by ID
func (pm *PeerManager) GetPeer(peerID string) (*Peer, bool) {
	pm.peersMu.RLock()
	defer pm.peersMu.RUnlock()

	peer, exists := pm.peers[peerID]
	return peer, exists
}

// GetAllPeers returns all connected peers
func (pm *PeerManager) GetAllPeers() []*Peer {
	pm.peersMu.RLock()
	defer pm.peersMu.RUnlock()

	peers := make([]*Peer, 0, len(pm.peers))
	for _, peer := range pm.peers {
		peers = append(peers, peer)
	}

	return peers
}

// GetBestPeers returns the top N peers by score
func (pm *PeerManager) GetBestPeers(n int) []*Peer {
	allPeers := pm.GetAllPeers()

	if len(allPeers) <= n {
		return allPeers
	}

	// Sort peers by score (simple bubble sort for small n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < len(allPeers); j++ {
			if allPeers[j].Score() > allPeers[i].Score() {
				allPeers[i], allPeers[j] = allPeers[j], allPeers[i]
			}
		}
	}

	return allPeers[:n]
}

// GetRandomPeer returns a random peer from the pool
func (pm *PeerManager) GetRandomPeer() (*Peer, error) {
	peers := pm.GetAllPeers()
	if len(peers) == 0 {
		return nil, errors.New("no peers available")
	}

	return peers[rand.Intn(len(peers))], nil
}

// GetRandomPeers returns N random peers from the pool
func (pm *PeerManager) GetRandomPeers(n int) []*Peer {
	allPeers := pm.GetAllPeers()
	if len(allPeers) <= n {
		return allPeers
	}

	// Shuffle and return first n
	rand.Shuffle(len(allPeers), func(i, j int) {
		allPeers[i], allPeers[j] = allPeers[j], allPeers[i]
	})

	return allPeers[:n]
}

// PeerCount returns the number of connected peers
func (pm *PeerManager) PeerCount() int {
	pm.peersMu.RLock()
	defer pm.peersMu.RUnlock()
	return len(pm.peers)
}

// IsFull returns whether the peer pool is full
func (pm *PeerManager) IsFull() bool {
	return pm.PeerCount() >= pm.maxPeers
}

// NeedsPeers returns whether the peer pool needs more peers
func (pm *PeerManager) NeedsPeers() bool {
	return pm.PeerCount() < pm.minPeers
}

// GetStats returns statistics about the peer pool
func (pm *PeerManager) GetStats() map[string]interface{} {
	pm.peersMu.RLock()
	defer pm.peersMu.RUnlock()

	totalScore := 0
	avgResponseTime := uint64(0)
	peerStats := make([]map[string]interface{}, 0, len(pm.peers))

	for _, peer := range pm.peers {
		totalScore += peer.Score()
		stats := peer.GetStats()
		peerStats = append(peerStats, stats)

		if rt, ok := stats["avg_response_time"].(uint64); ok {
			avgResponseTime += rt
		}
	}

	avgScore := 0
	if len(pm.peers) > 0 {
		avgScore = totalScore / len(pm.peers)
		avgResponseTime = avgResponseTime / uint64(len(pm.peers))
	}

	return map[string]interface{}{
		"peer_count":        len(pm.peers),
		"max_peers":         pm.maxPeers,
		"min_peers":         pm.minPeers,
		"avg_score":         avgScore,
		"avg_response_time": avgResponseTime,
		"peers":             peerStats,
	}
}

// managementLoop handles periodic peer management tasks
func (pm *PeerManager) managementLoop() {
	defer pm.wg.Done()

	ticker := time.NewTicker(peerCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.managePeers()
		}
	}
}

// healthCheckLoop performs periodic health checks on peers
func (pm *PeerManager) healthCheckLoop() {
	defer pm.wg.Done()

	ticker := time.NewTicker(peerHealthCheck)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.checkPeerHealth()
		}
	}
}

// managePeers performs peer management tasks
func (pm *PeerManager) managePeers() {
	pm.peersMu.Lock()
	defer pm.peersMu.Unlock()

	toRemove := make([]string, 0)

	for id, peer := range pm.peers {
		// Check if peer is still connected
		if !peer.IsConnected() {
			toRemove = append(toRemove, id)
			continue
		}

		// Check peer score
		if peer.Score() < disconnectScore {
			pm.logger.Warn("disconnecting low-score peer",
				zap.String("peer_id", id),
				zap.Int("score", peer.Score()),
			)
			toRemove = append(toRemove, id)
			continue
		}
	}

	// Remove disconnected or low-score peers
	for _, id := range toRemove {
		if peer, exists := pm.peers[id]; exists {
			peer.Disconnect()
			delete(pm.peers, id)
		}
	}

	if len(toRemove) > 0 {
		pm.logger.Info("removed peers",
			zap.Int("count", len(toRemove)),
			zap.Int("remaining", len(pm.peers)),
		)
	}
}

// checkPeerHealth performs health checks on all peers
func (pm *PeerManager) checkPeerHealth() {
	peers := pm.GetAllPeers()

	for _, peer := range peers {
		if !peer.IsConnected() {
			pm.RemovePeer(peer.ID())
			continue
		}

		// Log peer status
		stats := peer.GetStats()
		pm.logger.Debug("peer health check",
			zap.String("peer_id", peer.ID()),
			zap.Any("stats", stats),
		)
	}
}

// BroadcastMessage broadcasts a message to all connected peers
func (pm *PeerManager) BroadcastMessage(code uint64, data interface{}) error {
	peers := pm.GetAllPeers()
	if len(peers) == 0 {
		return errors.New("no peers to broadcast to")
	}

	errCount := 0
	for _, peer := range peers {
		if err := peer.SendMessage(code, data); err != nil {
			pm.logger.Warn("failed to send message to peer",
				zap.String("peer_id", peer.ID()),
				zap.Error(err),
			)
			errCount++
		}
	}

	if errCount > 0 {
		return fmt.Errorf("failed to send to %d/%d peers", errCount, len(peers))
	}

	return nil
}

// BroadcastMessageToN broadcasts a message to N random peers
func (pm *PeerManager) BroadcastMessageToN(code uint64, data interface{}, n int) error {
	peers := pm.GetRandomPeers(n)
	if len(peers) == 0 {
		return errors.New("no peers to broadcast to")
	}

	errCount := 0
	for _, peer := range peers {
		if err := peer.SendMessage(code, data); err != nil {
			pm.logger.Warn("failed to send message to peer",
				zap.String("peer_id", peer.ID()),
				zap.Error(err),
			)
			errCount++
		}
	}

	if errCount > 0 {
		return fmt.Errorf("failed to send to %d/%d peers", errCount, len(peers))
	}

	return nil
}

// SendToBestPeer sends a message to the best peer (highest score)
func (pm *PeerManager) SendToBestPeer(code uint64, data interface{}) error {
	bestPeers := pm.GetBestPeers(1)
	if len(bestPeers) == 0 {
		return errors.New("no peers available")
	}

	return bestPeers[0].SendMessage(code, data)
}

// FilterPeers returns peers matching the given predicate
func (pm *PeerManager) FilterPeers(predicate func(*Peer) bool) []*Peer {
	allPeers := pm.GetAllPeers()
	filtered := make([]*Peer, 0)

	for _, peer := range allPeers {
		if predicate(peer) {
			filtered = append(filtered, peer)
		}
	}

	return filtered
}

// GetPeersByMinScore returns peers with a score above the threshold
func (pm *PeerManager) GetPeersByMinScore(minScore int) []*Peer {
	return pm.FilterPeers(func(p *Peer) bool {
		return p.Score() >= minScore
	})
}

// GetGoodPeers returns peers with a good score
func (pm *PeerManager) GetGoodPeers() []*Peer {
	return pm.GetPeersByMinScore(goodPeerScore)
}
