package p2p

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
)

const (
	// Peer health check interval
	healthCheckInterval = 30 * time.Second
	// Maximum allowed response time for requests
	maxResponseTime = 30 * time.Second
	// Initial peer score
	initialScore = 50
	// Maximum peer score
	maxScore = 100
	// Minimum peer score before disconnection
	minScore = 10
)

// Peer represents a connected peer in the network
type Peer struct {
	id       string
	rw       p2p.MsgReadWriter
	version  uint32
	logger   *zap.Logger
	doneChan chan struct{}

	// Peer information
	networkID uint64
	head      common.Hash
	td        *big.Int
	genesis   common.Hash
	forkID    ForkID

	// Scoring and health
	score          int
	scoreMu        sync.RWMutex
	lastSeen       time.Time
	lastSeenMu     sync.RWMutex
	requestCount   uint64
	responseCount  uint64
	responseTimeMs uint64
	statsMu        sync.RWMutex

	// Pending requests
	pendingRequests map[uint64]*pendingRequest
	requestsMu      sync.RWMutex
	nextRequestID   uint64

	// Connection state
	connected bool
	connMu    sync.RWMutex
}

// pendingRequest tracks an outstanding request
type pendingRequest struct {
	requestID uint64
	sentAt    time.Time
	timeout   *time.Timer
	replyCh   chan interface{}
	errCh     chan error
}

// NewPeer creates a new peer instance
func NewPeer(id string, rw p2p.MsgReadWriter, version uint32, logger *zap.Logger) *Peer {
	return &Peer{
		id:              id,
		rw:              rw,
		version:         version,
		logger:          logger.With(zap.String("peer_id", id)),
		doneChan:        make(chan struct{}),
		score:           initialScore,
		lastSeen:        time.Now(),
		pendingRequests: make(map[uint64]*pendingRequest),
		nextRequestID:   1,
		connected:       true,
	}
}

// ID returns the peer identifier
func (p *Peer) ID() string {
	return p.id
}

// Version returns the protocol version
func (p *Peer) Version() uint32 {
	return p.version
}

// NetworkID returns the network ID
func (p *Peer) NetworkID() uint64 {
	return p.networkID
}

// Head returns the peer's head block hash
func (p *Peer) Head() common.Hash {
	return p.head
}

// TD returns the peer's total difficulty
func (p *Peer) TD() *big.Int {
	return new(big.Int).Set(p.td)
}

// Score returns the current peer score
func (p *Peer) Score() int {
	p.scoreMu.RLock()
	defer p.scoreMu.RUnlock()
	return p.score
}

// IsConnected returns whether the peer is currently connected
func (p *Peer) IsConnected() bool {
	p.connMu.RLock()
	defer p.connMu.RUnlock()
	return p.connected
}

// Handshake performs the initial status exchange with the peer
func (p *Peer) Handshake(ctx context.Context, status *StatusData) error {
	// Send our status
	if err := p.SendStatus(status); err != nil {
		return fmt.Errorf("failed to send status: %w", err)
	}

	// Receive peer status
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("failed to read status message: %w", err)
	}
	defer msg.Discard()

	if msg.Code != StatusMsg {
		return fmt.Errorf("expected status message, got code %d", msg.Code)
	}

	var peerStatus StatusData
	if err := msg.Decode(&peerStatus); err != nil {
		return fmt.Errorf("failed to decode status message: %w", err)
	}

	// Validate peer status
	if peerStatus.NetworkID != status.NetworkID {
		return fmt.Errorf("network ID mismatch: got %d, expected %d", peerStatus.NetworkID, status.NetworkID)
	}

	if peerStatus.Genesis != status.Genesis {
		return fmt.Errorf("genesis mismatch: got %s, expected %s", peerStatus.Genesis.Hex(), status.Genesis.Hex())
	}

	// Store peer information
	p.networkID = peerStatus.NetworkID
	p.head = peerStatus.Head
	p.td = new(big.Int).SetBytes(peerStatus.TD)
	p.genesis = peerStatus.Genesis
	p.forkID = peerStatus.ForkID

	p.logger.Info("handshake completed",
		zap.Uint64("network_id", p.networkID),
		zap.String("head", p.head.Hex()),
		zap.String("td", p.td.String()),
	)

	return nil
}

// SendStatus sends a status message to the peer
func (p *Peer) SendStatus(status *StatusData) error {
	return p2p.Send(p.rw, StatusMsg, status)
}

// RequestBlockHeaders sends a GetBlockHeaders request
func (p *Peer) RequestBlockHeaders(ctx context.Context, origin HashOrNumber, amount uint64, skip uint64, reverse bool) ([]*BlockHeadersRequest, error) {
	if !p.IsConnected() {
		return nil, errors.New("peer not connected")
	}

	requestID := p.getNextRequestID()
	replyCh := make(chan interface{}, 1)
	errCh := make(chan error, 1)

	// Register pending request
	pr := &pendingRequest{
		requestID: requestID,
		sentAt:    time.Now(),
		timeout:   time.NewTimer(maxResponseTime),
		replyCh:   replyCh,
		errCh:     errCh,
	}

	p.requestsMu.Lock()
	p.pendingRequests[requestID] = pr
	p.requestsMu.Unlock()

	// Send request
	req := &GetBlockHeadersPacket{
		RequestId: requestID,
		GetBlockHeadersRequest: GetBlockHeadersRequest{
			Origin:  origin,
			Amount:  amount,
			Skip:    skip,
			Reverse: reverse,
		},
	}

	if err := p2p.Send(p.rw, GetBlockHeadersMsg, req); err != nil {
		p.removeRequest(requestID)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	p.incrementRequestCount()

	// Wait for response
	select {
	case <-ctx.Done():
		p.removeRequest(requestID)
		return nil, ctx.Err()
	case <-pr.timeout.C:
		p.removeRequest(requestID)
		p.decreaseScore(5)
		return nil, errors.New("request timeout")
	case err := <-errCh:
		return nil, err
	case reply := <-replyCh:
		headers, ok := reply.([]*BlockHeadersRequest)
		if !ok {
			return nil, errors.New("invalid response type")
		}
		return headers, nil
	}
}

// RequestBlockBodies sends a GetBlockBodies request
func (p *Peer) RequestBlockBodies(ctx context.Context, hashes []common.Hash) ([]*BlockBody, error) {
	if !p.IsConnected() {
		return nil, errors.New("peer not connected")
	}

	requestID := p.getNextRequestID()
	replyCh := make(chan interface{}, 1)
	errCh := make(chan error, 1)

	pr := &pendingRequest{
		requestID: requestID,
		sentAt:    time.Now(),
		timeout:   time.NewTimer(maxResponseTime),
		replyCh:   replyCh,
		errCh:     errCh,
	}

	p.requestsMu.Lock()
	p.pendingRequests[requestID] = pr
	p.requestsMu.Unlock()

	req := &GetBlockBodiesPacket{
		RequestId:             requestID,
		GetBlockBodiesRequest: hashes,
	}

	if err := p2p.Send(p.rw, GetBlockBodiesMsg, req); err != nil {
		p.removeRequest(requestID)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	p.incrementRequestCount()

	select {
	case <-ctx.Done():
		p.removeRequest(requestID)
		return nil, ctx.Err()
	case <-pr.timeout.C:
		p.removeRequest(requestID)
		p.decreaseScore(5)
		return nil, errors.New("request timeout")
	case err := <-errCh:
		return nil, err
	case reply := <-replyCh:
		bodies, ok := reply.([]*BlockBody)
		if !ok {
			return nil, errors.New("invalid response type")
		}
		return bodies, nil
	}
}

// RequestReceipts sends a GetReceipts request
func (p *Peer) RequestReceipts(ctx context.Context, hashes []common.Hash) ([][]*ReceiptsResponse, error) {
	if !p.IsConnected() {
		return nil, errors.New("peer not connected")
	}

	requestID := p.getNextRequestID()
	replyCh := make(chan interface{}, 1)
	errCh := make(chan error, 1)

	pr := &pendingRequest{
		requestID: requestID,
		sentAt:    time.Now(),
		timeout:   time.NewTimer(maxResponseTime),
		replyCh:   replyCh,
		errCh:     errCh,
	}

	p.requestsMu.Lock()
	p.pendingRequests[requestID] = pr
	p.requestsMu.Unlock()

	req := &GetReceiptsPacket{
		RequestId:          requestID,
		GetReceiptsRequest: hashes,
	}

	if err := p2p.Send(p.rw, GetReceiptsMsg, req); err != nil {
		p.removeRequest(requestID)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	p.incrementRequestCount()

	select {
	case <-ctx.Done():
		p.removeRequest(requestID)
		return nil, ctx.Err()
	case <-pr.timeout.C:
		p.removeRequest(requestID)
		p.decreaseScore(5)
		return nil, errors.New("request timeout")
	case err := <-errCh:
		return nil, err
	case reply := <-replyCh:
		receipts, ok := reply.([][]*ReceiptsResponse)
		if !ok {
			return nil, errors.New("invalid response type")
		}
		return receipts, nil
	}
}

// SendMessage sends a generic message to the peer
func (p *Peer) SendMessage(code uint64, data interface{}) error {
	if !p.IsConnected() {
		return errors.New("peer not connected")
	}
	return p2p.Send(p.rw, code, data)
}

// ReadMessage reads a message from the peer
func (p *Peer) ReadMessage() (p2p.Msg, error) {
	return p.rw.ReadMsg()
}

// HandleResponse handles a response message
func (p *Peer) HandleResponse(requestID uint64, data interface{}) {
	p.requestsMu.Lock()
	pr, exists := p.pendingRequests[requestID]
	if !exists {
		p.requestsMu.Unlock()
		p.logger.Warn("received response for unknown request", zap.Uint64("request_id", requestID))
		return
	}
	delete(p.pendingRequests, requestID)
	p.requestsMu.Unlock()

	pr.timeout.Stop()
	responseTime := time.Since(pr.sentAt)
	p.updateResponseTime(responseTime)
	p.incrementResponseCount()
	p.increaseScore(1)

	select {
	case pr.replyCh <- data:
	default:
		p.logger.Warn("response channel full", zap.Uint64("request_id", requestID))
	}
}

// UpdateHead updates the peer's head block information
func (p *Peer) UpdateHead(head common.Hash, td *big.Int) {
	p.head = head
	p.td = new(big.Int).Set(td)
	p.updateLastSeen()
}

// Disconnect marks the peer as disconnected
func (p *Peer) Disconnect() {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	if !p.connected {
		return
	}

	p.connected = false
	close(p.doneChan)

	// Cancel all pending requests
	p.requestsMu.Lock()
	for id, pr := range p.pendingRequests {
		pr.timeout.Stop()
		select {
		case pr.errCh <- errors.New("peer disconnected"):
		default:
		}
		delete(p.pendingRequests, id)
	}
	p.requestsMu.Unlock()

	p.logger.Info("peer disconnected")
}

// Done returns a channel that is closed when the peer is disconnected
func (p *Peer) Done() <-chan struct{} {
	return p.doneChan
}

// GetStats returns peer statistics
func (p *Peer) GetStats() map[string]interface{} {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	p.scoreMu.RLock()
	defer p.scoreMu.RUnlock()
	p.lastSeenMu.RLock()
	defer p.lastSeenMu.RUnlock()

	return map[string]interface{}{
		"id":                p.id,
		"version":           p.version,
		"network_id":        p.networkID,
		"head":              p.head.Hex(),
		"td":                p.td.String(),
		"score":             p.score,
		"request_count":     p.requestCount,
		"response_count":    p.responseCount,
		"avg_response_time": p.responseTimeMs,
		"last_seen":         p.lastSeen,
	}
}

// Helper methods

func (p *Peer) getNextRequestID() uint64 {
	p.requestsMu.Lock()
	defer p.requestsMu.Unlock()
	id := p.nextRequestID
	p.nextRequestID++
	return id
}

func (p *Peer) removeRequest(requestID uint64) {
	p.requestsMu.Lock()
	defer p.requestsMu.Unlock()

	if pr, exists := p.pendingRequests[requestID]; exists {
		pr.timeout.Stop()
		delete(p.pendingRequests, requestID)
	}
}

func (p *Peer) incrementRequestCount() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.requestCount++
}

func (p *Peer) incrementResponseCount() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.responseCount++
}

func (p *Peer) updateResponseTime(duration time.Duration) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	ms := uint64(duration.Milliseconds())
	if p.responseCount == 0 {
		p.responseTimeMs = ms
	} else {
		p.responseTimeMs = (p.responseTimeMs + ms) / 2
	}
}

func (p *Peer) updateLastSeen() {
	p.lastSeenMu.Lock()
	defer p.lastSeenMu.Unlock()
	p.lastSeen = time.Now()
}

func (p *Peer) increaseScore(delta int) {
	p.scoreMu.Lock()
	defer p.scoreMu.Unlock()
	p.score += delta
	if p.score > maxScore {
		p.score = maxScore
	}
}

func (p *Peer) decreaseScore(delta int) {
	p.scoreMu.Lock()
	defer p.scoreMu.Unlock()
	p.score -= delta
	if p.score < 0 {
		p.score = 0
	}
}

// String returns a string representation of the peer
func (p *Peer) String() string {
	return fmt.Sprintf("Peer{id=%s, version=%d, score=%d}", p.id, p.version, p.Score())
}

// DecodeMessage decodes a message based on its code
func (p *Peer) DecodeMessage(msg p2p.Msg) (interface{}, error) {
	var payload []byte
	if err := msg.Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode message payload: %w", err)
	}
	return DecodePacket(msg.Code, payload, p.version)
}

// EncodeMessage encodes a message for sending
func (p *Peer) EncodeMessage(data interface{}) ([]byte, error) {
	return rlp.EncodeToBytes(data)
}
