package p2p

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p"
	"go.uber.org/zap"
)

// MessageHandler defines a function that handles a protocol message
type MessageHandler func(peer *Peer, msg p2p.Msg) error

// ProtocolHandler handles protocol messages for eth/66 and eth/67
type ProtocolHandler struct {
	logger         *zap.Logger
	peerManager    *PeerManager
	blockHandler   BlockHandler
	txHandler      TransactionHandler
	receiptHandler ReceiptHandler
}

// BlockHandler defines callbacks for block-related events
type BlockHandler interface {
	HandleNewBlock(peer *Peer, block *types.Block, td []byte) error
	HandleNewBlockHashes(peer *Peer, hashes []struct {
		Hash   [32]byte
		Number uint64
	}) error
	HandleBlockHeaders(peer *Peer, requestID uint64, headers []*types.Header) error
	HandleBlockBodies(peer *Peer, requestID uint64, bodies []*BlockBody) error
}

// TransactionHandler defines callbacks for transaction-related events
type TransactionHandler interface {
	HandleTransactions(peer *Peer, txs []*types.Transaction) error
}

// ReceiptHandler defines callbacks for receipt-related events
type ReceiptHandler interface {
	HandleReceipts(peer *Peer, requestID uint64, receipts [][]*types.Receipt) error
}

// NewProtocolHandler creates a new protocol handler
func NewProtocolHandler(
	logger *zap.Logger,
	peerManager *PeerManager,
	blockHandler BlockHandler,
	txHandler TransactionHandler,
	receiptHandler ReceiptHandler,
) *ProtocolHandler {
	return &ProtocolHandler{
		logger:         logger.With(zap.String("component", "protocol_handler")),
		peerManager:    peerManager,
		blockHandler:   blockHandler,
		txHandler:      txHandler,
		receiptHandler: receiptHandler,
	}
}

// HandleMessage handles an incoming protocol message
func (ph *ProtocolHandler) HandleMessage(peer *Peer, msg p2p.Msg) error {
	// Log message receipt
	ph.logger.Debug("received message",
		zap.String("peer_id", peer.ID()),
		zap.Uint64("msg_code", msg.Code),
		zap.Uint32("msg_size", msg.Size),
	)

	switch msg.Code {
	case StatusMsg:
		return ph.handleStatus(peer, msg)
	case NewBlockHashesMsg:
		return ph.handleNewBlockHashes(peer, msg)
	case TransactionsMsg:
		return ph.handleTransactions(peer, msg)
	case GetBlockHeadersMsg:
		return ph.handleGetBlockHeaders(peer, msg)
	case BlockHeadersMsg:
		return ph.handleBlockHeaders(peer, msg)
	case GetBlockBodiesMsg:
		return ph.handleGetBlockBodies(peer, msg)
	case BlockBodiesMsg:
		return ph.handleBlockBodies(peer, msg)
	case NewBlockMsg:
		return ph.handleNewBlock(peer, msg)
	case GetReceiptsMsg:
		return ph.handleGetReceipts(peer, msg)
	case ReceiptsMsg:
		return ph.handleReceipts(peer, msg)
	default:
		ph.logger.Warn("unknown message code",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("code", msg.Code),
		)
		return fmt.Errorf("unknown message code: %d", msg.Code)
	}
}

// handleStatus handles a status message
func (ph *ProtocolHandler) handleStatus(peer *Peer, msg p2p.Msg) error {
	// Status should only be sent during handshake
	ph.logger.Warn("unexpected status message after handshake",
		zap.String("peer_id", peer.ID()),
	)
	return fmt.Errorf("unexpected status message")
}

// handleNewBlockHashes handles a new block hashes announcement
func (ph *ProtocolHandler) handleNewBlockHashes(peer *Peer, msg p2p.Msg) error {
	var hashes NewBlockHashesPacket
	if err := msg.Decode(&hashes); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode new block hashes: %w", err)
	}

	if len(hashes) == 0 {
		return nil
	}

	ph.logger.Debug("received new block hashes",
		zap.String("peer_id", peer.ID()),
		zap.Int("count", len(hashes)),
	)

	// Convert to compatible format
	convertedHashes := make([]struct {
		Hash   [32]byte
		Number uint64
	}, len(hashes))

	for i, h := range hashes {
		copy(convertedHashes[i].Hash[:], h.Hash[:])
		convertedHashes[i].Number = h.Number
	}

	if ph.blockHandler != nil {
		return ph.blockHandler.HandleNewBlockHashes(peer, convertedHashes)
	}

	return nil
}

// handleTransactions handles a transactions broadcast
func (ph *ProtocolHandler) handleTransactions(peer *Peer, msg p2p.Msg) error {
	var txs TransactionsPacket
	if err := msg.Decode(&txs); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode transactions: %w", err)
	}

	if len(txs) == 0 {
		return nil
	}

	ph.logger.Debug("received transactions",
		zap.String("peer_id", peer.ID()),
		zap.Int("count", len(txs)),
	)

	if ph.txHandler != nil {
		return ph.txHandler.HandleTransactions(peer, txs)
	}

	return nil
}

// handleGetBlockHeaders handles a get block headers request
func (ph *ProtocolHandler) handleGetBlockHeaders(peer *Peer, msg p2p.Msg) error {
	if peer.Version() >= ETH66 {
		var req GetBlockHeadersPacket
		if err := msg.Decode(&req); err != nil {
			peer.decreaseScore(2)
			return fmt.Errorf("failed to decode get block headers packet: %w", err)
		}

		ph.logger.Debug("received get block headers request",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("request_id", req.RequestId),
			zap.Uint64("amount", req.Amount),
		)

		// TODO: Implement response logic
		// For now, send empty response
		response := &BlockHeadersPacket{
			RequestId:           req.RequestId,
			BlockHeadersRequest: BlockHeadersRequest{},
		}
		return peer.SendMessage(BlockHeadersMsg, response)
	}

	var req GetBlockHeadersRequest
	if err := msg.Decode(&req); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode get block headers request: %w", err)
	}

	// Send empty response for pre-eth66
	return peer.SendMessage(BlockHeadersMsg, BlockHeadersRequest{})
}

// handleBlockHeaders handles a block headers response
func (ph *ProtocolHandler) handleBlockHeaders(peer *Peer, msg p2p.Msg) error {
	if peer.Version() >= ETH66 {
		var packet BlockHeadersPacket
		if err := msg.Decode(&packet); err != nil {
			peer.decreaseScore(2)
			return fmt.Errorf("failed to decode block headers packet: %w", err)
		}

		ph.logger.Debug("received block headers",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("request_id", packet.RequestId),
			zap.Int("count", len(packet.BlockHeadersRequest)),
		)

		headers := []*types.Header(packet.BlockHeadersRequest)

		// Notify pending request
		peer.HandleResponse(packet.RequestId, headers)

		if ph.blockHandler != nil {
			return ph.blockHandler.HandleBlockHeaders(peer, packet.RequestId, headers)
		}

		return nil
	}

	var headers BlockHeadersRequest
	if err := msg.Decode(&headers); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode block headers: %w", err)
	}

	ph.logger.Debug("received block headers",
		zap.String("peer_id", peer.ID()),
		zap.Int("count", len(headers)),
	)

	if ph.blockHandler != nil {
		return ph.blockHandler.HandleBlockHeaders(peer, 0, []*types.Header(headers))
	}

	return nil
}

// handleGetBlockBodies handles a get block bodies request
func (ph *ProtocolHandler) handleGetBlockBodies(peer *Peer, msg p2p.Msg) error {
	if peer.Version() >= ETH66 {
		var req GetBlockBodiesPacket
		if err := msg.Decode(&req); err != nil {
			peer.decreaseScore(2)
			return fmt.Errorf("failed to decode get block bodies packet: %w", err)
		}

		ph.logger.Debug("received get block bodies request",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("request_id", req.RequestId),
			zap.Int("count", len(req.GetBlockBodiesRequest)),
		)

		// TODO: Implement response logic
		response := &BlockBodiesPacket{
			RequestId:           req.RequestId,
			BlockBodiesResponse: BlockBodiesResponse{},
		}
		return peer.SendMessage(BlockBodiesMsg, response)
	}

	var req GetBlockBodiesRequest
	if err := msg.Decode(&req); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode get block bodies request: %w", err)
	}

	return peer.SendMessage(BlockBodiesMsg, BlockBodiesResponse{})
}

// handleBlockBodies handles a block bodies response
func (ph *ProtocolHandler) handleBlockBodies(peer *Peer, msg p2p.Msg) error {
	if peer.Version() >= ETH66 {
		var packet BlockBodiesPacket
		if err := msg.Decode(&packet); err != nil {
			peer.decreaseScore(2)
			return fmt.Errorf("failed to decode block bodies packet: %w", err)
		}

		ph.logger.Debug("received block bodies",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("request_id", packet.RequestId),
			zap.Int("count", len(packet.BlockBodiesResponse)),
		)

		bodies := []*BlockBody(packet.BlockBodiesResponse)

		// Notify pending request
		peer.HandleResponse(packet.RequestId, bodies)

		if ph.blockHandler != nil {
			return ph.blockHandler.HandleBlockBodies(peer, packet.RequestId, bodies)
		}

		return nil
	}

	var bodies BlockBodiesResponse
	if err := msg.Decode(&bodies); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode block bodies: %w", err)
	}

	ph.logger.Debug("received block bodies",
		zap.String("peer_id", peer.ID()),
		zap.Int("count", len(bodies)),
	)

	if ph.blockHandler != nil {
		return ph.blockHandler.HandleBlockBodies(peer, 0, []*BlockBody(bodies))
	}

	return nil
}

// handleNewBlock handles a new block announcement
func (ph *ProtocolHandler) handleNewBlock(peer *Peer, msg p2p.Msg) error {
	var newBlock NewBlockPacket
	if err := msg.Decode(&newBlock); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode new block: %w", err)
	}

	ph.logger.Debug("received new block",
		zap.String("peer_id", peer.ID()),
		zap.Uint64("number", newBlock.Block.NumberU64()),
		zap.String("hash", newBlock.Block.Hash().Hex()),
	)

	// Update peer's head
	peer.UpdateHead(newBlock.Block.Hash(), nil)

	if ph.blockHandler != nil {
		return ph.blockHandler.HandleNewBlock(peer, newBlock.Block, newBlock.TD)
	}

	return nil
}

// handleGetReceipts handles a get receipts request
func (ph *ProtocolHandler) handleGetReceipts(peer *Peer, msg p2p.Msg) error {
	if peer.Version() >= ETH66 {
		var req GetReceiptsPacket
		if err := msg.Decode(&req); err != nil {
			peer.decreaseScore(2)
			return fmt.Errorf("failed to decode get receipts packet: %w", err)
		}

		ph.logger.Debug("received get receipts request",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("request_id", req.RequestId),
			zap.Int("count", len(req.GetReceiptsRequest)),
		)

		// TODO: Implement response logic
		response := &ReceiptsPacket{
			RequestId:        req.RequestId,
			ReceiptsResponse: ReceiptsResponse{},
		}
		return peer.SendMessage(ReceiptsMsg, response)
	}

	var req GetReceiptsRequest
	if err := msg.Decode(&req); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode get receipts request: %w", err)
	}

	return peer.SendMessage(ReceiptsMsg, ReceiptsResponse{})
}

// handleReceipts handles a receipts response
func (ph *ProtocolHandler) handleReceipts(peer *Peer, msg p2p.Msg) error {
	if peer.Version() >= ETH66 {
		var packet ReceiptsPacket
		if err := msg.Decode(&packet); err != nil {
			peer.decreaseScore(2)
			return fmt.Errorf("failed to decode receipts packet: %w", err)
		}

		ph.logger.Debug("received receipts",
			zap.String("peer_id", peer.ID()),
			zap.Uint64("request_id", packet.RequestId),
			zap.Int("count", len(packet.ReceiptsResponse)),
		)

		receipts := [][]*types.Receipt(packet.ReceiptsResponse)

		// Notify pending request
		peer.HandleResponse(packet.RequestId, receipts)

		if ph.receiptHandler != nil {
			return ph.receiptHandler.HandleReceipts(peer, packet.RequestId, receipts)
		}

		return nil
	}

	var receipts ReceiptsResponse
	if err := msg.Decode(&receipts); err != nil {
		peer.decreaseScore(2)
		return fmt.Errorf("failed to decode receipts: %w", err)
	}

	ph.logger.Debug("received receipts",
		zap.String("peer_id", peer.ID()),
		zap.Int("count", len(receipts)),
	)

	if ph.receiptHandler != nil {
		return ph.receiptHandler.HandleReceipts(peer, 0, [][]*types.Receipt(receipts))
	}

	return nil
}

// RunPeerMessageLoop runs the message handling loop for a peer
func (ph *ProtocolHandler) RunPeerMessageLoop(ctx context.Context, peer *Peer) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-peer.Done():
			return nil
		default:
			msg, err := peer.ReadMessage()
			if err != nil {
				ph.logger.Error("failed to read message",
					zap.String("peer_id", peer.ID()),
					zap.Error(err),
				)
				return err
			}

			if err := ph.HandleMessage(peer, msg); err != nil {
				ph.logger.Error("failed to handle message",
					zap.String("peer_id", peer.ID()),
					zap.Uint64("msg_code", msg.Code),
					zap.Error(err),
				)
				msg.Discard()
				continue
			}

			msg.Discard()
		}
	}
}
