package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sunvim/evm_syncer/pkg/p2p"
	"github.com/sunvim/evm_syncer/pkg/worker"
	"go.uber.org/zap"
)

const (
	// Download batch size
	HeaderBatchSize = 192
	BodyBatchSize   = 128
	ReceiptBatchSize = 128

	// Worker pool configuration
	DefaultWorkerCount = 50
	DefaultQueueSize   = 1000

	// Retry configuration
	MaxRetries     = 3
	RetryDelay     = 2 * time.Second
)

var (
	ErrNoValidPeers  = errors.New("no valid peers available")
	ErrDownloadTimeout = errors.New("download timeout")
)

// BlockData holds downloaded block data
type BlockData struct {
	Number   uint64
	Header   *types.Header
	Body     *p2p.BlockBody
	Receipts types.Receipts
}

// Downloader manages concurrent block downloads from P2P peers
type Downloader struct {
	peerManager *p2p.PeerManager
	workerPool  *worker.Pool
	logger      *zap.Logger

	// Download queues
	headerQueue  chan *headerTask
	bodyQueue    chan *bodyTask
	receiptQueue chan *receiptTask

	// Statistics
	downloadedHeaders  atomic.Uint64
	downloadedBodies   atomic.Uint64
	downloadedReceipts atomic.Uint64
	failedDownloads    atomic.Uint64

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDownloader creates a new block downloader
func NewDownloader(peerManager *p2p.PeerManager, logger *zap.Logger) (*Downloader, error) {
	pool, err := worker.NewPool(worker.PoolConfig{
		Name:            "downloader",
		WorkerCount:     DefaultWorkerCount,
		QueueSize:       DefaultQueueSize,
		ShutdownTimeout: 30 * time.Second,
		Logger:          logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create worker pool: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Downloader{
		peerManager:  peerManager,
		workerPool:   pool,
		logger:       logger.With(zap.String("component", "downloader")),
		headerQueue:  make(chan *headerTask, 100),
		bodyQueue:    make(chan *bodyTask, 100),
		receiptQueue: make(chan *receiptTask, 100),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Start starts the downloader
func (d *Downloader) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.logger.Info("starting downloader")
	d.workerPool.Start(ctx)
}

// Stop stops the downloader
func (d *Downloader) Stop(ctx context.Context) error {
	d.logger.Info("stopping downloader")
	d.cancel()

	close(d.headerQueue)
	close(d.bodyQueue)
	close(d.receiptQueue)

	if err := d.workerPool.Shutdown(ctx); err != nil {
		d.logger.Error("error shutting down worker pool", zap.Error(err))
		return err
	}

	d.wg.Wait()
	d.logger.Info("downloader stopped")
	return nil
}

// DownloadHeaders downloads block headers in batches
func (d *Downloader) DownloadHeaders(ctx context.Context, fromBlock, toBlock uint64) ([]*types.Header, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range: %d > %d", fromBlock, toBlock)
	}

	headers := make([]*types.Header, 0, toBlock-fromBlock+1)
	var mu sync.Mutex
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Download in batches
	for current := fromBlock; current <= toBlock; {
		batchSize := uint64(HeaderBatchSize)
		if current+batchSize-1 > toBlock {
			batchSize = toBlock - current + 1
		}

		wg.Add(1)
		task := &headerTask{
			fromBlock: current,
			toBlock:   current + batchSize - 1,
			resultCh:  make(chan []*types.Header, 1),
			errCh:     errCh,
		}

		go func(t *headerTask) {
			defer wg.Done()
			result, err := d.downloadHeaderBatch(ctx, t.fromBlock, t.toBlock)
			if err != nil {
				select {
				case t.errCh <- err:
				default:
				}
				return
			}

			mu.Lock()
			headers = append(headers, result...)
			mu.Unlock()

			d.downloadedHeaders.Add(uint64(len(result)))
		}(task)

		current += batchSize
	}

	// Wait for all batches
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case <-done:
		d.logger.Info("downloaded headers",
			zap.Uint64("from", fromBlock),
			zap.Uint64("to", toBlock),
			zap.Int("count", len(headers)))
		return headers, nil
	}
}

// DownloadBodies downloads block bodies for the given headers
func (d *Downloader) DownloadBodies(ctx context.Context, headers []*types.Header) ([]*p2p.BlockBody, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	bodies := make([]*p2p.BlockBody, len(headers))
	var mu sync.Mutex
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Download in batches
	for i := 0; i < len(headers); i += BodyBatchSize {
		end := i + BodyBatchSize
		if end > len(headers) {
			end = len(headers)
		}

		batch := headers[i:end]
		wg.Add(1)

		go func(idx int, hdrs []*types.Header) {
			defer wg.Done()

			result, err := d.downloadBodyBatch(ctx, hdrs)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			mu.Lock()
			copy(bodies[idx:], result)
			mu.Unlock()

			d.downloadedBodies.Add(uint64(len(result)))
		}(i, batch)
	}

	// Wait for all batches
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case <-done:
		d.logger.Info("downloaded bodies", zap.Int("count", len(bodies)))
		return bodies, nil
	}
}

// DownloadReceipts downloads receipts for the given headers
func (d *Downloader) DownloadReceipts(ctx context.Context, headers []*types.Header) ([]types.Receipts, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	receipts := make([]types.Receipts, len(headers))
	var mu sync.Mutex
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Download in batches
	for i := 0; i < len(headers); i += ReceiptBatchSize {
		end := i + ReceiptBatchSize
		if end > len(headers) {
			end = len(headers)
		}

		batch := headers[i:end]
		wg.Add(1)

		go func(idx int, hdrs []*types.Header) {
			defer wg.Done()

			result, err := d.downloadReceiptBatch(ctx, hdrs)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			mu.Lock()
			copy(receipts[idx:], result)
			mu.Unlock()

			d.downloadedReceipts.Add(uint64(len(result)))
		}(i, batch)
	}

	// Wait for all batches
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case <-done:
		d.logger.Info("downloaded receipts", zap.Int("count", len(receipts)))
		return receipts, nil
	}
}

// downloadHeaderBatch downloads a batch of headers from a peer
func (d *Downloader) downloadHeaderBatch(ctx context.Context, fromBlock, toBlock uint64) ([]*types.Header, error) {
	var lastErr error
	
	for retry := 0; retry < MaxRetries; retry++ {
		peer := d.selectBestPeer()
		if peer == nil {
			return nil, ErrNoValidPeers
		}

		amount := toBlock - fromBlock + 1
		headersResp, err := peer.RequestBlockHeaders(ctx, p2p.HashOrNumber{Number: fromBlock}, amount, 0, false)
		if err != nil {
			lastErr = err
			d.logger.Warn("failed to download headers from peer",
				zap.String("peer", peer.ID()),
				zap.Uint64("from", fromBlock),
				zap.Uint64("to", toBlock),
				zap.Int("retry", retry+1),
				zap.Error(err))

			if retry < MaxRetries-1 {
				time.Sleep(RetryDelay)
				continue
			}
			continue
		}

		if len(headersResp) == 0 {
			return nil, fmt.Errorf("peer returned empty headers")
		}

		// headersResp is []*BlockHeadersRequest where BlockHeadersRequest = []*types.Header
		// So we have [][]*types.Header and need to flatten to []*types.Header
		var headers []*types.Header
		for _, headerBatch := range headersResp {
			if headerBatch != nil {
				headers = append(headers, []*types.Header(*headerBatch)...)
			}
		}

		if len(headers) == 0 {
			return nil, fmt.Errorf("peer returned empty headers after flattening")
		}

		return headers, nil
	}

	d.failedDownloads.Add(1)
	return nil, fmt.Errorf("failed to download headers after %d retries: %w", MaxRetries, lastErr)
}

// downloadBodyBatch downloads a batch of block bodies from a peer
func (d *Downloader) downloadBodyBatch(ctx context.Context, headers []*types.Header) ([]*p2p.BlockBody, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	hashes := make([]common.Hash, len(headers))
	for i, header := range headers {
		hashes[i] = header.Hash()
	}

	var lastErr error
	for retry := 0; retry < MaxRetries; retry++ {
		peer := d.selectBestPeer()
		if peer == nil {
			return nil, ErrNoValidPeers
		}

		bodies, err := peer.RequestBlockBodies(ctx, hashes)
		if err != nil {
			lastErr = err
			d.logger.Warn("failed to download bodies from peer",
				zap.String("peer", peer.ID()),
				zap.Int("count", len(hashes)),
				zap.Int("retry", retry+1),
				zap.Error(err))

			if retry < MaxRetries-1 {
				time.Sleep(RetryDelay)
				continue
			}
			continue
		}

		if len(bodies) == 0 {
			return nil, fmt.Errorf("peer returned empty bodies")
		}

		return bodies, nil
	}

	d.failedDownloads.Add(1)
	return nil, fmt.Errorf("failed to download bodies after %d retries: %w", MaxRetries, lastErr)
}

// downloadReceiptBatch downloads a batch of receipts from a peer
func (d *Downloader) downloadReceiptBatch(ctx context.Context, headers []*types.Header) ([]types.Receipts, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	hashes := make([]common.Hash, len(headers))
	for i, header := range headers {
		hashes[i] = header.Hash()
	}

	var lastErr error
	for retry := 0; retry < MaxRetries; retry++ {
		peer := d.selectBestPeer()
		if peer == nil {
			return nil, ErrNoValidPeers
		}

		receiptsResp, err := peer.RequestReceipts(ctx, hashes)
		if err != nil {
			lastErr = err
			d.logger.Warn("failed to download receipts from peer",
				zap.String("peer", peer.ID()),
				zap.Int("count", len(hashes)),
				zap.Int("retry", retry+1),
				zap.Error(err))

			if retry < MaxRetries-1 {
				time.Sleep(RetryDelay)
				continue
			}
			continue
		}

		if len(receiptsResp) == 0 {
			return nil, fmt.Errorf("peer returned empty receipts")
		}

		// Since ReceiptsResponse is [][]*types.Receipt, receiptsResp is [][][]*types.Receipt
		// We need to convert to []types.Receipts (which is [][]*types.Receipt)
		receipts := make([]types.Receipts, len(receiptsResp))
		for i, blockReceiptBatches := range receiptsResp {
			// blockReceiptBatches is [][]*types.Receipt (one ReceiptsResponse)
			// We expect it to have one batch per block, so take the first if available
			if len(blockReceiptBatches) > 0 && blockReceiptBatches[0] != nil {
				// Dereference the ReceiptsResponse pointer to get [][]*types.Receipt
				// and take the first element to get []*types.Receipt
				receiptBatch := *blockReceiptBatches[0]
				if len(receiptBatch) > 0 {
					receipts[i] = receiptBatch[0]
				} else {
					receipts[i] = types.Receipts{}
				}
			} else {
				receipts[i] = types.Receipts{}
			}
		}

		return receipts, nil
	}

	d.failedDownloads.Add(1)
	return nil, fmt.Errorf("failed to download receipts after %d retries: %w", MaxRetries, lastErr)
}

// selectBestPeer selects the best peer for downloading
func (d *Downloader) selectBestPeer() *p2p.Peer {
	peers := d.peerManager.GetAllPeers()
	if len(peers) == 0 {
		return nil
	}

	// Simple selection: pick the peer with the highest score
	var bestPeer *p2p.Peer
	bestScore := -1

	for _, peer := range peers {
		if !peer.IsConnected() {
			continue
		}

		score := peer.Score()
		if score > bestScore {
			bestScore = score
			bestPeer = peer
		}
	}

	return bestPeer
}

// GetStats returns downloader statistics
func (d *Downloader) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"downloaded_headers":  d.downloadedHeaders.Load(),
		"downloaded_bodies":   d.downloadedBodies.Load(),
		"downloaded_receipts": d.downloadedReceipts.Load(),
		"failed_downloads":    d.failedDownloads.Load(),
		"worker_pool":         d.workerPool.Stats(),
	}
}

// Task types for internal use

type headerTask struct {
	fromBlock uint64
	toBlock   uint64
	resultCh  chan []*types.Header
	errCh     chan error
}

type bodyTask struct {
	headers  []*types.Header
	resultCh chan []*p2p.BlockBody
	errCh    chan error
}

type receiptTask struct {
	headers  []*types.Header
	resultCh chan []types.Receipts
	errCh    chan error
}
