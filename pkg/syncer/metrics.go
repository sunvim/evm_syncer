package syncer

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Prometheus metrics for syncer
	syncCurrentBlockGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "syncer_current_block",
			Help: "Current block number being synced",
		},
	)

	syncTargetBlockGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "syncer_target_block",
			Help: "Target block number for sync",
		},
	)

	syncProgressGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "syncer_progress_percent",
			Help: "Sync progress percentage",
		},
	)

	syncBlocksPerSecondGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "syncer_blocks_per_second",
			Help: "Sync speed in blocks per second",
		},
	)

	downloadedHeadersCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_downloaded_headers_total",
			Help: "Total number of headers downloaded",
		},
	)

	downloadedBodiesCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_downloaded_bodies_total",
			Help: "Total number of block bodies downloaded",
		},
	)

	downloadedReceiptsCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_downloaded_receipts_total",
			Help: "Total number of receipts downloaded",
		},
	)

	validatedBlocksCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_validated_blocks_total",
			Help: "Total number of blocks validated",
		},
	)

	validationErrorsCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_validation_errors_total",
			Help: "Total number of validation errors",
		},
	)

	writtenBlocksCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_written_blocks_total",
			Help: "Total number of blocks written to storage",
		},
	)

	reorgsCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_reorgs_total",
			Help: "Total number of blockchain reorganizations detected",
		},
	)

	syncErrorsCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "syncer_errors_total",
			Help: "Total number of sync errors",
		},
	)

	downloadDurationHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "syncer_download_duration_seconds",
			Help:    "Time taken to download a batch of blocks",
			Buckets: []float64{.1, .5, 1, 2, 5, 10, 30, 60},
		},
	)

	validationDurationHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "syncer_validation_duration_seconds",
			Help:    "Time taken to validate a batch of blocks",
			Buckets: []float64{.01, .05, .1, .5, 1, 2, 5},
		},
	)

	writeDurationHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "syncer_write_duration_seconds",
			Help:    "Time taken to write a batch of blocks",
			Buckets: []float64{.01, .05, .1, .5, 1, 2, 5},
		},
	)

	metricsOnce sync.Once
)

// SyncMetrics tracks synchronization metrics
type SyncMetrics struct {
	downloadedHeaders  atomic.Uint64
	downloadedBodies   atomic.Uint64
	downloadedReceipts atomic.Uint64
	validatedBlocks    atomic.Uint64
	validationErrors   atomic.Uint64
	writtenBlocks      atomic.Uint64
	reorgs             atomic.Uint64
	errors             atomic.Uint64
}

// NewSyncMetrics creates a new sync metrics instance
func NewSyncMetrics() *SyncMetrics {
	metricsOnce.Do(func() {
		// Metrics are already initialized via promauto
	})

	return &SyncMetrics{}
}

// UpdateProgress updates current and target block metrics
func (m *SyncMetrics) UpdateProgress(current, target uint64) {
	syncCurrentBlockGauge.Set(float64(current))
	syncTargetBlockGauge.Set(float64(target))

	if target > 0 && current <= target {
		progress := float64(current) / float64(target) * 100
		syncProgressGauge.Set(progress)
	}
}

// UpdateBlocksPerSecond updates the sync speed metric
func (m *SyncMetrics) UpdateBlocksPerSecond(bps float64) {
	syncBlocksPerSecondGauge.Set(bps)
}

// IncrementDownloadedHeaders increments the downloaded headers counter
func (m *SyncMetrics) IncrementDownloadedHeaders(count uint64) {
	m.downloadedHeaders.Add(count)
	downloadedHeadersCounter.Add(float64(count))
}

// IncrementDownloadedBodies increments the downloaded bodies counter
func (m *SyncMetrics) IncrementDownloadedBodies(count uint64) {
	m.downloadedBodies.Add(count)
	downloadedBodiesCounter.Add(float64(count))
}

// IncrementDownloadedReceipts increments the downloaded receipts counter
func (m *SyncMetrics) IncrementDownloadedReceipts(count uint64) {
	m.downloadedReceipts.Add(count)
	downloadedReceiptsCounter.Add(float64(count))
}

// IncrementValidatedBlocks increments the validated blocks counter
func (m *SyncMetrics) IncrementValidatedBlocks(count uint64) {
	m.validatedBlocks.Add(count)
	validatedBlocksCounter.Add(float64(count))
}

// IncrementValidationErrors increments the validation errors counter
func (m *SyncMetrics) IncrementValidationErrors() {
	m.validationErrors.Add(1)
	validationErrorsCounter.Inc()
}

// IncrementWrittenBlocks increments the written blocks counter
func (m *SyncMetrics) IncrementWrittenBlocks(count uint64) {
	m.writtenBlocks.Add(count)
	writtenBlocksCounter.Add(float64(count))
}

// IncrementReorgs increments the reorgs counter
func (m *SyncMetrics) IncrementReorgs() {
	m.reorgs.Add(1)
	reorgsCounter.Inc()
}

// IncrementErrors increments the errors counter
func (m *SyncMetrics) IncrementErrors() {
	m.errors.Add(1)
	syncErrorsCounter.Inc()
}

// ObserveDownloadDuration records download duration
func (m *SyncMetrics) ObserveDownloadDuration(seconds float64) {
	downloadDurationHistogram.Observe(seconds)
}

// ObserveValidationDuration records validation duration
func (m *SyncMetrics) ObserveValidationDuration(seconds float64) {
	validationDurationHistogram.Observe(seconds)
}

// ObserveWriteDuration records write duration
func (m *SyncMetrics) ObserveWriteDuration(seconds float64) {
	writeDurationHistogram.Observe(seconds)
}

// GetStats returns a snapshot of all metrics
func (m *SyncMetrics) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"downloaded_headers":  m.downloadedHeaders.Load(),
		"downloaded_bodies":   m.downloadedBodies.Load(),
		"downloaded_receipts": m.downloadedReceipts.Load(),
		"validated_blocks":    m.validatedBlocks.Load(),
		"validation_errors":   m.validationErrors.Load(),
		"written_blocks":      m.writtenBlocks.Load(),
		"reorgs":              m.reorgs.Load(),
		"errors":              m.errors.Load(),
	}
}

// Reset resets all metrics counters
func (m *SyncMetrics) Reset() {
	m.downloadedHeaders.Store(0)
	m.downloadedBodies.Store(0)
	m.downloadedReceipts.Store(0)
	m.validatedBlocks.Store(0)
	m.validationErrors.Store(0)
	m.writtenBlocks.Store(0)
	m.reorgs.Store(0)
	m.errors.Store(0)
}
