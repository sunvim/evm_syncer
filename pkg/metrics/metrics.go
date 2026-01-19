package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	// Sync metrics
	SyncHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sync_height",
		Help: "Current synchronized block height",
	})

	SyncLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sync_lag",
		Help: "Number of blocks behind the network",
	})

	SyncSpeedBPS = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sync_speed_bps",
		Help: "Synchronization speed in blocks per second",
	})

	PeerCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "peer_count",
		Help: "Number of connected peers",
	})

	ValidationFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "validation_failures_total",
		Help: "Total number of block validation failures",
	})

	ReorgCountTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reorg_count_total",
		Help: "Total number of chain reorganizations detected",
	})

	// Transaction pool metrics
	TxPoolPendingCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "txpool_pending_count",
		Help: "Number of pending transactions in the pool",
	})

	TxBroadcastQueueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tx_broadcast_queue_length",
		Help: "Length of the transaction broadcast queue",
	})

	TxsBroadcastedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "txs_broadcasted_total",
		Help: "Total number of transactions broadcasted",
	})

	TxsReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "txs_received_total",
		Help: "Total number of transactions received from peers",
	})
)

// RegisterMetrics registers all metrics with Prometheus
func RegisterMetrics() {
	prometheus.MustRegister(
		SyncHeight,
		SyncLag,
		SyncSpeedBPS,
		PeerCount,
		ValidationFailuresTotal,
		ReorgCountTotal,
		TxPoolPendingCount,
		TxBroadcastQueueLength,
		TxsBroadcastedTotal,
		TxsReceivedTotal,
	)
}

// StartMetricsServer starts the Prometheus metrics HTTP server
func StartMetricsServer(addr string, logger *zap.Logger) error {
	RegisterMetrics()

	http.Handle("/metrics", promhttp.Handler())
	
	// Health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	logger.Info("starting metrics server", zap.String("addr", addr))
	return http.ListenAndServe(addr, nil)
}
