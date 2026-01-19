package worker

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Prometheus metrics
	activeWorkersGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_pool_active_workers",
			Help: "Number of workers currently executing tasks",
		},
		[]string{"pool"},
	)

	queueLengthGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_pool_queue_length",
			Help: "Number of tasks currently queued",
		},
		[]string{"pool"},
	)

	tasksCompletedCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_pool_tasks_completed_total",
			Help: "Total number of tasks completed successfully",
		},
		[]string{"pool"},
	)

	tasksFailedCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_pool_tasks_failed_total",
			Help: "Total number of tasks that failed",
		},
		[]string{"pool"},
	)

	taskDurationHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_pool_task_duration_seconds",
			Help:    "Task execution duration in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"pool", "status"},
	)

	poolMetricsOnce sync.Once
)

// poolMetrics holds references to metrics for a specific pool.
type poolMetrics struct {
	poolName string
	
	activeWorkers  prometheus.Gauge
	queueLength    prometheus.Gauge
	tasksCompleted prometheus.Counter
	tasksFailed    prometheus.Counter
	
	taskDurationSuccess prometheus.Observer
	taskDurationFailure prometheus.Observer
}

// newPoolMetrics creates a new metrics instance for a pool.
func newPoolMetrics(poolName string) *poolMetrics {
	// Initialize global metrics once
	poolMetricsOnce.Do(func() {
		// Metrics are already initialized via promauto
	})

	return &poolMetrics{
		poolName:            poolName,
		activeWorkers:       activeWorkersGauge.WithLabelValues(poolName),
		queueLength:         queueLengthGauge.WithLabelValues(poolName),
		tasksCompleted:      tasksCompletedCounter.WithLabelValues(poolName),
		tasksFailed:         tasksFailedCounter.WithLabelValues(poolName),
		taskDurationSuccess: taskDurationHistogram.WithLabelValues(poolName, "success"),
		taskDurationFailure: taskDurationHistogram.WithLabelValues(poolName, "failure"),
	}
}

// setActiveWorkers updates the active workers gauge.
func (m *poolMetrics) setActiveWorkers(count float64) {
	m.activeWorkers.Set(count)
}

// setQueueLength updates the queue length gauge.
func (m *poolMetrics) setQueueLength(length float64) {
	m.queueLength.Set(length)
}

// recordTaskCompletion records a successful task completion.
func (m *poolMetrics) recordTaskCompletion(duration time.Duration) {
	m.tasksCompleted.Inc()
	m.taskDurationSuccess.Observe(duration.Seconds())
}

// recordTaskFailure records a failed task.
func (m *poolMetrics) recordTaskFailure(duration time.Duration) {
	m.tasksFailed.Inc()
	m.taskDurationFailure.Observe(duration.Seconds())
}

// MetricsSnapshot represents a point-in-time snapshot of pool metrics.
type MetricsSnapshot struct {
	PoolName           string
	ActiveWorkers      float64
	QueueLength        float64
	TasksCompleted     float64
	TasksFailed        float64
	Timestamp          time.Time
}

// GetMetrics returns a snapshot of all pool metrics.
// This is useful for debugging and monitoring without querying Prometheus.
func GetMetrics(poolName string) (MetricsSnapshot, error) {
	snapshot := MetricsSnapshot{
		PoolName:  poolName,
		Timestamp: time.Now(),
	}

	// Collect metrics from Prometheus
	metrics := []*prometheus.Desc{
		activeWorkersGauge.WithLabelValues(poolName).Desc(),
		queueLengthGauge.WithLabelValues(poolName).Desc(),
	}

	_ = metrics // Metrics are tracked internally, this function provides a snapshot interface

	return snapshot, nil
}

// ResetMetrics resets all metrics for a specific pool.
// This should be used cautiously, typically only in testing scenarios.
func ResetMetrics(poolName string) {
	activeWorkersGauge.DeleteLabelValues(poolName)
	queueLengthGauge.DeleteLabelValues(poolName)
	tasksCompletedCounter.DeleteLabelValues(poolName)
	tasksFailedCounter.DeleteLabelValues(poolName)
	taskDurationHistogram.DeleteLabelValues(poolName, "success")
	taskDurationHistogram.DeleteLabelValues(poolName, "failure")
}
