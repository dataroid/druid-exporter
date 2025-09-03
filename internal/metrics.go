package internal

import (
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// InternalMetrics holds all internal monitoring metrics for the druid-exporter
type InternalMetrics struct {
	// Request monitoring
	RequestsTotal    *prometheus.CounterVec
	RequestDuration  *prometheus.HistogramVec
	RequestSizeBytes *prometheus.HistogramVec
	MetricsProcessed *prometheus.CounterVec
	ErrorsTotal      *prometheus.CounterVec

	// System monitoring
	MemoryUsageBytes *prometheus.GaugeVec
	HttpClientStats  *prometheus.GaugeVec

	// Dynamic metrics monitoring
	DynamicMetricsTotal  *prometheus.CounterVec
	DynamicMetricsActive *prometheus.GaugeVec
	DynamicMetricsErrors *prometheus.CounterVec

	// Cleaner monitoring
	CleanerDuration       *prometheus.HistogramVec
	CleanerErrors         *prometheus.CounterVec
	CleanerMetricsTracked *prometheus.GaugeVec
	CleanerMetricsDeleted *prometheus.CounterVec
	CleanerRuns           *prometheus.CounterVec

	// Legacy cleanup metrics (for backwards compatibility)
	CleanupMetrics *prometheus.CounterVec

	// Control flags
	enabled bool
}

// NewInternalMetrics creates and initializes all internal monitoring metrics
func NewInternalMetrics(enabled bool) *InternalMetrics {
	if !enabled {
		logrus.Info("Internal metrics disabled")
		return &InternalMetrics{enabled: false}
	}

	logrus.Info("Initializing internal metrics")

	m := &InternalMetrics{
		enabled: true,
		// Request monitoring metrics
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_requests_total",
				Help: "Total number of requests processed by the exporter",
			}, []string{"method", "status", "source_ip"},
		),

		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "druid_exporter_request_duration_seconds",
				Help:    "Duration of request processing",
				Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0},
			}, []string{"method", "status", "source_ip"},
		),

		RequestSizeBytes: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "druid_exporter_request_size_bytes",
				Help:    "Size of incoming requests in bytes",
				Buckets: []float64{1024, 10240, 102400, 1048576, 10485760, 104857600}, // 1KB to 100MB
			}, []string{"source_ip"},
		),

		MetricsProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_metrics_processed_total",
				Help: "Total number of metrics processed by the exporter",
			}, []string{"source_ip"},
		),

		ErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_errors_total",
				Help: "Total number of errors encountered by the exporter",
			}, []string{"type", "source_ip"},
		),

		// System monitoring metrics
		MemoryUsageBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_exporter_memory_usage_bytes",
				Help: "Memory usage statistics for the exporter",
			}, []string{"type"},
		),

		CleanupMetrics: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_cleanup_metrics_total",
				Help: "Total number of metrics cleaned up",
			}, []string{"type"},
		),

		HttpClientStats: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_exporter_http_client_stats",
				Help: "HTTP client connection statistics",
			}, []string{"stat"},
		),

		// Dynamic metrics monitoring
		DynamicMetricsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_dynamic_metrics_created_total",
				Help: "Total number of dynamic metrics created",
			}, []string{"metric_type"},
		),

		DynamicMetricsActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_exporter_dynamic_metrics_active",
				Help: "Number of currently active dynamic metrics",
			}, []string{"metric_type"},
		),

		DynamicMetricsErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_dynamic_metrics_errors_total",
				Help: "Total number of dynamic metric creation/registration errors",
			}, []string{"error_type"},
		),

		// Cleaner monitoring metrics
		CleanerDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "druid_exporter_cleaner_duration_seconds",
				Help:    "Duration of cleanup operations per cleaner",
				Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0},
			}, []string{"cleaner_id"},
		),

		CleanerErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_cleaner_errors_total",
				Help: "Total number of cleanup errors per cleaner",
			}, []string{"cleaner_id", "error_type"},
		),

		CleanerMetricsTracked: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_exporter_cleaner_metrics_tracked",
				Help: "Current number of metrics being tracked per cleaner",
			}, []string{"cleaner_id"},
		),

		CleanerMetricsDeleted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_cleaner_metrics_deleted_total",
				Help: "Total number of metrics deleted per cleaner",
			}, []string{"cleaner_id"},
		),

		CleanerRuns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_exporter_cleaner_runs_total",
				Help: "Total number of cleanup runs per cleaner",
			}, []string{"cleaner_id"},
		),
	}

	// Register all metrics with Prometheus
	m.register()

	return m
}

// register registers all internal metrics with the Prometheus default registry
func (m *InternalMetrics) register() {
	if !m.enabled {
		return
	}

	// Register request monitoring metrics
	prometheus.MustRegister(m.RequestsTotal)
	prometheus.MustRegister(m.RequestDuration)
	prometheus.MustRegister(m.RequestSizeBytes)
	prometheus.MustRegister(m.MetricsProcessed)
	prometheus.MustRegister(m.ErrorsTotal)

	// Register system monitoring metrics
	prometheus.MustRegister(m.MemoryUsageBytes)
	prometheus.MustRegister(m.CleanupMetrics)
	prometheus.MustRegister(m.HttpClientStats)

	// Register dynamic metrics monitoring
	prometheus.MustRegister(m.DynamicMetricsTotal)
	prometheus.MustRegister(m.DynamicMetricsActive)
	prometheus.MustRegister(m.DynamicMetricsErrors)

	// Register cleaner monitoring metrics
	prometheus.MustRegister(m.CleanerDuration)
	prometheus.MustRegister(m.CleanerErrors)
	prometheus.MustRegister(m.CleanerMetricsTracked)
	prometheus.MustRegister(m.CleanerMetricsDeleted)
	prometheus.MustRegister(m.CleanerRuns)

	logrus.Info("All internal metrics registered successfully")
}

// IsEnabled returns whether internal metrics are enabled
func (m *InternalMetrics) IsEnabled() bool {
	return m.enabled
}

// StartMemoryMonitoring starts a goroutine to periodically collect memory statistics
func (m *InternalMetrics) StartMemoryMonitoring() {
	if !m.enabled || m.MemoryUsageBytes == nil {
		return
	}

	logrus.Info("Starting memory monitoring goroutine")

	ticker := time.NewTicker(30 * time.Second)
	go func() {
		defer ticker.Stop()

		for range ticker.C {
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			// Update memory usage metrics
			m.MemoryUsageBytes.WithLabelValues("heap_alloc").Set(float64(memStats.HeapAlloc))
			m.MemoryUsageBytes.WithLabelValues("heap_sys").Set(float64(memStats.HeapSys))
			m.MemoryUsageBytes.WithLabelValues("heap_idle").Set(float64(memStats.HeapIdle))
			m.MemoryUsageBytes.WithLabelValues("heap_inuse").Set(float64(memStats.HeapInuse))
			m.MemoryUsageBytes.WithLabelValues("stack_inuse").Set(float64(memStats.StackInuse))
			m.MemoryUsageBytes.WithLabelValues("stack_sys").Set(float64(memStats.StackSys))
			m.MemoryUsageBytes.WithLabelValues("next_gc").Set(float64(memStats.NextGC))
		}
	}()
}

// GetRequestMetrics returns the request monitoring metrics for use in HTTP handlers
func (m *InternalMetrics) GetRequestMetrics() (
	*prometheus.CounterVec,
	*prometheus.HistogramVec,
	*prometheus.HistogramVec,
	*prometheus.CounterVec,
	*prometheus.CounterVec,
) {
	if !m.enabled {
		return nil, nil, nil, nil, nil
	}
	return m.RequestsTotal, m.RequestDuration, m.RequestSizeBytes, m.MetricsProcessed, m.ErrorsTotal
}

// GetDynamicMetrics returns the dynamic metrics monitoring for use in dynamic metric creation
func (m *InternalMetrics) GetDynamicMetrics() (
	*prometheus.CounterVec,
	*prometheus.GaugeVec,
	*prometheus.CounterVec,
) {
	if !m.enabled {
		return nil, nil, nil
	}
	return m.DynamicMetricsTotal, m.DynamicMetricsActive, m.DynamicMetricsErrors
}

// GetCleanerMetrics returns the cleaner monitoring metrics for use in cleaner creation
func (m *InternalMetrics) GetCleanerMetrics() (
	*prometheus.HistogramVec,
	*prometheus.CounterVec,
	*prometheus.GaugeVec,
	*prometheus.CounterVec,
	*prometheus.CounterVec,
) {
	if !m.enabled {
		return nil, nil, nil, nil, nil
	}
	return m.CleanerDuration, m.CleanerErrors, m.CleanerMetricsTracked, m.CleanerMetricsDeleted, m.CleanerRuns
}
