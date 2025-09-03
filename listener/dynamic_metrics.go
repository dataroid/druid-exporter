package listener

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// DynamicMetricsManager encapsulates all dynamic metric creation and management
type DynamicMetricsManager struct {
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	cleaners   map[string]*cleaner
	mutex      sync.RWMutex

	// Monitoring metrics
	metricsTotal  *prometheus.CounterVec
	metricsActive *prometheus.GaugeVec
	metricsErrors *prometheus.CounterVec

	// Cleaner metrics
	cleanerDuration       *prometheus.HistogramVec
	cleanerErrors         *prometheus.CounterVec
	cleanerMetricsTracked *prometheus.GaugeVec
	cleanerMetricsDeleted *prometheus.CounterVec
	cleanerRuns           *prometheus.CounterVec

	// Configuration
	cleanupTTL int
}

// MetricResult holds the result of dynamic metric creation
type MetricResult struct {
	Gauge     *prometheus.GaugeVec
	Histogram *prometheus.HistogramVec
	Cleaner   *cleaner
}

// NewDynamicMetricsManager creates a new dynamic metrics manager
func NewDynamicMetricsManager(cleanupTTL int, metricsTotal *prometheus.CounterVec, metricsActive *prometheus.GaugeVec, metricsErrors *prometheus.CounterVec, cleanerDuration *prometheus.HistogramVec, cleanerErrors *prometheus.CounterVec, cleanerMetricsTracked *prometheus.GaugeVec, cleanerMetricsDeleted *prometheus.CounterVec, cleanerRuns *prometheus.CounterVec) *DynamicMetricsManager {
	return &DynamicMetricsManager{
		gauges:                make(map[string]*prometheus.GaugeVec),
		histograms:            make(map[string]*prometheus.HistogramVec),
		cleaners:              make(map[string]*cleaner),
		cleanupTTL:            cleanupTTL,
		metricsTotal:          metricsTotal,
		metricsActive:         metricsActive,
		metricsErrors:         metricsErrors,
		cleanerDuration:       cleanerDuration,
		cleanerErrors:         cleanerErrors,
		cleanerMetricsTracked: cleanerMetricsTracked,
		cleanerMetricsDeleted: cleanerMetricsDeleted,
		cleanerRuns:           cleanerRuns,
	}
}

// sanitizeMetricName converts druid metric names to valid Prometheus metric names
func (dmm *DynamicMetricsManager) sanitizeMetricName(metricName string) string {
	// Replace invalid characters with underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := reg.ReplaceAllString(metricName, "_")

	// Ensure the name doesn't start with a number
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "_" + sanitized
	}

	// Add druid prefix and convert to lowercase
	return "druid_" + strings.ToLower(sanitized)
}

// getOrCreateGauge creates or retrieves an existing gauge for the given metric name
func (dmm *DynamicMetricsManager) getOrCreateGauge(metricName string) (*prometheus.GaugeVec, *cleaner) {
	sanitizedName := dmm.sanitizeMetricName(metricName)

	// Try to get existing gauge (read lock)
	dmm.mutex.RLock()
	if gauge, exists := dmm.gauges[sanitizedName]; exists {
		cleaner := dmm.cleaners[sanitizedName]
		dmm.mutex.RUnlock()
		return gauge, cleaner
	}
	dmm.mutex.RUnlock()

	// Create new gauge (write lock)
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	// Double-check after acquiring write lock
	if gauge, exists := dmm.gauges[sanitizedName]; exists {
		cleaner := dmm.cleaners[sanitizedName]
		return gauge, cleaner
	}

	// Create the gauge
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: sanitizedName,
			Help: fmt.Sprintf("Druid gauge metric: %s", metricName),
		}, []string{"dns_name", "service", "host", "datasource", "id"},
	)

	// Register the new gauge
	if err := prometheus.Register(gauge); err != nil {
		dmm.trackError("gauge_registration_error")
		logrus.Errorf("Failed to register gauge %s: %v", sanitizedName, err)
		return nil, nil
	}

	// Track dynamic metric creation
	dmm.trackCreation("gauge")

	// Create and start cleaner for this gauge
	cleaner := newCleaner(gauge, dmm.cleanupTTL, sanitizedName, dmm.cleanerDuration, dmm.cleanerErrors, dmm.cleanerMetricsTracked, dmm.cleanerMetricsDeleted, dmm.cleanerRuns)

	// Store in registry
	dmm.gauges[sanitizedName] = gauge
	dmm.cleaners[sanitizedName] = cleaner

	logrus.Debugf("Created new dynamic gauge: %s (from %s)", sanitizedName, metricName)
	return gauge, cleaner
}

// getOrCreateHistogram creates or retrieves an existing histogram for the given metric name
func (dmm *DynamicMetricsManager) getOrCreateHistogram(metricName string) (*prometheus.HistogramVec, *cleaner) {
	sanitizedName := dmm.sanitizeMetricName(metricName) + "_histogram"

	// Try to get existing histogram (read lock)
	dmm.mutex.RLock()
	if histogram, exists := dmm.histograms[sanitizedName]; exists {
		cleaner := dmm.cleaners[sanitizedName]
		dmm.mutex.RUnlock()
		return histogram, cleaner
	}
	dmm.mutex.RUnlock()

	// Create new histogram (write lock)
	dmm.mutex.Lock()
	defer dmm.mutex.Unlock()

	// Double-check after acquiring write lock
	if histogram, exists := dmm.histograms[sanitizedName]; exists {
		cleaner := dmm.cleaners[sanitizedName]
		return histogram, cleaner
	}

	// Create the histogram
	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    sanitizedName,
			Help:    fmt.Sprintf("Druid histogram metric: %s", metricName),
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"dns_name", "service", "host", "datasource", "id"},
	)

	// Register the new histogram
	if err := prometheus.Register(histogram); err != nil {
		dmm.trackError("histogram_registration_error")
		logrus.Errorf("Failed to register histogram %s: %v", sanitizedName, err)
		return nil, nil
	}

	// Track dynamic metric creation
	dmm.trackCreation("histogram")

	// Note: Histograms don't use cleaners in current implementation
	var cleaner *cleaner = nil

	// Store in registry
	dmm.histograms[sanitizedName] = histogram
	dmm.cleaners[sanitizedName] = cleaner

	logrus.Debugf("Created new dynamic histogram: %s (from %s)", sanitizedName, metricName)
	return histogram, cleaner
}

// GetOrCreateMetrics creates or retrieves both gauge and histogram for the given metric name
func (dmm *DynamicMetricsManager) GetOrCreateMetrics(metricName string, enableHistogram bool) *MetricResult {
	gauge, gaugeCleaner := dmm.getOrCreateGauge(metricName)

	var histogram *prometheus.HistogramVec
	var histogramCleaner *cleaner

	if enableHistogram {
		histogram, histogramCleaner = dmm.getOrCreateHistogram(metricName)
	}

	// For simplicity, we return the gauge cleaner since histograms don't use cleaners currently
	cleaner := gaugeCleaner
	if histogramCleaner != nil {
		cleaner = histogramCleaner
	}

	return &MetricResult{
		Gauge:     gauge,
		Histogram: histogram,
		Cleaner:   cleaner,
	}
}

// trackCreation tracks the creation of a new dynamic metric
func (dmm *DynamicMetricsManager) trackCreation(metricType string) {
	if dmm.metricsTotal != nil {
		dmm.metricsTotal.WithLabelValues(metricType).Inc()
	}
	if dmm.metricsActive != nil {
		dmm.metricsActive.WithLabelValues(metricType).Inc()
	}
}

// trackError tracks errors in dynamic metric operations
func (dmm *DynamicMetricsManager) trackError(errorType string) {
	if dmm.metricsErrors != nil {
		dmm.metricsErrors.WithLabelValues(errorType).Inc()
	}
}
