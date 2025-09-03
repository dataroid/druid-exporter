package listener

import (
	"druid-exporter/internal"
	"druid-exporter/utils"
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// DruidMetric represents a parsed druid metric data point
type DruidMetric struct {
	MetricName  string
	ServiceName string
	HostValue   string
	Datasource  string
	ID          string
	Value       float64
	SourceIP    string
	DNSLookup   string
}

// RequestProcessor handles the processing of druid metric requests
type RequestProcessor struct {
	config           *internal.Config
	deps             *internal.Dependencies
	dynamicManager   *DynamicMetricsManager
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestSizeBytes *prometheus.HistogramVec
	metricsProcessed *prometheus.CounterVec
	errorsTotal      *prometheus.CounterVec
}

// NewRequestProcessor creates a new request processor
func NewRequestProcessor(config *internal.Config, deps *internal.Dependencies) *RequestProcessor {
	// Get metrics from dependencies
	requestsTotal, requestDuration, requestSizeBytes, metricsProcessed, errorsTotal := deps.InternalMetrics.GetRequestMetrics()
	dynamicMetricsTotal, dynamicMetricsActive, dynamicMetricsErrors := deps.InternalMetrics.GetDynamicMetrics()
	cleanerDuration, cleanerErrors, cleanerMetricsTracked, cleanerMetricsDeleted, cleanerRuns := deps.InternalMetrics.GetCleanerMetrics()

	// Create dynamic metrics manager
	dynamicManager := NewDynamicMetricsManager(
		config.MetricsCleanupTTL,
		dynamicMetricsTotal, dynamicMetricsActive, dynamicMetricsErrors,
		cleanerDuration, cleanerErrors, cleanerMetricsTracked, cleanerMetricsDeleted, cleanerRuns,
	)

	return &RequestProcessor{
		config:           config,
		deps:             deps,
		dynamicManager:   dynamicManager,
		requestsTotal:    requestsTotal,
		requestDuration:  requestDuration,
		requestSizeBytes: requestSizeBytes,
		metricsProcessed: metricsProcessed,
		errorsTotal:      errorsTotal,
	}
}

// safeStringConvert safely converts interface{} to string
func (rp *RequestProcessor) safeStringConvert(value interface{}) string {
	if str, ok := value.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", value)
}

// extractRequiredField extracts a required field from metric data
func (rp *RequestProcessor) extractRequiredField(data map[string]interface{}, fieldName, sourceIP, dnsLookup string) (string, bool) {
	value, exists := data[fieldName]
	if !exists {
		// Get available keys for debugging
		var availableKeys []string
		for key := range data {
			availableKeys = append(availableKeys, key)
		}

		// If metric field is missing, dump the entire data structure for debugging
		if fieldName == "metric" {
			logrus.Errorf("MISSING METRIC FIELD from %s (%s) - DUMPING FULL DATA: %+v", sourceIP, dnsLookup, data)
		} else {
			logrus.Warnf("Metric data missing '%s' field from %s (%s). Available keys: %v",
				fieldName, sourceIP, dnsLookup, availableKeys)
		}

		if rp.errorsTotal != nil {
			rp.errorsTotal.WithLabelValues("missing_"+fieldName+"_field", sourceIP).Inc()
		}
		return "", false
	}
	return rp.safeStringConvert(value), true
}

// parseFloatValue safely parses a float value from interface{}
func (rp *RequestProcessor) parseFloatValue(value interface{}, sourceIP, dnsLookup string) (float64, bool) {
	floatVal, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
	if err != nil {
		logrus.Warnf("Failed to parse value from %s (%s): %v", sourceIP, dnsLookup, err)
		return 0, false
	}
	return floatVal, true
}

// parseDruidMetric converts raw metric data into a structured DruidMetric
func (rp *RequestProcessor) parseDruidMetric(data map[string]interface{}, sourceIP, dnsLookup string) (*DruidMetric, bool) {
	// Extract required fields
	metricName, ok := rp.extractRequiredField(data, "metric", sourceIP, dnsLookup)
	if !ok {
		return nil, false
	}

	serviceName, ok := rp.extractRequiredField(data, "service", sourceIP, dnsLookup)
	if !ok {
		return nil, false
	}

	hostValue, ok := rp.extractRequiredField(data, "host", sourceIP, dnsLookup)
	if !ok {
		return nil, false
	}

	// Parse numeric value
	value, ok := rp.parseFloatValue(data["value"], sourceIP, dnsLookup)
	if !ok {
		return nil, false
	}

	// Extract optional fields
	datasource := rp.safeStringConvert(data["dataSource"])

	var id string
	if data["id"] != nil {
		id = rp.safeStringConvert(data["id"])
	}

	// Clean up values
	serviceName = strings.Replace(serviceName, "/", "-", -1)
	hostValue = strings.Split(hostValue, ":")[0]
	hostDNSLookup := utils.ReverseDNSLookup(hostValue, rp.deps.DNSCache)

	return &DruidMetric{
		MetricName:  metricName,
		ServiceName: serviceName,
		HostValue:   hostValue,
		Datasource:  datasource,
		ID:          id,
		Value:       value,
		SourceIP:    sourceIP,
		DNSLookup:   hostDNSLookup,
	}, true
}

// updateMetrics updates Prometheus metrics with the parsed druid metric
func (rp *RequestProcessor) updateMetrics(metric *DruidMetric) {
	enableHistogram := !rp.config.DisableHistogram
	result := rp.dynamicManager.GetOrCreateMetrics(metric.MetricName, enableHistogram)

	// Update gauge
	if result.Gauge != nil {
		result.Gauge.WithLabelValues(
			metric.DNSLookup,
			metric.ServiceName,
			metric.HostValue,
			metric.Datasource,
			metric.ID,
		).Set(metric.Value)

		if result.Cleaner != nil {
			result.Cleaner.add(prometheus.Labels{
				"dns_name":   metric.DNSLookup,
				"service":    metric.ServiceName,
				"host":       metric.HostValue,
				"datasource": metric.Datasource,
				"id":         metric.ID,
			})
		}
	}

	// Update histogram if enabled
	if result.Histogram != nil && enableHistogram {
		result.Histogram.WithLabelValues(
			metric.DNSLookup,
			metric.ServiceName,
			metric.HostValue,
			metric.Datasource,
			metric.ID,
		).Observe(metric.Value)
	}
}

// ProcessMetrics processes a slice of raw druid metric data
func (rp *RequestProcessor) ProcessMetrics(druidData []map[string]interface{}, sourceIP, dnsLookup string) int {
	processedCount := 0

	for _, data := range druidData {
		metric, ok := rp.parseDruidMetric(data, sourceIP, dnsLookup)
		if !ok {
			continue // Skip invalid metrics
		}

		rp.updateMetrics(metric)
		processedCount++
	}

	// Track processed metrics
	if rp.metricsProcessed != nil {
		rp.metricsProcessed.WithLabelValues(sourceIP).Add(float64(processedCount))
	}

	return processedCount
}

// TrackError tracks an error in processing
func (rp *RequestProcessor) TrackError(errorType, sourceIP string) {
	if rp.errorsTotal != nil {
		rp.errorsTotal.WithLabelValues(errorType, sourceIP).Inc()
	}
}

// TrackRequest tracks request metrics
func (rp *RequestProcessor) TrackRequest(method, status, sourceIP string) {
	if rp.requestsTotal != nil {
		rp.requestsTotal.WithLabelValues(method, status, sourceIP).Inc()
	}
}

// TrackRequestDuration tracks request duration
func (rp *RequestProcessor) TrackRequestDuration(method, status, sourceIP string, duration float64) {
	if rp.requestDuration != nil {
		rp.requestDuration.WithLabelValues(method, status, sourceIP).Observe(duration)
	}
}

// TrackRequestSize tracks request size
func (rp *RequestProcessor) TrackRequestSize(sourceIP string, size float64) {
	if rp.requestSizeBytes != nil {
		rp.requestSizeBytes.WithLabelValues(sourceIP).Observe(size)
	}
}
