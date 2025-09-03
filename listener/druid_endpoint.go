package listener

import (
	"druid-exporter/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/gddo/httputil/header"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Dynamic metric registries to avoid high cardinality
var (
	dynamicGauges     = make(map[string]*prometheus.GaugeVec)
	dynamicHistograms = make(map[string]*prometheus.HistogramVec)
	dynamicCleaners   = make(map[string]*cleaner)
	dynamicMutex      = sync.RWMutex{}
)

// sanitizeMetricName converts druid metric names to valid Prometheus metric names
func sanitizeMetricName(metricName string) string {
	// Replace invalid characters with underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := reg.ReplaceAllString(metricName, "_")

	// Ensure it starts with a letter or underscore
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "_" + sanitized
	}

	// Add druid prefix
	return "druid_" + strings.ToLower(sanitized)
}

// getOrCreateGauge returns existing gauge or creates new one for the metric
func getOrCreateGauge(metricName string, metricsCleanupTTL int) (*prometheus.GaugeVec, *cleaner) {
	sanitizedName := sanitizeMetricName(metricName)

	dynamicMutex.RLock()
	if gauge, exists := dynamicGauges[sanitizedName]; exists {
		cleaner := dynamicCleaners[sanitizedName]
		dynamicMutex.RUnlock()
		return gauge, cleaner
	}
	dynamicMutex.RUnlock()

	// Create new gauge with write lock
	dynamicMutex.Lock()
	defer dynamicMutex.Unlock()

	// Double-check after acquiring write lock
	if gauge, exists := dynamicGauges[sanitizedName]; exists {
		cleaner := dynamicCleaners[sanitizedName]
		return gauge, cleaner
	}

	// Create new gauge without metric_name label (reduces cardinality)
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: sanitizedName,
			Help: fmt.Sprintf("Druid emitted metric: %s", metricName),
		}, []string{"host", "sourceIp", "service", "datasource", "id"},
	)

	// Register the new gauge
	if err := prometheus.Register(gauge); err != nil {
		logrus.Errorf("Failed to register gauge %s: %v", sanitizedName, err)
		// Return a dummy gauge to prevent panics
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dummy"}, []string{}), nil
	}

	// Create cleaner for this specific gauge
	cleaner := newCleaner(gauge, metricsCleanupTTL)

	// Store in registry
	dynamicGauges[sanitizedName] = gauge
	dynamicCleaners[sanitizedName] = cleaner

	logrus.Debugf("Created new dynamic gauge: %s (from %s)", sanitizedName, metricName)
	return gauge, cleaner
}

// getOrCreateHistogram returns existing histogram or creates new one for the metric
func getOrCreateHistogram(metricName string) *prometheus.HistogramVec {
	sanitizedName := sanitizeMetricName(metricName) + "_histogram"

	dynamicMutex.RLock()
	if histogram, exists := dynamicHistograms[sanitizedName]; exists {
		dynamicMutex.RUnlock()
		return histogram
	}
	dynamicMutex.RUnlock()

	// Create new histogram with write lock
	dynamicMutex.Lock()
	defer dynamicMutex.Unlock()

	// Double-check after acquiring write lock
	if histogram, exists := dynamicHistograms[sanitizedName]; exists {
		return histogram
	}

	// Create new histogram without metric_name label (reduces cardinality)
	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: sanitizedName,
			Help: fmt.Sprintf("Druid emitted metric histogram: %s", metricName),
		}, []string{"host", "sourceIp", "service", "datasource", "id"},
	)

	// Register the new histogram
	if err := prometheus.Register(histogram); err != nil {
		logrus.Errorf("Failed to register histogram %s: %v", sanitizedName, err)
		// Return a dummy histogram to prevent panics
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "dummy_histogram"}, []string{})
	}

	// Store in registry
	dynamicHistograms[sanitizedName] = histogram

	logrus.Debugf("Created new dynamic histogram: %s (from %s)", sanitizedName, metricName)
	return histogram
}

// DruidHTTPEndpoint is the endpoint to listen all druid metrics
func DruidHTTPEndpoint(metricsCleanupTTL int, maxRequestSizeMB int, disableHistogram bool, dnsCache *cache.Cache, requestsTotal *prometheus.CounterVec, requestDuration *prometheus.HistogramVec, requestSizeBytes *prometheus.HistogramVec, metricsProcessed *prometheus.CounterVec, errorsTotal *prometheus.CounterVec) http.HandlerFunc {

	// Track metrics to prevent excessive memory usage
	requestCount := 0

	logrus.Infof("Druid HTTP endpoint initialized with %dMB request size limit", maxRequestSizeMB)

	return func(w http.ResponseWriter, req *http.Request) {
		startTime := time.Now()
		requestCount++
		var druidData []map[string]interface{}
		var id string
		var status string = "200"

		// Extract source IP for DNS lookup
		sourceIP := strings.Split(req.RemoteAddr, ":")[0]
		dnsLookupValue := utils.ReverseDNSLookup(sourceIP, dnsCache)

		// Always track request count and duration at the end
		defer func() {
			if requestsTotal != nil {
				requestsTotal.WithLabelValues(req.Method, status, sourceIP).Inc()
			}
			if requestDuration != nil {
				requestDuration.WithLabelValues(req.Method, status, sourceIP).Observe(time.Since(startTime).Seconds())
			}
		}()

		reqHeader, _ := header.ParseValueAndParams(req.Header, "Content-Type")
		if req.Method == "POST" && reqHeader == "application/json" {
			// Limit request body size to prevent memory exhaustion
			maxBytes := int64(maxRequestSizeMB) * 1024 * 1024
			req.Body = http.MaxBytesReader(w, req.Body, maxBytes)

			output, err := ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			if err != nil {
				// Check if error is due to request size limit
				if strings.Contains(err.Error(), "http: request body too large") {
					status = "413"
					if errorsTotal != nil {
						errorsTotal.WithLabelValues("request_too_large", sourceIP).Inc()
					}
					logrus.WithFields(logrus.Fields{
						"source_ip":      sourceIP,
						"dns_lookup":     dnsLookupValue,
						"request_method": req.Method,
						"request_path":   req.URL.Path,
						"content_length": req.Header.Get("Content-Length"),
						"max_allowed":    fmt.Sprintf("%dMB", maxRequestSizeMB),
					}).Errorf("Request body size exceeds limit: %v. Consider increasing --max-request-size flag", err)
					http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
					return
				}
				status = "400"
				if errorsTotal != nil {
					errorsTotal.WithLabelValues("read_error", sourceIP).Inc()
				}
				logrus.Errorf("Error reading request body: %v", err)
				http.Error(w, "Bad request", http.StatusBadRequest)
				return
			}

			err = json.Unmarshal(output, &druidData)
			if err != nil {
				status = "400"
				if errorsTotal != nil {
					errorsTotal.WithLabelValues("invalid_json", sourceIP).Inc()
				}
				logrus.Errorf("Error unmarshaling JSON: %v", err)
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}

			// Track request size
			if requestSizeBytes != nil {
				requestSizeBytes.WithLabelValues(sourceIP).Observe(float64(len(output)))
			}

			// Log large requests (>1MB) and very large requests (>100KB)
			if len(output) > 1024*1024 {
				logrus.Infof("Large request received: %d bytes from %s (%s)", len(output), sourceIP, dnsLookupValue)
			} else if len(output) > 100*1024 {
				logrus.Debugf("Medium request received: %d bytes from %s (%s)", len(output), sourceIP, dnsLookupValue)
			}

			if druidData == nil {
				status = "empty_data"
				logrus.Debugf("The dataset for druid is empty, can be ignored: %v", druidData)
				return
			}

			// Track processed metrics count
			if len(druidData) > 100 {
				logrus.Infof("Processing %d metrics from %s (%s)", len(druidData), sourceIP, dnsLookupValue)
			} else if len(druidData) > 10 {
				logrus.Debugf("Processing %d metrics from %s (%s)", len(druidData), sourceIP, dnsLookupValue)
			}
			if metricsProcessed != nil {
				metricsProcessed.WithLabelValues(sourceIP).Add(float64(len(druidData)))
			}
			for i, data := range druidData {
				// Use type assertions instead of fmt.Sprintf to reduce allocations
				metric, _ := data["metric"].(string)
				if metric == "" {
					metric = fmt.Sprintf("%v", data["metric"])
				}
				service, _ := data["service"].(string)
				if service == "" {
					service = fmt.Sprintf("%v", data["service"])
				}
				hostname, _ := data["host"].(string)
				if hostname == "" {
					hostname = fmt.Sprintf("%v", data["host"])
				}
				datasource := data["dataSource"]
				value, _ := strconv.ParseFloat(fmt.Sprintf("%v", data["value"]), 64)

				if data["id"] != nil {
					if idStr, ok := data["id"].(string); ok {
						id = idStr
					} else {
						id = fmt.Sprintf("%v", data["id"])
					}
				} else {
					id = ""
				}

				// Pre-compute string replacements to prevent memory leaks
				serviceName := strings.Replace(service, "/", "-", 3)

				// Reverse DNS Lookup
				// Mutates dnsCache
				hostValue := strings.Split(hostname, ":")[0]
				dnsLookupValue := utils.ReverseDNSLookup(hostValue, dnsCache)

				host := strings.Replace(hostname, hostValue, dnsLookupValue, 1) // Adding back port

				if i == 0 { // Comment out this line if you want the whole metrics received
					logrus.Tracef("parameters received and mapped:")
					logrus.Tracef("    metric     => %s", metric)
					logrus.Tracef("    service    => %s", serviceName)
					logrus.Tracef("    hostname   => (%s -> %s)", hostname, host)
					logrus.Tracef("    datasource => %v", datasource)
					logrus.Tracef("    value      => %v", value)
					logrus.Tracef("    id         => %v", id)
				}

				// Get or create dynamic metrics for this specific metric name
				dynamicGauge, dynamicCleaner := getOrCreateGauge(metric, metricsCleanupTTL)
				var dynamicHistogram *prometheus.HistogramVec
				if !disableHistogram {
					dynamicHistogram = getOrCreateHistogram(metric)
				}

				if data["dataSource"] != nil {
					if arrDatasource, ok := datasource.([]interface{}); ok { // Array datasource
						for _, entryDatasource := range arrDatasource {
							labels := prometheus.Labels{
								"service":    serviceName,
								"datasource": entryDatasource.(string),
								"host":       host,
								"sourceIp":   hostValue,
								"id":         id,
							}

							if !disableHistogram && dynamicHistogram != nil {
								dynamicHistogram.With(labels).Observe(value)
							}

							if dynamicCleaner != nil {
								dynamicCleaner.add(labels)
							}
							dynamicGauge.With(labels).Set(value)
						}
					} else { // Single datasource
						labels := prometheus.Labels{
							"service":    serviceName,
							"datasource": datasource.(string),
							"host":       host,
							"sourceIp":   hostValue,
							"id":         id,
						}

						if !disableHistogram && dynamicHistogram != nil {
							dynamicHistogram.With(labels).Observe(value)
						}

						if dynamicCleaner != nil {
							dynamicCleaner.add(labels)
						}
						dynamicGauge.With(labels).Set(value)
					}
				} else { // Missing datasource case
					labels := prometheus.Labels{
						"service":    serviceName,
						"datasource": "",
						"host":       host,
						"sourceIp":   hostValue,
						"id":         id,
					}

					if !disableHistogram && dynamicHistogram != nil {
						dynamicHistogram.With(labels).Observe(value)
					}

					if dynamicCleaner != nil {
						dynamicCleaner.add(labels)
					}
					dynamicGauge.With(labels).Set(value)
				}
			}

			logrus.Infof("Successfully collected data from druid emitter, %s", druidData[0]["service"].(string))
		}
	}
}
