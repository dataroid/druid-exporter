package listener

import (
	"druid-exporter/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/gddo/httputil/header"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// DruidHTTPEndpoint is the endpoint to listen all druid metrics
func DruidHTTPEndpoint(metricsCleanupTTL int, maxRequestSizeMB int, disableHistogram bool, histogram *prometheus.HistogramVec, gauge *prometheus.GaugeVec, dnsCache *cache.Cache, requestsTotal *prometheus.CounterVec, requestDuration *prometheus.HistogramVec, requestSizeBytes *prometheus.HistogramVec, metricsProcessed *prometheus.CounterVec, errorsTotal *prometheus.CounterVec) http.HandlerFunc {
	gaugeCleaner := newCleaner(gauge, metricsCleanupTTL)

	// Track metrics to prevent excessive memory usage
	requestCount := 0

	logrus.Infof("Druid HTTP endpoint initialized with %dMB request size limit", maxRequestSizeMB)

	return func(w http.ResponseWriter, req *http.Request) {
		startTime := time.Now()
		requestCount++
		var druidData []map[string]interface{}
		var id string
		var status string = "success"
		var sourceIP string = strings.Split(req.RemoteAddr, ":")[0]

		// Always track request count and duration at the end
		defer func() {
			requestsTotal.WithLabelValues(req.Method, status, sourceIP).Inc()
			requestDuration.WithLabelValues(req.Method, status).Observe(time.Since(startTime).Seconds())
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
				if err.Error() == "http: request body too large" {
					status = "error_size_limit"
					errorsTotal.WithLabelValues("request_too_large", sourceIP).Inc()
					logrus.Errorf("Request body too large! Max allowed: %dMB. Rejecting request from %s",
						maxRequestSizeMB, req.RemoteAddr)
					logrus.Warnf("Consider increasing MAX_REQUEST_SIZE_MB if legitimate large payloads are expected")
					http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
					return
				}
				status = "error_read"
				errorsTotal.WithLabelValues("request_read_error", sourceIP).Inc()
				logrus.Debugf("Unable to read JSON response: %v", err)
				http.Error(w, "Bad request", http.StatusBadRequest)
				return
			}

			err = json.Unmarshal(output, &druidData)
			if err != nil {
				status = "error_json"
				errorsTotal.WithLabelValues("json_unmarshal_error", sourceIP).Inc()
				logrus.Errorf("Error decoding JSON sent by druid: %v", err)
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}

			// Track request size
			requestSizeBytes.WithLabelValues(sourceIP).Observe(float64(len(output)))

			// Log request size for monitoring
			requestSizeKB := float64(len(output)) / 1024
			requestSizeMB := requestSizeKB / 1024
			if requestSizeKB > 100 { // Log requests larger than 100KB
				if requestSizeMB >= 1.0 {
					logrus.Infof("Processing large request: %.2fMB (%d metrics) from %s",
						requestSizeMB, len(druidData), req.RemoteAddr)
				} else {
					logrus.Debugf("Processing request: %.1fKB (%d metrics) from %s",
						requestSizeKB, len(druidData), req.RemoteAddr)
				}
			}

			if druidData == nil {
				status = "empty_data"
				logrus.Debugf("The dataset for druid is empty, can be ignored: %v", druidData)
				return
			}

			// Track number of metrics processed
			metricsProcessed.WithLabelValues("druid_emitted", sourceIP).Add(float64(len(druidData)))
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
				metricName := strings.Replace(metric, "/", "-", 3)
				serviceName := strings.Replace(service, "/", "-", 3)

				// Reverse DNS Lookup
				// Mutates dnsCache
				hostValue := strings.Split(hostname, ":")[0]
				dnsLookupValue := utils.ReverseDNSLookup(hostValue, dnsCache)

				host := strings.Replace(hostname, hostValue, dnsLookupValue, 1) // Adding back port

				if i == 0 { // Comment out this line if you want the whole metrics received
					logrus.Tracef("parameters received and mapped:")
					logrus.Tracef("    metric     => %s", metric)
					logrus.Tracef("    service    => %s", service)
					logrus.Tracef("    hostname   => (%s -> %s)", hostname, host)
					logrus.Tracef("    datasource => %v", datasource)
					logrus.Tracef("    value      => %v", value)
					logrus.Tracef("    id         => %v", id)
				}

				if data["dataSource"] != nil {
					if arrDatasource, ok := datasource.([]interface{}); ok { // Array datasource
						for _, entryDatasource := range arrDatasource {
							if !disableHistogram {
								histogram.With(prometheus.Labels{
									"metric_name": metricName,
									"service":     serviceName,
									"datasource":  entryDatasource.(string),
									"host":        host,
									"sourceIp":    hostValue,
									"id":          id,
								}).Observe(value)
							}

							labels := prometheus.Labels{
								"metric_name": metricName,
								"service":     serviceName,
								"datasource":  entryDatasource.(string),
								"host":        host,
								"sourceIp":    hostValue,
								"id":          id,
							}
							gaugeCleaner.add(labels)
							gauge.With(labels).Set(value)
						}
					} else { // Single datasource
						if !disableHistogram {
							histogram.With(prometheus.Labels{
								"metric_name": metricName,
								"service":     serviceName,
								"datasource":  datasource.(string),
								"host":        host,
								"sourceIp":    hostValue,
								"id":          id,
							}).Observe(value)
						}

						labels := prometheus.Labels{
							"metric_name": metricName,
							"service":     serviceName,
							"datasource":  datasource.(string),
							"host":        host,
							"sourceIp":    hostValue,
							"id":          id,
						}
						gaugeCleaner.add(labels)
						gauge.With(labels).Set(value)
					}
				} else { // Missing datasource case
					if !disableHistogram {
						histogram.With(prometheus.Labels{
							"metric_name": metricName,
							"service":     serviceName,
							"datasource":  "",
							"host":        host,
							"sourceIp":    hostValue,
							"id":          id,
						}).Observe(value)
					}

					labels := prometheus.Labels{
						"metric_name": metricName,
						"service":     serviceName,
						"datasource":  "",
						"host":        host,
						"sourceIp":    hostValue,
						"id":          id,
					}
					gaugeCleaner.add(labels)
					gauge.With(labels).Set(value)
				}
			}

			logrus.Infof("Successfully collected data from druid emitter, %s", druidData[0]["service"].(string))
		}
	}
}
