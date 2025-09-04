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

	"github.com/golang/gddo/httputil/header"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// isSeparateMetric checks if a metric name should be routed to separate metrics
func isSeparateMetric(metricName string, separateMetrics []string) bool {
	if len(separateMetrics) == 0 {
		return false
	}
	for _, separate := range separateMetrics {
		if metricName == separate {
			return true
		}
	}
	return false
}

// sanitizeMetricName converts a druid metric name to a valid Prometheus metric name
func sanitizeMetricName(metricName string) string {
	// Replace invalid characters with underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_:]`)
	sanitized := reg.ReplaceAllString(metricName, "_")

	// Ensure it starts with a letter or underscore
	if len(sanitized) > 0 && (sanitized[0] >= '0' && sanitized[0] <= '9') {
		sanitized = "_" + sanitized
	}

	return sanitized
}

// getOrCreateSeparateMetric gets or creates a separate metric for the given metric name
func getOrCreateSeparateMetric(metricName, prefix string, separateHistograms map[string]*prometheus.HistogramVec, separateGauges map[string]*prometheus.GaugeVec, mux *sync.RWMutex, disableHistogram bool) (*prometheus.HistogramVec, *prometheus.GaugeVec) {
	sanitizedName := sanitizeMetricName(metricName)
	metricKey := prefix + "_" + sanitizedName

	mux.RLock()
	histogram, histExists := separateHistograms[metricKey]
	gauge, gaugeExists := separateGauges[metricKey]
	mux.RUnlock()

	if histExists && gaugeExists {
		return histogram, gauge
	}

	mux.Lock()
	defer mux.Unlock()

	// Double-check after acquiring write lock
	histogram, histExists = separateHistograms[metricKey]
	gauge, gaugeExists = separateGauges[metricKey]

	if !histExists {
		histogram = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: metricKey + "_histogram",
				Help: fmt.Sprintf("Druid metric %s histogram from druid emitter", metricName),
			}, []string{"host", "service", "datasource", "id", "source_ip"},
		)
		separateHistograms[metricKey] = histogram
		prometheus.MustRegister(histogram)
	}

	if !gaugeExists {
		gauge = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metricKey,
				Help: fmt.Sprintf("Druid metric %s from druid emitter", metricName),
			}, []string{"host", "service", "datasource", "id", "source_ip"},
		)
		separateGauges[metricKey] = gauge
		prometheus.MustRegister(gauge)
	}

	logrus.Infof("Created separate metric: %s for original metric: %s", metricKey, metricName)
	return histogram, gauge
}

// DruidHTTPEndpoint is the endpoint to listen all druid metrics
func DruidHTTPEndpoint(metricsCleanupTTL int, disableHistogram bool, histogram *prometheus.HistogramVec, gauge *prometheus.GaugeVec, separateHistograms map[string]*prometheus.HistogramVec, separateGauges map[string]*prometheus.GaugeVec, separateMetricsMux *sync.RWMutex, separateMetricPrefix string, separateMetrics []string, dnsCache *cache.Cache) http.HandlerFunc {
	gaugeCleaner := newCleaner(gauge, metricsCleanupTTL)
	return func(w http.ResponseWriter, req *http.Request) {
		var druidData []map[string]interface{}
		var id string
		reqHeader, _ := header.ParseValueAndParams(req.Header, "Content-Type")
		if req.Method == "POST" && reqHeader == "application/json" {
			output, err := ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			if err != nil {
				logrus.Debugf("Unable to read JSON response: %v", err)
				return
			}
			err = json.Unmarshal(output, &druidData)
			if err != nil {
				logrus.Errorf("Error decoding JSON sent by druid: %v", err)
				if druidData != nil {
					logrus.Debugf("%v", druidData)
				}
				return
			}
			if druidData == nil {
				logrus.Debugf("The dataset for druid is empty, can be ignored: %v", druidData)
				return
			}
			for i, data := range druidData {
				metric := fmt.Sprintf("%v", data["metric"])
				service := fmt.Sprintf("%v", data["service"])
				hostname := fmt.Sprintf("%v", data["host"])
				datasource := data["dataSource"]
				value, _ := strconv.ParseFloat(fmt.Sprintf("%v", data["value"]), 64)

				if data["id"] != nil {
					id = fmt.Sprintf("%v", data["id"])
				} else {
					id = ""
				}
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

				// Determine which metrics to use based on metric name
				useSeparateMetrics := isSeparateMetric(metric, separateMetrics)
				var targetHistogram *prometheus.HistogramVec
				var targetGauge *prometheus.GaugeVec
				var targetCleaner cleaner
				var separateGaugeCleaner cleaner

				if useSeparateMetrics {
					targetHistogram, targetGauge = getOrCreateSeparateMetric(metric, separateMetricPrefix, separateHistograms, separateGauges, separateMetricsMux, disableHistogram)
					separateGaugeCleaner = newCleaner(targetGauge, metricsCleanupTTL)
					targetCleaner = separateGaugeCleaner
					logrus.Debugf("Routing metric '%s' to separate metrics", metric)
				} else {
					targetHistogram = histogram
					targetGauge = gauge
					targetCleaner = gaugeCleaner
				}

				if data["dataSource"] != nil {
					if arrDatasource, ok := datasource.([]interface{}); ok { // Array datasource
						for _, entryDatasource := range arrDatasource {
							var histLabels, gaugeLabels prometheus.Labels

							if useSeparateMetrics {
								// For separate metrics, don't include metric_name as it's part of the metric name
								histLabels = prometheus.Labels{
									"service":    strings.Replace(service, "/", "-", 3),
									"datasource": entryDatasource.(string),
									"host":       host,
									"id":         id,
									"source_ip":  hostValue,
								}
								gaugeLabels = histLabels
							} else {
								// For regular metrics, include metric_name
								histLabels = prometheus.Labels{
									"metric_name": strings.Replace(metric, "/", "-", 3),
									"service":     strings.Replace(service, "/", "-", 3),
									"datasource":  entryDatasource.(string),
									"host":        host,
									"id":          id,
								}
								gaugeLabels = histLabels
							}

							if !disableHistogram {
								targetHistogram.With(histLabels).Observe(value)
							}

							targetCleaner.add(gaugeLabels)
							targetGauge.With(gaugeLabels).Set(value)
						}
					} else { // Single datasource
						var histLabels, gaugeLabels prometheus.Labels

						if useSeparateMetrics {
							// For separate metrics, don't include metric_name as it's part of the metric name
							histLabels = prometheus.Labels{
								"service":    strings.Replace(service, "/", "-", 3),
								"datasource": datasource.(string),
								"host":       host,
								"id":         id,
								"source_ip":  hostValue,
							}
							gaugeLabels = histLabels
						} else {
							// For regular metrics, include metric_name
							histLabels = prometheus.Labels{
								"metric_name": strings.Replace(metric, "/", "-", 3),
								"service":     strings.Replace(service, "/", "-", 3),
								"datasource":  datasource.(string),
								"host":        host,
								"id":          id,
							}
							gaugeLabels = histLabels
						}

						if !disableHistogram {
							targetHistogram.With(histLabels).Observe(value)
						}

						targetCleaner.add(gaugeLabels)
						targetGauge.With(gaugeLabels).Set(value)
					}
				} else { // Missing datasource case
					var histLabels, gaugeLabels prometheus.Labels

					if useSeparateMetrics {
						// For separate metrics, don't include metric_name as it's part of the metric name
						histLabels = prometheus.Labels{
							"service":    strings.Replace(service, "/", "-", 3),
							"datasource": "",
							"host":       host,
							"id":         id,
							"source_ip":  hostValue,
						}
						gaugeLabels = histLabels
					} else {
						// For regular metrics, include metric_name
						histLabels = prometheus.Labels{
							"metric_name": strings.Replace(metric, "/", "-", 3),
							"service":     strings.Replace(service, "/", "-", 3),
							"datasource":  "",
							"host":        host,
							"id":          id,
						}
						gaugeLabels = histLabels
					}

					if !disableHistogram {
						targetHistogram.With(histLabels).Observe(value)
					}

					targetCleaner.add(gaugeLabels)
					targetGauge.With(gaugeLabels).Set(value)
				}
			}

			logrus.Infof("Successfully collected data from druid emitter, %s", druidData[0]["service"].(string))
		}
	}
}
