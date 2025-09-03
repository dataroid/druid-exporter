package main

import (
	"druid-exporter/collector"
	"druid-exporter/listener"
	"net/http"
	"runtime"
	"time"

	"github.com/gorilla/mux"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	port = kingpin.Flag(
		"port",
		"Port to listen druid exporter, EnvVar - PORT. (Default - 8080)",
	).Default("8080").OverrideDefaultFromEnvar("PORT").Short('p').String()
	logLevel = kingpin.Flag(
		"log.level",
		"Log level for druid exporter, EnvVar - LOG_LEVEL. (Default: info)",
	).Default("info").OverrideDefaultFromEnvar("LOG_LEVEL").Short('l').String()
	logFormat = kingpin.Flag(
		"log.format",
		"Log format for druid exporter, text or json, EnvVar - LOG_FORMAT. (Default: text)",
	).Default("text").OverrideDefaultFromEnvar("LOG_FORMAT").Short('f').String()
	disableHistogram = kingpin.Flag(
		"no-histogram",
		"Flag whether to export histogram metrics or not.",
	).Default("false").OverrideDefaultFromEnvar("NO_HISTOGRAM").Bool()

	metricsCleanupTTL = kingpin.Flag(
		"metrics-cleanup-ttl",
		"Flag to provide time in minutes for metrics cleanup.",
	).Default("5").OverrideDefaultFromEnvar("METRICS_CLEANUP_TTL").Int()

	maxRequestSize = kingpin.Flag(
		"max-request-size",
		"Maximum request body size in MB to prevent memory exhaustion.",
	).Default("10").OverrideDefaultFromEnvar("MAX_REQUEST_SIZE_MB").Int()

	// Internal monitoring metrics variables
	exporterRequestsTotal    *prometheus.CounterVec
	exporterRequestDuration  *prometheus.HistogramVec
	exporterRequestSizeBytes *prometheus.HistogramVec
	exporterMetricsProcessed *prometheus.CounterVec
	exporterErrorsTotal      *prometheus.CounterVec
	exporterMemoryUsageBytes *prometheus.GaugeVec
	exporterCleanupMetrics   *prometheus.CounterVec
	exporterHttpClientStats  *prometheus.GaugeVec

	druidEmittedDataHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "druid_emitted_metrics_histogram",
			Help: "Druid emitted metrics from druid emitter",
		}, []string{"host", "sourceIp", "metric_name", "service", "datasource", "id"},
	)
	druidEmittedDataGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "druid_emitted_metrics",
			Help: "Druid emitted metrics from druid emitter",
		}, []string{"host", "sourceIp", "metric_name", "service", "datasource", "id"},
	)
)

func init() {
	prometheus.MustRegister(druidEmittedDataHistogram)
	prometheus.MustRegister(druidEmittedDataGauge)

	// Initialize and register internal monitoring metrics
	exporterRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "druid_exporter_requests_total",
			Help: "Total number of requests received by druid exporter",
		}, []string{"method", "status", "source_ip"},
	)

	exporterRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "druid_exporter_request_duration_seconds",
			Help:    "Duration of requests processed by druid exporter",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "status"},
	)

	exporterRequestSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "druid_exporter_request_size_bytes",
			Help:    "Size of requests received by druid exporter",
			Buckets: []float64{1024, 10240, 102400, 1048576, 10485760, 52428800, 104857600}, // 1KB to 100MB
		}, []string{"source_ip"},
	)

	exporterMetricsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "druid_exporter_metrics_processed_total",
			Help: "Total number of metrics processed by druid exporter",
		}, []string{"metric_type", "source_ip"},
	)

	exporterErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "druid_exporter_errors_total",
			Help: "Total number of errors in druid exporter",
		}, []string{"error_type", "source_ip"},
	)

	exporterMemoryUsageBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "druid_exporter_memory_usage_bytes",
			Help: "Current memory usage of druid exporter",
		}, []string{"memory_type"},
	)

	exporterCleanupMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "druid_exporter_cleanup_metrics_total",
			Help: "Total number of metrics cleaned up by druid exporter",
		}, []string{"cleanup_reason"},
	)

	exporterHttpClientStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "druid_exporter_http_client_connections",
			Help: "HTTP client connection statistics",
		}, []string{"connection_state"},
	)

	prometheus.MustRegister(exporterRequestsTotal)
	prometheus.MustRegister(exporterRequestDuration)
	prometheus.MustRegister(exporterRequestSizeBytes)
	prometheus.MustRegister(exporterMetricsProcessed)
	prometheus.MustRegister(exporterErrorsTotal)
	prometheus.MustRegister(exporterMemoryUsageBytes)
	prometheus.MustRegister(exporterCleanupMetrics)
	prometheus.MustRegister(exporterHttpClientStats)
}

func main() {
	kingpin.Version("0.10")
	kingpin.Parse()
	parsedLevel, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.Errorf("log-level flag has invalid value %s", *logLevel)
	} else {
		logrus.SetLevel(parsedLevel)
	}
	if *logFormat == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	dnsCache := cache.New(5*time.Minute, 10*time.Minute)
	router := mux.NewRouter()

	// Create collector once to prevent memory leaks
	druidCollector := collector.Collector()
	prometheus.MustRegister(druidCollector)

	router.Handle("/druid", listener.DruidHTTPEndpoint(*metricsCleanupTTL, *maxRequestSize, *disableHistogram, druidEmittedDataHistogram, druidEmittedDataGauge, dnsCache, exporterRequestsTotal, exporterRequestDuration, exporterRequestSizeBytes, exporterMetricsProcessed, exporterErrorsTotal))
	// Start memory monitoring goroutine
	go startMemoryMonitoring()

	router.Handle("/metrics", promhttp.Handler()) // Use default handler instead of creating new registries
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Druid Exporter</title></head>
			<body>
			<h1>Druid Exporter</h1>
			<p><a href="/metrics">Metrics</a></p>
			</body>
			</html>`))
	})
	logrus.Infof("Druid exporter started listening on: %v", *port)
	logrus.Infof("Metrics endpoint - http://0.0.0.0:%v/metrics", *port)
	logrus.Infof("Druid emitter endpoint - http://0.0.0.0:%v/druid", *port)
	http.ListenAndServe("0.0.0.0:"+*port, router)
}

// startMemoryMonitoring monitors memory usage and reports it via metrics
func startMemoryMonitoring() {
	ticker := time.NewTicker(30 * time.Second) // Update every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			// Update memory metrics
			exporterMemoryUsageBytes.WithLabelValues("heap_alloc").Set(float64(m.Alloc))
			exporterMemoryUsageBytes.WithLabelValues("heap_sys").Set(float64(m.HeapSys))
			exporterMemoryUsageBytes.WithLabelValues("heap_idle").Set(float64(m.HeapIdle))
			exporterMemoryUsageBytes.WithLabelValues("heap_inuse").Set(float64(m.HeapInuse))
			exporterMemoryUsageBytes.WithLabelValues("stack_inuse").Set(float64(m.StackInuse))
			exporterMemoryUsageBytes.WithLabelValues("stack_sys").Set(float64(m.StackSys))
			exporterMemoryUsageBytes.WithLabelValues("sys_total").Set(float64(m.Sys))

			logrus.Debugf("Memory stats: Alloc=%dMB, Sys=%dMB, NumGC=%d",
				bToMb(m.Alloc), bToMb(m.Sys), m.NumGC)
		}
	}
}

// bToMb converts bytes to megabytes
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// newHandler function removed - was causing memory leaks by creating new registries on every request
// Now using promhttp.Handler() directly which uses the default registry
