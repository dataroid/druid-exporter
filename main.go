package main

import (
	"druid-exporter/collector"
	"druid-exporter/internal"
	"druid-exporter/listener"
	"net/http"
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

	enableInternalMetrics = kingpin.Flag(
		"enable-internal-metrics",
		"Enable internal monitoring metrics for the exporter itself.",
	).Default("true").OverrideDefaultFromEnvar("ENABLE_INTERNAL_METRICS").Bool()

	// Internal metrics instance
	internalMetrics *internal.InternalMetrics
)

func init() {
	// Dynamic metrics will be registered on-demand
}

func main() {
	kingpin.Version("0.10")
	kingpin.Parse()

	// Set log level and format
	if level, err := logrus.ParseLevel(*logLevel); err == nil {
		logrus.SetLevel(level)
		logrus.Infof("Set log level to %s", *logLevel)
	} else {
		logrus.Warnf("Invalid log level %s, using default (info)", *logLevel)
	}

	if *logFormat == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
		logrus.Info("Set log format to JSON")
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{})
		logrus.Info("Set log format to text")
	}

	// Initialize internal metrics
	internalMetrics = internal.NewInternalMetrics(*enableInternalMetrics)

	// Create DNS cache
	dnsCache := cache.New(5*time.Minute, 10*time.Minute)

	// Create collector once to prevent memory leaks
	druidCollector := collector.Collector()
	prometheus.MustRegister(druidCollector)

	// Create configuration and dependencies
	config := internal.NewConfig(*metricsCleanupTTL, *maxRequestSize, *disableHistogram)
	deps := internal.NewDependencies(dnsCache, internalMetrics)

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.Handle("/druid", listener.DruidHTTPEndpoint(config, deps))

	// Start memory monitoring goroutine if internal metrics enabled
	if internalMetrics.IsEnabled() {
		internalMetrics.StartMemoryMonitoring()
	}

	logrus.Infof("Starting druid-exporter on port %s", *port)
	logrus.Fatal(http.ListenAndServe(":"+*port, router))
}
