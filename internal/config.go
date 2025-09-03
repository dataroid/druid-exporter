package internal

import (
	"github.com/patrickmn/go-cache"
)

// Config holds all configuration parameters for the druid endpoint
type Config struct {
	MetricsCleanupTTL int
	MaxRequestSizeMB  int
	DisableHistogram  bool
}

// Dependencies holds all external dependencies for the druid endpoint
type Dependencies struct {
	DNSCache        *cache.Cache
	InternalMetrics *InternalMetrics
}

// NewConfig creates a new configuration instance
func NewConfig(metricsCleanupTTL, maxRequestSizeMB int, disableHistogram bool) *Config {
	return &Config{
		MetricsCleanupTTL: metricsCleanupTTL,
		MaxRequestSizeMB:  maxRequestSizeMB,
		DisableHistogram:  disableHistogram,
	}
}

// NewDependencies creates a new dependencies instance
func NewDependencies(dnsCache *cache.Cache, internalMetrics *InternalMetrics) *Dependencies {
	return &Dependencies{
		DNSCache:        dnsCache,
		InternalMetrics: internalMetrics,
	}
}
