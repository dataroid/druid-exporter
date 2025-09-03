package listener

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

type cleaner struct {
	m       *sync.Map
	gauge   *prometheus.GaugeVec
	minutes time.Duration
	cron    *cron.Cron
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex // Add mutex for safe concurrent access
	// Monitoring metrics (shared across all cleaners with labels)
	cleanerID       string
	cleanupDuration *prometheus.HistogramVec
	cleanupErrors   *prometheus.CounterVec
	metricsTracked  *prometheus.GaugeVec
	metricsDeleted  *prometheus.CounterVec
	cleanupRuns     *prometheus.CounterVec
}

func newCleaner(gauge *prometheus.GaugeVec, minutes int, cleanerID string, cleanupDuration *prometheus.HistogramVec, cleanupErrors *prometheus.CounterVec, metricsTracked *prometheus.GaugeVec, metricsDeleted *prometheus.CounterVec, cleanupRuns *prometheus.CounterVec) *cleaner {
	ctx, cancel := context.WithCancel(context.Background())

	c := &cleaner{
		m:               &sync.Map{},
		gauge:           gauge,
		minutes:         time.Duration(minutes) * time.Minute,
		ctx:             ctx,
		cancel:          cancel,
		cleanerID:       cleanerID,
		cleanupDuration: cleanupDuration,
		cleanupErrors:   cleanupErrors,
		metricsTracked:  metricsTracked,
		metricsDeleted:  metricsDeleted,
		cleanupRuns:     cleanupRuns,
	}

	// Create and start cron job
	c.cron = cron.New()
	_, err := c.cron.AddFunc("@every 1m", c.cleanup)
	if err != nil {
		logrus.Errorf("Failed to create cleanup cron job for cleaner %s: %v", cleanerID, err)
		if c.cleanupErrors != nil {
			c.cleanupErrors.WithLabelValues(cleanerID, "cron_creation_error").Inc()
		}
		return c
	}

	c.cron.Start()
	logrus.Infof("Started metric cleaner %s with %d minute TTL", cleanerID, minutes)

	return c
}

func (c *cleaner) Stop() {
	if c.cron != nil {
		c.cron.Stop()
	}
	if c.cancel != nil {
		c.cancel()
	}
	logrus.Infof("Stopped metric cleaner %s", c.cleanerID)
}

func (c *cleaner) add(labels prometheus.Labels) {
	// Store the metric with current timestamp
	metricData := map[string]interface{}{
		"labels":    labels,
		"timestamp": time.Now(),
		"cleanerID": c.cleanerID,
	}

	// Convert to JSON for storage key
	key, err := json.Marshal(labels)
	if err != nil {
		logrus.Errorf("Failed to marshal labels for cleaner %s: %v", c.cleanerID, err)
		if c.cleanupErrors != nil {
			c.cleanupErrors.WithLabelValues(c.cleanerID, "marshal_error").Inc()
		}
		return
	}

	c.m.Store(string(key), metricData)

	// Update metrics tracked count in a goroutine to avoid blocking
	go func() {
		if c.metricsTracked != nil {
			count := 0
			c.m.Range(func(_, _ interface{}) bool {
				count++
				return true
			})
			c.metricsTracked.WithLabelValues(c.cleanerID).Set(float64(count))
		}
	}()
}

func (c *cleaner) cleanup() {
	if c.gauge == nil {
		return
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		if c.cleanupDuration != nil {
			c.cleanupDuration.WithLabelValues(c.cleanerID).Observe(duration)
		}
		if c.cleanupRuns != nil {
			c.cleanupRuns.WithLabelValues(c.cleanerID).Inc()
		}
	}()

	c.mu.Lock() // Use write lock for deletion
	defer c.mu.Unlock()

	now := time.Now()
	totalCount := 0
	deletedCount := 0
	errorCount := 0
	gaugeDeleteSuccessCount := 0
	gaugeDeleteFailCount := 0

	c.m.Range(func(key, value interface{}) bool {
		totalCount++

		metricData, ok := value.(map[string]interface{})
		if !ok {
			errorCount++
			logrus.Errorf("Invalid metric data type in cleaner %s", c.cleanerID)
			return true
		}

		timestamp, ok := metricData["timestamp"].(time.Time)
		if !ok {
			errorCount++
			logrus.Errorf("Invalid timestamp in metric data for cleaner %s", c.cleanerID)
			return true
		}

		// Check if metric is older than TTL
		if now.Sub(timestamp) > c.minutes {
			labels, ok := metricData["labels"].(prometheus.Labels)
			if !ok {
				errorCount++
				logrus.Errorf("Invalid labels in metric data for cleaner %s", c.cleanerID)
				return true
			}

			// Delete from Prometheus gauge
			deleteSuccess := c.gauge.Delete(labels)
			if deleteSuccess {
				gaugeDeleteSuccessCount++
			} else {
				gaugeDeleteFailCount++
			}

			// Delete from our tracking map
			c.m.Delete(key)
			deletedCount++

			logrus.Debugf("Cleaned up expired metric in cleaner %s: %v (age: %v)",
				c.cleanerID, labels, now.Sub(timestamp))
		}
		return true
	})

	// Update metrics
	if c.metricsDeleted != nil {
		c.metricsDeleted.WithLabelValues(c.cleanerID).Add(float64(deletedCount))
	}
	if c.metricsTracked != nil {
		c.metricsTracked.WithLabelValues(c.cleanerID).Set(float64(totalCount - deletedCount))
	}
	if c.cleanupErrors != nil && errorCount > 0 {
		c.cleanupErrors.WithLabelValues(c.cleanerID, "data_processing_error").Add(float64(errorCount))
	}
	if c.cleanupErrors != nil && gaugeDeleteFailCount > 0 {
		c.cleanupErrors.WithLabelValues(c.cleanerID, "gauge_delete_error").Add(float64(gaugeDeleteFailCount))
	}

	// Log cleanup summary
	if deletedCount > 0 || errorCount > 0 {
		logrus.Infof("Cleanup completed for cleaner %s: %d total, %d deleted, %d errors, %d gauge_deletes_success, %d gauge_deletes_fail",
			c.cleanerID, totalCount, deletedCount, errorCount, gaugeDeleteSuccessCount, gaugeDeleteFailCount)
	} else {
		logrus.Debugf("Cleanup completed for cleaner %s: %d metrics tracked, no deletions needed",
			c.cleanerID, totalCount)
	}
}
