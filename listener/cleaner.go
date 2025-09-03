package listener

import (
	"context"
	"encoding/json"
	"fmt"
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
	// Monitoring metrics
	cleanupDuration prometheus.Histogram
	cleanupErrors   prometheus.Counter
	metricsTracked  prometheus.Gauge
	metricsDeleted  prometheus.Counter
	cleanupRuns     prometheus.Counter
}

func newCleaner(gauge *prometheus.GaugeVec, minutes int) *cleaner {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize cleaner monitoring metrics
	cleanupDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "druid_exporter_cleaner_duration_seconds",
		Help:    "Duration of cleanup operations",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0},
	})

	cleanupErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "druid_exporter_cleaner_errors_total",
		Help: "Total number of cleanup errors",
	})

	metricsTracked := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "druid_exporter_cleaner_metrics_tracked",
		Help: "Current number of metrics being tracked by cleaner",
	})

	metricsDeleted := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "druid_exporter_cleaner_metrics_deleted_total",
		Help: "Total number of metrics deleted by cleaner",
	})

	cleanupRuns := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "druid_exporter_cleaner_runs_total",
		Help: "Total number of cleanup runs",
	})

	// Register metrics
	prometheus.MustRegister(cleanupDuration)
	prometheus.MustRegister(cleanupErrors)
	prometheus.MustRegister(metricsTracked)
	prometheus.MustRegister(metricsDeleted)
	prometheus.MustRegister(cleanupRuns)

	c := cleaner{
		m:               &sync.Map{},
		gauge:           gauge,
		minutes:         time.Duration(minutes),
		ctx:             ctx,
		cancel:          cancel,
		cleanupDuration: cleanupDuration,
		cleanupErrors:   cleanupErrors,
		metricsTracked:  metricsTracked,
		metricsDeleted:  metricsDeleted,
		cleanupRuns:     cleanupRuns,
	}

	c.cron = cron.New()
	_, err := c.cron.AddFunc(fmt.Sprintf("@every %dm", minutes), func() {
		if c.ctx.Err() != nil {
			return // Context cancelled, don't run cleanup
		}
		c.cleanup()
	})
	if err != nil {
		logrus.Errorf("Failed to add cleanup job to cron: %v", err)
	}
	c.cron.Start()
	logrus.Infof("Started metrics cleaner with %d minute TTL", minutes)
	return &c
}

// Stop gracefully stops the cleaner and cleans up resources
func (c *cleaner) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	if c.cron != nil {
		c.cron.Stop()
	}
	logrus.Info("Stopped metrics cleaner")
}

func (c *cleaner) add(labels prometheus.Labels) {
	bytes, err := json.Marshal(labels)
	if err != nil {
		logrus.Errorf("marshal labels error: %v", err)
		c.cleanupErrors.Inc()
		return
	}

	// Store with current timestamp
	c.m.Store(string(bytes), time.Now())

	// Update metrics tracked count (approximate, but better than nothing)
	go func() {
		count := 0
		c.m.Range(func(k, v interface{}) bool {
			count++
			return true
		})
		c.metricsTracked.Set(float64(count))
	}()
}

func (c *cleaner) cleanup() {
	startTime := time.Now()
	defer func() {
		c.cleanupDuration.Observe(time.Since(startTime).Seconds())
		c.cleanupRuns.Inc()
	}()

	// Calculate cutoff time - metrics older than this will be deleted
	beforeTime := time.Now().Add(-time.Minute * c.minutes)

	// Use write lock since we'll be modifying the map
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if context is cancelled
	if c.ctx.Err() != nil {
		return
	}

	deletedCount := 0
	totalCount := 0
	errorCount := 0
	gaugeDeleteSuccessCount := 0
	gaugeDeleteFailCount := 0
	tobeDeletes := make([]interface{}, 0)

	// First pass: identify metrics to delete
	c.m.Range(func(k, v interface{}) bool {
		totalCount++
		var labels prometheus.Labels
		err := json.Unmarshal([]byte(k.(string)), &labels)
		if err != nil {
			logrus.Errorf("unmarshal labels error: %v", err)
			c.cleanupErrors.Inc()
			errorCount++
			// Still add to delete list to clean up corrupted entries
			tobeDeletes = append(tobeDeletes, k)
			return true
		}

		updatedAt, ok := v.(time.Time)
		if !ok {
			logrus.Errorf("invalid timestamp type in cleaner map - expected time.Time, got %T", v)
			c.cleanupErrors.Inc()
			errorCount++
			tobeDeletes = append(tobeDeletes, k)
			return true
		}

		// Check if metric is expired
		if updatedAt.Before(beforeTime) {
			// Try to delete from Prometheus gauge
			deleted := c.gauge.Delete(labels)
			if deleted {
				gaugeDeleteSuccessCount++
				logrus.Tracef("Successfully deleted gauge metric: %v", labels)
			} else {
				gaugeDeleteFailCount++
				logrus.Debugf("Gauge metric not found for deletion (may be normal): %v", labels)
			}

			tobeDeletes = append(tobeDeletes, k)
			deletedCount++
		}
		return true
	})

	// Second pass: delete from tracking map
	for _, tobeDelete := range tobeDeletes {
		c.m.Delete(tobeDelete)
	}

	// Update metrics
	c.metricsDeleted.Add(float64(deletedCount))
	c.metricsTracked.Set(float64(totalCount - len(tobeDeletes)))

	// Log cleanup summary
	duration := time.Since(startTime)
	if deletedCount > 0 || errorCount > 0 {
		logrus.Infof("Cleanup completed: deleted %d/%d metrics (%d errors, %d gauge successes, %d gauge misses) in %v",
			deletedCount, totalCount, errorCount, gaugeDeleteSuccessCount, gaugeDeleteFailCount, duration)
	} else {
		logrus.Debugf("Cleanup completed: no metrics to delete from %d total in %v", totalCount, duration)
	}

	// Log detailed stats at trace level
	logrus.Tracef("Cleanup details: total=%d, expired=%d, errors=%d, gauge_deleted=%d, gauge_missing=%d, duration=%v",
		totalCount, deletedCount, errorCount, gaugeDeleteSuccessCount, gaugeDeleteFailCount, duration)
}
