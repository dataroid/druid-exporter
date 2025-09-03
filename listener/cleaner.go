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
}

func newCleaner(gauge *prometheus.GaugeVec, minutes int) *cleaner {
	ctx, cancel := context.WithCancel(context.Background())

	c := cleaner{
		m:       &sync.Map{},
		gauge:   gauge,
		minutes: time.Duration(minutes),
		ctx:     ctx,
		cancel:  cancel,
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
		return
	}
	c.m.Store(string(bytes), time.Now())
}

func (c *cleaner) cleanup() {
	startTime := time.Now()
	beforeTime := time.Now().Add(-time.Minute * c.minutes)

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if context is cancelled
	if c.ctx.Err() != nil {
		return
	}

	deletedCount := 0
	totalCount := 0
	tobeDeletes := make([]interface{}, 0)

	c.m.Range(func(k, v interface{}) bool {
		totalCount++
		var labels prometheus.Labels
		err := json.Unmarshal([]byte(k.(string)), &labels)
		if err != nil {
			logrus.Errorf("unmarshal labels error: %v", err)
			// Still add to delete list to clean up corrupted entries
			tobeDeletes = append(tobeDeletes, k)
			return true
		}

		updatedAt, ok := v.(time.Time)
		if !ok {
			logrus.Errorf("invalid timestamp type in cleaner map")
			tobeDeletes = append(tobeDeletes, k)
			return true
		}

		if updatedAt.Before(beforeTime) {
			if err := c.gauge.Delete(labels); err {
				logrus.Debugf("Failed to delete gauge metric: %v", err)
			}
			tobeDeletes = append(tobeDeletes, k)
			deletedCount++
		}
		return true
	})

	// Delete from map
	for _, tobeDelete := range tobeDeletes {
		c.m.Delete(tobeDelete)
	}

	duration := time.Since(startTime)
	logrus.Debugf("Metrics cleanup completed: deleted %d/%d metrics in %v",
		deletedCount, totalCount, duration)
}
