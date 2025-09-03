package listener

import (
	"druid-exporter/internal"
	"druid-exporter/utils"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/gddo/httputil/header"
	"github.com/sirupsen/logrus"
)

// DruidHTTPEndpoint is the refactored endpoint to listen all druid metrics
func DruidHTTPEndpoint(config *internal.Config, deps *internal.Dependencies) http.HandlerFunc {
	// Create the request processor once during initialization
	processor := NewRequestProcessor(config, deps)
	requestCount := 0

	logrus.Infof("Druid HTTP endpoint initialized with %dMB request size limit", config.MaxRequestSizeMB)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		requestCount++

		// Extract client information
		sourceIP := strings.Split(req.RemoteAddr, ":")[0]
		dnsLookupValue := utils.ReverseDNSLookup(sourceIP, deps.DNSCache)

		// Track incoming request
		processor.TrackRequest(req.Method, "received", dnsLookupValue)

		// Validate request method and content type
		if !isValidRequest(w, req, processor, dnsLookupValue) {
			return
		}

		// Read and parse request body
		body, druidData, ok := readAndParseBody(w, req, config, processor, sourceIP, dnsLookupValue)
		if !ok {
			return
		}

		// Track request size and log large requests
		bodySize := len(body)
		processor.TrackRequestSize(dnsLookupValue, float64(bodySize))
		logRequestSize(sourceIP, dnsLookupValue, bodySize, len(druidData))

		// Validate we have metrics to process
		if len(druidData) == 0 {
			logrus.Warnf("No metrics received from %s (%s)", sourceIP, dnsLookupValue)
			processor.TrackError("no_metrics", dnsLookupValue)
			http.Error(w, "No metrics received", http.StatusBadRequest)
			return
		}

		// Process all metrics
		processedCount := processor.ProcessMetrics(druidData, sourceIP, dnsLookupValue)

		// Track successful request
		duration := time.Since(start).Seconds()
		processor.TrackRequestDuration(req.Method, "200", dnsLookupValue, duration)

		// Log request completion
		logrus.Debugf("Request #%d from %s (%s): processed %d/%d metrics in %v",
			requestCount, sourceIP, dnsLookupValue, processedCount, len(druidData), time.Since(start))

		// Send successful response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
}

// isValidRequest validates the HTTP request method and content type
func isValidRequest(w http.ResponseWriter, req *http.Request, processor *RequestProcessor, dnsLookup string) bool {
	reqHeader, _ := header.ParseValueAndParams(req.Header, "Content-Type")
	if req.Method != "POST" || reqHeader != "application/json" {
		processor.TrackError("invalid_request", dnsLookup)
		http.Error(w, "Only POST with application/json is supported", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// readAndParseBody reads and parses the request body
func readAndParseBody(w http.ResponseWriter, req *http.Request, config *internal.Config, processor *RequestProcessor, sourceIP, dnsLookup string) ([]byte, []map[string]interface{}, bool) {
	// Set request size limit
	req.Body = http.MaxBytesReader(w, req.Body, int64(config.MaxRequestSizeMB)*1024*1024)

	// Read body
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		handleReadError(w, err, config, processor, sourceIP, dnsLookup)
		return nil, nil, false
	}

	// Parse JSON
	var druidData []map[string]interface{}
	if err := json.Unmarshal(body, &druidData); err != nil {
		logrus.Errorf("Failed to parse JSON from %s (%s): %v", sourceIP, dnsLookup, err)
		processor.TrackError("json_parse_error", dnsLookup)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return nil, nil, false
	}

	return body, druidData, true
}

// handleReadError handles errors during body reading
func handleReadError(w http.ResponseWriter, err error, config *internal.Config, processor *RequestProcessor, sourceIP, dnsLookup string) {
	logrus.Errorf("Failed to read request body from %s (%s): %v", sourceIP, dnsLookup, err)

	if strings.Contains(err.Error(), "http: request body too large") {
		processor.TrackError("request_body_too_large", dnsLookup)
		logrus.Errorf("Request body too large from %s (%s). Current limit: %dMB. Consider increasing --max-request-size flag if this is expected.",
			sourceIP, dnsLookup, config.MaxRequestSizeMB)
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
	} else {
		processor.TrackError("body_read_error", dnsLookup)
		http.Error(w, "Bad request", http.StatusBadRequest)
	}
}

// logRequestSize logs information about request sizes
func logRequestSize(sourceIP, dnsLookup string, bodySize, metricCount int) {
	if bodySize > 1024*1024 { // 1MB
		logrus.Infof("Large request from %s (%s): %d bytes, %d metrics",
			sourceIP, dnsLookup, bodySize, metricCount)
	} else if bodySize > 100*1024 { // 100KB
		logrus.Debugf("Medium request from %s (%s): %d bytes, %d metrics",
			sourceIP, dnsLookup, bodySize, metricCount)
	}
}
