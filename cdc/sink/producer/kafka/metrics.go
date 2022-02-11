// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
)

var (
	// producer metrics
	// histogram update by each request batch
	batchSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "the number of bytes sent per partition per request for all topics",
		}, []string{"capture", "changefeed"})

	// record-send-rate
	// meter mark by total records count
	recordSendRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_record_send_rate",
			Help:      "Records/second sent to all topics",
		}, []string{"capture", "changefeed"})

	// records-per-request
	// histogram update by all records count.
	recordPerRequestGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_records_per_request",
			Help:      "the number of records sent per request for all topics",
		}, []string{"capture", "changefeed"})

	// compression-ratio
	compressionRatioGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_compression_ratio",
			Help:      "the compression ratio times 100 of record batches for all topics",
		}, []string{"capture", "changefeed"})

	// meter mark for each received response's size in bytes
	incomingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_incoming_byte_rate",
			Help:      "Bytes/second read off all brokers",
		}, []string{"capture", "changefeed"})

	// meter mark for each request's size in bytes
	outgoingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_outgoing_byte_rate",
			Help:      "Bytes/second written off all brokers",
		}, []string{"capture", "changefeed"})

	// meter mark by 1 for each request
	requestRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_rate",
			Help:      "Requests/second sent to all brokers",
		}, []string{"capture", "changefeed"})

	// meter mark for each request's size in bytes
	requestSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_size",
			Help:      "the request size in bytes for all brokers",
		}, []string{"capture", "changefeed"})

	// histogram update for each received response, requestLatency := time.Since(response.requestTime)
	requestLatencyInMsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_latency",
			Help:      "the request latency in ms for all brokers",
		}, []string{"capture", "changefeed"})

	// counter inc by 1 once a request send, dec by 1 for a response received.
	requestsInFlightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_in_flight_requests",
			Help:      "the current number of in-flight requests awaiting a response for all brokers",
		}, []string{"capture", "changefeed"})

	// meter mark by 1 once a response received.
	responseRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_rate",
			Help:      "Responses/second received from all brokers",
		}, []string{"capture", "changefeed"})

	// meter mark by each read response size
	responseSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_size",
			Help:      "the response size in bytes for all brokers",
		}, []string{"capture", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchSizeGauge)
	registry.MustRegister(recordSendRateGauge)
	registry.MustRegister(recordPerRequestGauge)
	registry.MustRegister(compressionRatioGauge)

	registry.MustRegister(incomingByteRateGauge)
	registry.MustRegister(outgoingByteRateGauge)
	registry.MustRegister(requestSizeGauge)
	registry.MustRegister(requestRateGauge)
	registry.MustRegister(requestLatencyInMsGauge)
	registry.MustRegister(requestsInFlightGauge)
	registry.MustRegister(responseSizeGauge)
	registry.MustRegister(responseRateGauge)
}

// sarama metrics names, see https://pkg.go.dev/github.com/Shopify/sarama#pkg-overview
const (
	// producer metrics
	batchSizeMetricName        = "batch-size"
	recordSendRateMetricName   = "record-send-rate"
	recordPerRequestMetricName = "records-per-request"
	compressionRatioMetricName = "compression-ratio"

	// broker metrics
	incomingByteRateMetricName   = "incoming-byte-rate"
	outgoingByteRateMetricName   = "outgoing-byte-rate"
	requestRateMetricName        = "request-rate"
	requestSizeMetricName        = "request-size"
	requestLatencyInMsMetricName = "request-latency-in-ms"
	requestsInFlightMetricName   = "requests-in-flight"
	responseRateMetricName       = "response-rate"
	responseSizeMetricName       = "response-size"
)

type saramaMetricsMonitor struct {
	captureAddr  string
	changefeedID string

	registry metrics.Registry
}

// CollectMetrics collect all monitored metrics
func (sm *saramaMetricsMonitor) CollectMetrics() {
	sm.collectProducerMetrics()
	sm.collectBrokerMetrics()
}

func (sm *saramaMetricsMonitor) collectProducerMetrics() {
	batchSizeMetric := sm.registry.Get(batchSizeMetricName)
	if histogram, ok := batchSizeMetric.(metrics.Histogram); ok {
		batchSizeGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Snapshot().Mean())
	}

	recordSendRateMetric := sm.registry.Get(recordSendRateMetricName)
	if meter, ok := recordSendRateMetric.(metrics.Meter); ok {
		recordSendRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(meter.Snapshot().Rate1())
	}

	recordPerRequestMetric := sm.registry.Get(recordPerRequestMetricName)
	if histogram, ok := recordPerRequestMetric.(metrics.Histogram); ok {
		recordPerRequestGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Snapshot().Mean())
	}

	compressionRatioMetric := sm.registry.Get(compressionRatioMetricName)
	if histogram, ok := compressionRatioMetric.(metrics.Histogram); ok {
		compressionRatioGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Snapshot().Mean())
	}
}

func (sm *saramaMetricsMonitor) collectBrokerMetrics() {
	incomingByteRateMetric := sm.registry.Get(incomingByteRateMetricName)
	if meter, ok := incomingByteRateMetric.(metrics.Meter); ok {
		incomingByteRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(meter.Snapshot().Rate1())
	}

	outgoingByteRateMetric := sm.registry.Get(outgoingByteRateMetricName)
	if meter, ok := outgoingByteRateMetric.(metrics.Meter); ok {
		outgoingByteRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(meter.Snapshot().Rate1())
	}

	requestRateMetric := sm.registry.Get(requestRateMetricName)
	if meter, ok := requestRateMetric.(metrics.Meter); ok {
		requestRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(meter.Snapshot().Rate1())
	}

	requestSizeMetric := sm.registry.Get(requestSizeMetricName)
	if histogram, ok := requestSizeMetric.(metrics.Histogram); ok {
		requestSizeGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Snapshot().Mean())
	}

	requestLatencyMetric := sm.registry.Get(requestLatencyInMsMetricName)
	if histogram, ok := requestLatencyMetric.(metrics.Histogram); ok {
		requestLatencyInMsGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Snapshot().Mean())
	}

	requestsInFlightMetric := sm.registry.Get(requestsInFlightMetricName)
	if counter, ok := requestsInFlightMetric.(metrics.Counter); ok {
		requestsInFlightGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(float64(counter.Snapshot().Count()))
	}

	responseRateMetric := sm.registry.Get(responseRateMetricName)
	if meter, ok := responseRateMetric.(metrics.Meter); ok {
		responseRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(meter.Snapshot().Rate1())
	}

	responseSizeMetric := sm.registry.Get(responseSizeMetricName)
	if histogram, ok := responseSizeMetric.(metrics.Histogram); ok {
		responseSizeGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Snapshot().Mean())
	}
}

func NewSaramaMetricsMonitor(registry metrics.Registry, captureAddr, changefeedID string) *saramaMetricsMonitor {
	return &saramaMetricsMonitor{
		captureAddr:  captureAddr,
		changefeedID: changefeedID,
		registry:     registry,
	}
}

func (sm *saramaMetricsMonitor) Cleanup() {
	batchSizeGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	recordSendRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	recordPerRequestGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	compressionRatioGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)

	incomingByteRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	outgoingByteRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	requestRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	requestSizeGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	requestLatencyInMsGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	requestsInFlightGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	responseRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	responseSizeGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
}
