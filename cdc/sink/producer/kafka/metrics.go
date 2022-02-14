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
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

var (
	// Histogram update by the `batch-size`
	batchSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "the number of bytes sent per partition per request for all topics",
		}, []string{"capture", "changefeed"})

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

	// histogram update by `compression-ratio`.
	compressionRatioGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_compression_ratio",
			Help:      "the compression ratio times 100 of record batches for all topics",
		}, []string{"capture", "changefeed"})

	// metrics for outgoing events
	// meter mark for each request's size in bytes
	outgoingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_outgoing_byte_rate",
			Help:      "Bytes/second written off all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// meter mark by 1 for each request
	requestRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_rate",
			Help:      "Requests/second sent to all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// meter mark for each request's size in bytes
	requestSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_size",
			Help:      "the request size in bytes for all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// histogram update for each received response, requestLatency := time.Since(response.requestTime)
	requestLatencyInMsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_latency",
			Help:      "the request latency in ms for all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// counter inc by 1 once a request send, dec by 1 for a response received.
	requestsInFlightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_in_flight_requests",
			Help:      "the current number of in-flight requests awaiting a response for all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// metrics for incoming events
	// meter mark for each received response's size in bytes
	incomingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_incoming_byte_rate",
			Help:      "Bytes/second read off all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// meter mark by 1 once a response received.
	responseRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_rate",
			Help:      "Responses/second received from all brokers",
		}, []string{"capture", "changefeed", "broker"})

	// meter mark by each read response size
	responseSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_size",
			Help:      "the response size in bytes for all brokers",
		}, []string{"capture", "changefeed", "broker"})
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
	// metrics at producer level.
	batchSizeMetricName        = "batch-size"
	recordSendRateMetricName   = "record-send-rate"
	recordPerRequestMetricName = "records-per-request"
	compressionRatioMetricName = "compression-ratio"

	// metrics at broker level.
	incomingByteRateMetricNamePrefix   = "incoming-byte-rate-for-broker-"
	outgoingByteRateMetricNamePrefix   = "outgoing-byte-rate-for-broker-"
	requestRateMetricNamePrefix        = "request-rate-for-broker-"
	requestSizeMetricNamePrefix        = "request-size-for-broker-"
	requestLatencyInMsMetricNamePrefix = "request-latency-in-ms-for-broker-"
	requestsInFlightMetricNamePrefix   = "requests-in-flight-for-broker-"
	responseRateMetricNamePrefix       = "response-rate-for-broker-"
	responseSizeMetricNamePrefix       = "response-size-for-broker-"
)

type saramaMetricsMonitor struct {
	captureAddr  string
	changefeedID string

	registry metrics.Registry
	admin    kafka.ClusterAdminClient
}

// CollectMetrics collect all monitored metrics
func (sm *saramaMetricsMonitor) CollectMetrics() {
	sm.collectProducerMetrics()
	if err := sm.collectBrokerMetrics(); err != nil {
		log.Warn("collect broker metrics failed", zap.Error(err))
	}
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

func getBrokerMetricName(prefix, brokerID string) string {
	return prefix + brokerID
}

func (sm *saramaMetricsMonitor) collectBrokerMetrics() error {
	brokers, _, err := sm.admin.DescribeCluster()
	if err != nil {
		return err
	}

	for _, b := range brokers {
		brokerID := strconv.Itoa(int(b.ID()))

		incomingByteRateMetric := sm.registry.Get(getBrokerMetricName(incomingByteRateMetricNamePrefix, brokerID))
		if meter, ok := incomingByteRateMetric.(metrics.Meter); ok {
			incomingByteRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(meter.Snapshot().Rate1())
		}

		outgoingByteRateMetric := sm.registry.Get(getBrokerMetricName(outgoingByteRateMetricNamePrefix, brokerID))
		if meter, ok := outgoingByteRateMetric.(metrics.Meter); ok {
			outgoingByteRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(meter.Snapshot().Rate1())
		}

		requestRateMetric := sm.registry.Get(getBrokerMetricName(requestRateMetricNamePrefix, brokerID))
		if meter, ok := requestRateMetric.(metrics.Meter); ok {
			requestRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(meter.Snapshot().Rate1())
		}

		requestSizeMetric := sm.registry.Get(getBrokerMetricName(requestSizeMetricNamePrefix, brokerID))
		if histogram, ok := requestSizeMetric.(metrics.Histogram); ok {
			requestSizeGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(histogram.Snapshot().Mean())
		}

		requestLatencyMetric := sm.registry.Get(getBrokerMetricName(requestLatencyInMsMetricNamePrefix, brokerID))
		if histogram, ok := requestLatencyMetric.(metrics.Histogram); ok {
			requestLatencyInMsGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(histogram.Snapshot().Mean())
		}

		requestsInFlightMetric := sm.registry.Get(getBrokerMetricName(requestsInFlightMetricNamePrefix, brokerID))
		if counter, ok := requestsInFlightMetric.(metrics.Counter); ok {
			requestsInFlightGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(float64(counter.Snapshot().Count()))
		}

		responseRateMetric := sm.registry.Get(getBrokerMetricName(responseRateMetricNamePrefix, brokerID))
		if meter, ok := responseRateMetric.(metrics.Meter); ok {
			responseRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(meter.Snapshot().Rate1())
		}

		responseSizeMetric := sm.registry.Get(getBrokerMetricName(responseSizeMetricNamePrefix, brokerID))
		if histogram, ok := responseSizeMetric.(metrics.Histogram); ok {
			responseSizeGauge.WithLabelValues(sm.captureAddr, sm.changefeedID, brokerID).Set(histogram.Snapshot().Mean())
		}
	}
	return nil
}

func NewSaramaMetricsMonitor(registry metrics.Registry, captureAddr, changefeedID string, admin kafka.ClusterAdminClient) *saramaMetricsMonitor {
	return &saramaMetricsMonitor{
		captureAddr:  captureAddr,
		changefeedID: changefeedID,
		registry:     registry,
		admin:        admin,
	}
}

func (sm *saramaMetricsMonitor) Cleanup() {
	sm.cleanUpProducerMetrics()
	if err := sm.cleanUpBrokerMetrics(); err != nil {
		log.Warn("clean up broker metrics failed", zap.Error(err))
	}
}

func (sm *saramaMetricsMonitor) cleanUpProducerMetrics() {
	batchSizeGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	recordSendRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	recordPerRequestGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
	compressionRatioGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID)
}

func (sm *saramaMetricsMonitor) cleanUpBrokerMetrics() error {
	brokers, _, err := sm.admin.DescribeCluster()
	if err != nil {
		return err
	}

	for _, b := range brokers {
		brokerID := strconv.Itoa(int(b.ID()))

		incomingByteRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		outgoingByteRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		requestRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		requestSizeGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		requestLatencyInMsGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		requestsInFlightGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		responseRateGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
		responseSizeGauge.DeleteLabelValues(sm.captureAddr, sm.changefeedID, brokerID)
	}
	return nil
}
