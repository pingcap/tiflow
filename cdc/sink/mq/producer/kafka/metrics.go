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
	"context"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
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
		}, []string{"namespace", "changefeed"})

	// meter mark by total records count
	recordSendRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_record_send_rate",
			Help:      "Records/second sent to all topics",
		}, []string{"namespace", "changefeed"})

	// records-per-request
	// histogram update by all records count.
	recordPerRequestGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_records_per_request",
			Help:      "the number of records sent per request for all topics",
		}, []string{"namespace", "changefeed"})

	// histogram update by `compression-ratio`.
	compressionRatioGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_compression_ratio",
			Help:      "the compression ratio times 100 of record batches for all topics",
		}, []string{"namespace", "changefeed"})

	// metrics for outgoing events
	// meter mark for each request's size in bytes
	outgoingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_outgoing_byte_rate",
			Help:      "Bytes/second written off all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// meter mark by 1 for each request
	requestRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_rate",
			Help:      "Requests/second sent to all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// meter mark for each request's size in bytes
	requestSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_size",
			Help:      "the request size in bytes for all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// histogram update for each received response, requestLatency := time.Since(response.requestTime)
	requestLatencyInMsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_latency",
			Help:      "the request latency in ms for all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// counter inc by 1 once a request send, dec by 1 for a response received.
	requestsInFlightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_in_flight_requests",
			Help:      "the current number of in-flight requests awaiting a response for all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// metrics for incoming events
	// meter mark for each received response's size in bytes
	incomingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_incoming_byte_rate",
			Help:      "Bytes/second read off all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// meter mark by 1 once a response received.
	responseRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_rate",
			Help:      "Responses/second received from all brokers",
		}, []string{"namespace", "changefeed", "broker"})

	// meter mark by each read response size
	responseSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_size",
			Help:      "the response size in bytes for all brokers",
		}, []string{"namespace", "changefeed", "broker"})
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
	changefeedID model.ChangeFeedID
	role         util.Role

	registry metrics.Registry
	admin    kafka.ClusterAdminClient

	brokers map[int32]struct{}
}

// collectMetrics collect all monitored metrics
func (sm *saramaMetricsMonitor) collectMetrics() {
	sm.collectProducerMetrics()
	sm.collectBrokerMetrics()
}

func (sm *saramaMetricsMonitor) collectProducerMetrics() {
	batchSizeMetric := sm.registry.Get(batchSizeMetricName)
	if histogram, ok := batchSizeMetric.(metrics.Histogram); ok {
		batchSizeGauge.
			WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID).
			Set(histogram.Snapshot().Mean())
	}

	recordSendRateMetric := sm.registry.Get(recordSendRateMetricName)
	if meter, ok := recordSendRateMetric.(metrics.Meter); ok {
		recordSendRateGauge.
			WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID).
			Set(meter.Snapshot().Rate1())
	}

	recordPerRequestMetric := sm.registry.Get(recordPerRequestMetricName)
	if histogram, ok := recordPerRequestMetric.(metrics.Histogram); ok {
		recordPerRequestGauge.
			WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID).
			Set(histogram.Snapshot().Mean())
	}

	compressionRatioMetric := sm.registry.Get(compressionRatioMetricName)
	if histogram, ok := compressionRatioMetric.(metrics.Histogram); ok {
		compressionRatioGauge.
			WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID).
			Set(histogram.Snapshot().Mean())
	}
}

func getBrokerMetricName(prefix, brokerID string) string {
	return prefix + brokerID
}

func (sm *saramaMetricsMonitor) collectBrokers() {
	start := time.Now()
	brokers, _, err := sm.admin.DescribeCluster()
	if err != nil {
		log.Warn("kafka cluster unreachable, "+
			"use historical brokers to collect kafka broker level metrics",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID),
			zap.Any("role", sm.role),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return
	}

	for _, b := range brokers {
		sm.brokers[b.ID()] = struct{}{}
	}
}

func (sm *saramaMetricsMonitor) collectBrokerMetrics() {
	for id := range sm.brokers {
		brokerID := strconv.Itoa(int(id))

		incomingByteRateMetric := sm.registry.Get(getBrokerMetricName(incomingByteRateMetricNamePrefix, brokerID))
		if meter, ok := incomingByteRateMetric.(metrics.Meter); ok {
			incomingByteRateGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		outgoingByteRateMetric := sm.registry.Get(getBrokerMetricName(outgoingByteRateMetricNamePrefix, brokerID))
		if meter, ok := outgoingByteRateMetric.(metrics.Meter); ok {
			outgoingByteRateGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		requestRateMetric := sm.registry.Get(getBrokerMetricName(requestRateMetricNamePrefix, brokerID))
		if meter, ok := requestRateMetric.(metrics.Meter); ok {
			requestRateGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		requestSizeMetric := sm.registry.Get(getBrokerMetricName(requestSizeMetricNamePrefix, brokerID))
		if histogram, ok := requestSizeMetric.(metrics.Histogram); ok {
			requestSizeGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(histogram.Snapshot().Mean())
		}

		requestLatencyMetric := sm.registry.Get(getBrokerMetricName(requestLatencyInMsMetricNamePrefix, brokerID))
		if histogram, ok := requestLatencyMetric.(metrics.Histogram); ok {
			requestLatencyInMsGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(histogram.Snapshot().Mean())
		}

		requestsInFlightMetric := sm.registry.Get(getBrokerMetricName(requestsInFlightMetricNamePrefix, brokerID))
		if counter, ok := requestsInFlightMetric.(metrics.Counter); ok {
			requestsInFlightGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(float64(counter.Snapshot().Count()))
		}

		responseRateMetric := sm.registry.Get(getBrokerMetricName(responseRateMetricNamePrefix, brokerID))
		if meter, ok := responseRateMetric.(metrics.Meter); ok {
			responseRateGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		responseSizeMetric := sm.registry.Get(getBrokerMetricName(responseSizeMetricNamePrefix, brokerID))
		if histogram, ok := responseSizeMetric.(metrics.Histogram); ok {
			responseSizeGauge.
				WithLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID).
				Set(histogram.Snapshot().Mean())
		}
	}
}

const (
	// flushMetricsInterval specifies the interval of refresh sarama metrics.
	flushMetricsInterval = 5 * time.Second
	// refreshClusterMetaInterval specifies the interval of refresh kafka cluster meta.
	// Do not set it too small, because it will cause too many requests to kafka cluster.
	// Every request will get all topics and all brokers information.
	refreshClusterMetaInterval = 30 * time.Minute
)

func runSaramaMetricsMonitor(ctx context.Context,
	registry metrics.Registry,
	changefeedID model.ChangeFeedID,
	role util.Role, admin kafka.ClusterAdminClient,
) {
	monitor := &saramaMetricsMonitor{
		changefeedID: changefeedID,
		role:         role,
		registry:     registry,
		admin:        admin,
		brokers:      make(map[int32]struct{}),
	}

	// Initialize brokers.
	monitor.collectBrokers()

	refreshMetricsTicker := time.NewTicker(flushMetricsInterval)
	refreshClusterMetaTicker := time.NewTicker(refreshClusterMetaInterval)
	go func() {
		defer func() {
			refreshMetricsTicker.Stop()
			refreshClusterMetaTicker.Stop()
			monitor.cleanup()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-refreshMetricsTicker.C:
				monitor.collectMetrics()
			case <-refreshClusterMetaTicker.C:
				monitor.collectBrokers()
			}
		}
	}()
}

// cleanup called when the changefeed / processor stop the kafka sink.
func (sm *saramaMetricsMonitor) cleanup() {
	sm.cleanUpProducerMetrics()
	sm.cleanUpBrokerMetrics()
}

func (sm *saramaMetricsMonitor) cleanUpProducerMetrics() {
	batchSizeGauge.
		DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID)
	recordSendRateGauge.
		DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID)
	recordPerRequestGauge.
		DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID)
	compressionRatioGauge.
		DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID)
}

func (sm *saramaMetricsMonitor) cleanUpBrokerMetrics() {
	for id := range sm.brokers {
		brokerID := strconv.Itoa(int(id))
		incomingByteRateGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		outgoingByteRateGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		requestRateGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		requestSizeGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		requestLatencyInMsGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		requestsInFlightGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		responseRateGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
		responseSizeGauge.
			DeleteLabelValues(sm.changefeedID.Namespace, sm.changefeedID.ID, brokerID)
	}
}
