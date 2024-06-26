// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

// MetricsCollector is the interface for kafka metrics collector.
type MetricsCollector interface {
	Run(ctx context.Context)
}

const (
	// RefreshMetricsInterval specifies the interval of refresh kafka client metrics.
	RefreshMetricsInterval = 5 * time.Second
	// refreshClusterMetaInterval specifies the interval of refresh kafka cluster meta.
	// Do not set it too small, because it will cause too many requests to kafka cluster.
	// Every request will get all topics and all brokers information.
	refreshClusterMetaInterval = 30 * time.Minute
)

// Sarama metrics names, see https://pkg.go.dev/github.com/IBM/sarama#pkg-overview.
const (
	// Producer level.
	compressionRatioMetricName  = "compression-ratio"
	recordsPerRequestMetricName = "records-per-request"

	// Broker level.
	outgoingByteRateMetricNamePrefix   = "outgoing-byte-rate-for-broker-"
	requestRateMetricNamePrefix        = "request-rate-for-broker-"
	requestLatencyInMsMetricNamePrefix = "request-latency-in-ms-for-broker-"
	requestsInFlightMetricNamePrefix   = "requests-in-flight-for-broker-"
	responseRateMetricNamePrefix       = "response-rate-for-broker-"
)

type saramaMetricsCollector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	// adminClient is used to get broker infos from broker.
	adminClient ClusterAdminClient
	brokers     map[int32]struct{}
	registry    metrics.Registry
}

// NewSaramaMetricsCollector return a kafka metrics collector based on sarama library.
func NewSaramaMetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	adminClient ClusterAdminClient,
	registry metrics.Registry,
) MetricsCollector {
	return &saramaMetricsCollector{
		changefeedID: changefeedID,
		role:         role,
		adminClient:  adminClient,
		brokers:      make(map[int32]struct{}),
		registry:     registry,
	}
}

func (m *saramaMetricsCollector) Run(ctx context.Context) {
	// Initialize brokers.
	m.updateBrokers(ctx)

	refreshMetricsTicker := time.NewTicker(RefreshMetricsInterval)
	refreshClusterMetaTicker := time.NewTicker(refreshClusterMetaInterval)
	defer func() {
		refreshMetricsTicker.Stop()
		refreshClusterMetaTicker.Stop()
		m.cleanupMetrics()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("Kafka metrics collector stopped",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID))
			return
		case <-refreshMetricsTicker.C:
			m.collectBrokerMetrics()
			m.collectProducerMetrics()
		case <-refreshClusterMetaTicker.C:
			m.updateBrokers(ctx)
		}
	}
}

func (m *saramaMetricsCollector) updateBrokers(ctx context.Context) {
	start := time.Now()
	brokers, err := m.adminClient.GetAllBrokers(ctx)
	if err != nil {
		log.Warn("Get Kafka brokers failed, "+
			"use historical brokers to collect kafka broker level metrics",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Any("role", m.role),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return
	}

	for _, b := range brokers {
		m.brokers[b.ID] = struct{}{}
	}
}

func (m *saramaMetricsCollector) collectProducerMetrics() {
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID

	compressionRatioMetric := m.registry.Get(compressionRatioMetricName)
	if histogram, ok := compressionRatioMetric.(metrics.Histogram); ok {
		compressionRatioGauge.
			WithLabelValues(namespace, changefeedID).
			Set(histogram.Snapshot().Mean())
	}

	recordsPerRequestMetric := m.registry.Get(recordsPerRequestMetricName)
	if histogram, ok := recordsPerRequestMetric.(metrics.Histogram); ok {
		recordsPerRequestGauge.
			WithLabelValues(namespace, changefeedID).
			Set(histogram.Snapshot().Mean())
	}
}

func (m *saramaMetricsCollector) collectBrokerMetrics() {
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID
	for id := range m.brokers {
		brokerID := strconv.Itoa(int(id))

		outgoingByteRateMetric := m.registry.Get(
			getBrokerMetricName(outgoingByteRateMetricNamePrefix, brokerID))
		if meter, ok := outgoingByteRateMetric.(metrics.Meter); ok {
			OutgoingByteRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().RateMean())
		}

		requestRateMetric := m.registry.Get(
			getBrokerMetricName(requestRateMetricNamePrefix, brokerID))
		if meter, ok := requestRateMetric.(metrics.Meter); ok {
			RequestRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().RateMean())
		}

		requestLatencyMetric := m.registry.Get(
			getBrokerMetricName(requestLatencyInMsMetricNamePrefix, brokerID))
		if histogram, ok := requestLatencyMetric.(metrics.Histogram); ok {
			RequestLatencyGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(histogram.Snapshot().Mean() / 1000) // convert millisecond to second.
		}

		requestsInFlightMetric := m.registry.Get(getBrokerMetricName(
			requestsInFlightMetricNamePrefix, brokerID))
		if counter, ok := requestsInFlightMetric.(metrics.Counter); ok {
			requestsInFlightGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(float64(counter.Snapshot().Count()))
		}

		responseRateMetric := m.registry.Get(getBrokerMetricName(
			responseRateMetricNamePrefix, brokerID))
		if meter, ok := responseRateMetric.(metrics.Meter); ok {
			responseRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().RateMean())
		}
	}
}

func getBrokerMetricName(prefix, brokerID string) string {
	return prefix + brokerID
}

func (m *saramaMetricsCollector) cleanupProducerMetrics() {
	compressionRatioGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	recordsPerRequestGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
}

func (m *saramaMetricsCollector) cleanupBrokerMetrics() {
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID
	for id := range m.brokers {
		brokerID := strconv.Itoa(int(id))
		OutgoingByteRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		RequestRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		RequestLatencyGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		requestsInFlightGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		responseRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)

	}
}

func (m *saramaMetricsCollector) cleanupMetrics() {
	m.cleanupProducerMetrics()
	m.cleanupBrokerMetrics()
}
