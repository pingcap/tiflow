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
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

const (
	// flushMetricsInterval specifies the interval of refresh sarama metrics.
	flushMetricsInterval = 5 * time.Second
	// refreshClusterMetaInterval specifies the interval of refresh kafka cluster meta.
	// Do not set it too small, because it will cause too many requests to kafka cluster.
	// Every request will get all topics and all brokers information.
	refreshClusterMetaInterval = 30 * time.Minute
)

// Sarama metrics names, see https://pkg.go.dev/github.com/Shopify/sarama#pkg-overview.
const (
	// Producer level.
	compressionRatioMetricName = "compression-ratio"
	// Broker level.
	outgoingByteRateMetricNamePrefix   = "outgoing-byte-rate-for-broker-"
	requestRateMetricNamePrefix        = "request-rate-for-broker-"
	requestLatencyInMsMetricNamePrefix = "request-latency-in-ms-for-broker-"
	requestsInFlightMetricNamePrefix   = "requests-in-flight-for-broker-"
	responseRateMetricNamePrefix       = "response-rate-for-broker-"
)

// Collector is a metric collector for kafka.
type Collector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	// adminClient is used to get broker infos from broker.
	adminClient kafka.ClusterAdminClient
	brokers     map[int32]struct{}
	// TiCDC metrics registry.
	registry metrics.Registry
}

// New creates a new metric collector.
func New(
	changefeedID model.ChangeFeedID,
	role util.Role,
	adminClient kafka.ClusterAdminClient,
	registry metrics.Registry,
) *Collector {
	return &Collector{
		changefeedID: changefeedID,
		role:         role,
		adminClient:  adminClient,
		brokers:      make(map[int32]struct{}),
		registry:     registry,
	}
}

// Run collects kafka metrics.
// It will close the admin client when it's done.
func (m *Collector) Run(ctx context.Context) {
	// Initialize brokers.
	m.updateBrokers()

	refreshMetricsTicker := time.NewTicker(flushMetricsInterval)
	refreshClusterMetaTicker := time.NewTicker(refreshClusterMetaInterval)
	defer func() {
		refreshMetricsTicker.Stop()
		refreshClusterMetaTicker.Stop()
		m.cleanupMetrics()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-refreshMetricsTicker.C:
			m.collectBrokerMetrics()
			m.collectProducerMetrics()
		case <-refreshClusterMetaTicker.C:
			m.updateBrokers()
		}
	}
}

func (m *Collector) updateBrokers() {
	start := time.Now()
	brokers, _, err := m.adminClient.DescribeCluster()
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
		m.brokers[b.ID()] = struct{}{}
	}
}

func (m *Collector) collectProducerMetrics() {
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID

	compressionRatioMetric := m.registry.Get(compressionRatioMetricName)
	if histogram, ok := compressionRatioMetric.(metrics.Histogram); ok {
		compressionRatioGauge.
			WithLabelValues(namespace, changefeedID).
			Set(histogram.Snapshot().Mean())
	}
}

func (m *Collector) collectBrokerMetrics() {
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID
	for id := range m.brokers {
		brokerID := strconv.Itoa(int(id))

		outgoingByteRateMetric := m.registry.Get(
			getBrokerMetricName(outgoingByteRateMetricNamePrefix, brokerID))
		if meter, ok := outgoingByteRateMetric.(metrics.Meter); ok {
			outgoingByteRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		requestRateMetric := m.registry.Get(
			getBrokerMetricName(requestRateMetricNamePrefix, brokerID))
		if meter, ok := requestRateMetric.(metrics.Meter); ok {
			requestRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		requestLatencyMetric := m.registry.Get(
			getBrokerMetricName(requestLatencyInMsMetricNamePrefix, brokerID))
		if histogram, ok := requestLatencyMetric.(metrics.Histogram); ok {
			requestLatencyInMsGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(histogram.Snapshot().Mean())
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
				Set(meter.Snapshot().Rate1())
		}
	}
}

func getBrokerMetricName(prefix, brokerID string) string {
	return prefix + brokerID
}

func (m *Collector) cleanupProducerMetrics() {
	compressionRatioGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
}

func (m *Collector) cleanupBrokerMetrics() {
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID
	for id := range m.brokers {
		brokerID := strconv.Itoa(int(id))
		outgoingByteRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		requestRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		requestLatencyInMsGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		requestsInFlightGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		responseRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)

	}
}

func (m *Collector) cleanupMetrics() {
	m.cleanupProducerMetrics()
	m.cleanupBrokerMetrics()
}

// Close closes the admin client.
func (m *Collector) Close() {
	start := time.Now()
	namespace := m.changefeedID.Namespace
	changefeedID := m.changefeedID.ID
	if err := m.adminClient.Close(); err != nil {
		log.Warn("Close kafka cluster admin with error in "+
			"metric collector", zap.Error(err),
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", namespace),
			zap.String("changefeed", changefeedID), zap.Any("role", m.role))
	} else {
		log.Info("Kafka cluster admin closed in "+
			"metric collector", zap.Duration("duration", time.Since(start)),
			zap.String("namespace", namespace),
			zap.String("changefeed", changefeedID), zap.Any("role", m.role))
	}
}
