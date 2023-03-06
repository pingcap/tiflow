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

package v2

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/segmentio/kafka-go"
)

// MetricsCollector is the kafka metrics collector based on kafka-go library.
type MetricsCollector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	admin        pkafka.ClusterAdminClient
	writer       *kafka.Writer
}

// NewMetricsCollector return a kafka metrics collector
func NewMetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	admin pkafka.ClusterAdminClient,
	writer *kafka.Writer,
) *MetricsCollector {
	return &MetricsCollector{
		changefeedID: changefeedID,
		role:         role,
		admin:        admin,
		writer:       writer,
	}
}

// flushMetricsInterval specifies the interval of refresh sarama metrics.
const flushMetricsInterval = 5 * time.Second

// Run implement the MetricsCollector interface
func (m *MetricsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(flushMetricsInterval)
	defer func() {
		ticker.Stop()
		m.cleanupMetrics()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.collectMetrics()
		}
	}
}

func (m *MetricsCollector) collectMetrics() {
	statistics := m.writer.Stats()

	// batch related metrics
	batchDurationHistogram.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Observe(statistics.BatchTime.Avg.Seconds())
	batchMessageCountHistogram.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Observe(float64(statistics.BatchSize.Avg))
	batchSizeHistogram.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Observe(float64(statistics.BatchBytes.Avg))

	// send batch related metrics
	requestRateGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Writes))
	requestLatencyInMsGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(statistics.WriteTime.Avg.Seconds())

	retryCount.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Retries))
	errCount.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Errors))
}

func (m *MetricsCollector) cleanupMetrics() {
	batchDurationHistogram.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	batchMessageCountHistogram.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	batchSizeHistogram.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)

	requestRateGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	requestLatencyInMsGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)

	retryCount.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	errCount.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
}
