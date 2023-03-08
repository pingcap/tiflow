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

	"github.com/labstack/gommon/log"
	"github.com/pingcap/tiflow/cdc/model"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// MetricsCollector is the kafka metrics collector based on kafka-go library.
type MetricsCollector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	writer       *kafka.Writer
}

// NewMetricsCollector return a kafka metrics collector
func NewMetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	writer *kafka.Writer,
) *MetricsCollector {
	return &MetricsCollector{
		changefeedID: changefeedID,
		role:         role,
		writer:       writer,
	}
}

// Run implement the MetricsCollector interface
func (m *MetricsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(pkafka.RefreshMetricsInterval)
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
	pkafka.BatchDurationHistogram.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Observe(statistics.BatchTime.Avg.Seconds())
	pkafka.BatchMessageCountHistogram.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Observe(float64(statistics.BatchSize.Avg))
	pkafka.BatchSizeHistogram.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Observe(float64(statistics.BatchBytes.Avg))

	// send batch related metrics
	pkafka.RequestRateGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Writes))
	pkafka.RequestLatencyInMsGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(statistics.WriteTime.Avg.Seconds())

	pkafka.RetryCount.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Retries))
	pkafka.ErrorCount.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Errors))
}

func (m *MetricsCollector) cleanupMetrics() {
	pkafka.BatchDurationHistogram.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.BatchMessageCountHistogram.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.BatchSizeHistogram.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)

	pkafka.RequestRateGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.RequestLatencyInMsGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)

	pkafka.RetryCount.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.ErrorCount.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)

	log.Info("metrics collector clean up all metrics",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID))
}
