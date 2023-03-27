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
)

// MetricsCollector is the kafka metrics collector based on kafka-go library.
type MetricsCollector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	writer       Writer
}

// NewMetricsCollector return a kafka metrics collector
func NewMetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	writer Writer,
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
	pkafka.BatchDurationGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(statistics.BatchTime.Avg.Seconds())
	pkafka.BatchMessageCountGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.BatchSize.Avg))
	pkafka.BatchSizeGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.BatchBytes.Avg))

	// send request related metrics.
	// metrics is collected each 5 seconds, divide by 5 to get per seconds average.
	// since kafka-go does not support per broker metrics, so we add `v2` as the broker ID.
	pkafka.RequestRateGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, "v2").
		Set(float64(statistics.Writes / 5))
	pkafka.RequestLatencyGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, "v2").
		Set(statistics.WriteTime.Avg.Seconds())
	pkafka.OutgoingByteRateGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, "v2").
		Set(float64(statistics.Bytes / 5))

	pkafka.ClientRetryGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Retries))
	pkafka.ClientErrorGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID).
		Set(float64(statistics.Errors))
}

func (m *MetricsCollector) cleanupMetrics() {
	pkafka.BatchDurationGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.BatchMessageCountGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.BatchSizeGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)

	pkafka.RequestRateGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, "v2")
	pkafka.RequestLatencyGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, "v2")
	pkafka.OutgoingByteRateGauge.
		DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, "v2")

	pkafka.ClientRetryGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	pkafka.ClientErrorGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
}
