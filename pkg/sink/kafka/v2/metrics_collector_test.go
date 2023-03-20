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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/model"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	mock "github.com/pingcap/tiflow/pkg/sink/kafka/v2/mock"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestMetricsCollector(t *testing.T) {
	changefeedID := model.DefaultChangeFeedID("test")
	mockWriter := mock.NewMockWriter(gomock.NewController(t))
	c := NewMetricsCollector(changefeedID, util.RoleOwner, mockWriter)
	mockWriter.EXPECT().Stats().AnyTimes().Return(kafka.WriterStats{})
	cases := []struct {
		labelValues []string
		gauge       *prometheus.GaugeVec
	}{
		{[]string{changefeedID.Namespace, changefeedID.ID}, pkafka.BatchDurationGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID}, pkafka.BatchMessageCountGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID}, pkafka.BatchSizeGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID, "v2"}, pkafka.RequestRateGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID, "v2"}, pkafka.RequestLatencyGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID, "v2"}, pkafka.OutgoingByteRateGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID}, pkafka.ClientRetryGauge},
		{[]string{changefeedID.Namespace, changefeedID.ID}, pkafka.ClientErrorGauge},
	}
	c.collectMetrics()
	for _, cs := range cases {
		require.True(t, cs.gauge.DeleteLabelValues(cs.labelValues...))
	}
	c.collectMetrics()
	c.cleanupMetrics()
	for _, cs := range cases {
		require.False(t, cs.gauge.DeleteLabelValues(cs.labelValues...))
	}
}

func TestCollectorRun(t *testing.T) {
	changefeedID := model.DefaultChangeFeedID("test")
	mockWriter := mock.NewMockWriter(gomock.NewController(t))
	c := NewMetricsCollector(changefeedID, util.RoleOwner, mockWriter)
	mockWriter.EXPECT().Stats().AnyTimes().Return(kafka.WriterStats{})
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)
	cancel()
}
