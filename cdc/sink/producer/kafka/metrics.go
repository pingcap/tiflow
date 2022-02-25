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
	// batch-size
	batchSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "the number of bytes sent per partition per request for all topics",
		}, []string{"capture", "changefeed"})

	// record-send-rate
	recordSendRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_record_send_rate",
			Help:      "Records/second sent to all topics",
		}, []string{"capture", "changefeed"})

	// records-per-request
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
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchSizeGauge)
	registry.MustRegister(recordSendRateGauge)
	registry.MustRegister(recordPerRequestGauge)
	registry.MustRegister(compressionRatioGauge)
}

// sarama metrics names, see https://pkg.go.dev/github.com/Shopify/sarama#pkg-overview
const (
	batchSizeMetricName        = "batch-size"
	recordSendRateMetricName   = "record-send-rate"
	recordPerRequestMetricName = "records-per-request"
	compressionRatioMetricName = "compression-ratio"
)

type saramaMetricsMonitor struct {
	captureAddr  string
	changefeedID string

	registry metrics.Registry
}

// CollectMetrics collect all monitored metrics
func (sm *saramaMetricsMonitor) CollectMetrics() {
	batchSizeMetric := sm.registry.Get(batchSizeMetricName)
	if histogram, ok := batchSizeMetric.(metrics.Histogram); ok {
		batchSizeGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Mean())
	}

	recordSendRateMetric := sm.registry.Get(recordSendRateMetricName)
	if meter, ok := recordSendRateMetric.(metrics.Meter); ok {
		recordSendRateGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(meter.Rate1())
	}

	recordPerRequestMetric := sm.registry.Get(recordPerRequestMetricName)
	if histogram, ok := recordPerRequestMetric.(metrics.Histogram); ok {
		recordPerRequestGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Mean())
	}

	compressionRatioMetric := sm.registry.Get(compressionRatioMetricName)
	if histogram, ok := compressionRatioMetric.(metrics.Histogram); ok {
		compressionRatioGauge.WithLabelValues(sm.captureAddr, sm.changefeedID).Set(histogram.Mean())
	}
}

func newSaramaMetricsMonitor(registry metrics.Registry, captureAddr, changefeedID string) *saramaMetricsMonitor {
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
}
