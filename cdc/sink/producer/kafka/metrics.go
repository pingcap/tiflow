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
	"fmt"
	"io"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

// saramaMetrics wrap prometheus metrics, update value from sarama metrics.
type saramaMetrics struct {
	// metricsName is used to fetch sarama metrics
	metricsName string
	collector   prometheus.Collector
}

//type kafkaMetrics struct {
//	metrics []*saramaMetrics
//}
//

var (
	batchSize = saramaMetrics{
		metricsName: "batch-size",
		collector: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "ticdc",
				Subsystem: "sink",
				Name:      "kafka_producer_batch_size",
				Help:      "Distribution of the number of bytes sent per partition per request for all topics",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
			}, []string{}),
	}

	recordSendRate = saramaMetrics{
		metricsName: "record-send-rate",
		collector: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "ticdc",
				Subsystem: "sink",
				Name:      "kafka_producer_record_send_rate",
				Help:      "Records/second sent to all topics",
			}, []string{}),
	}

	recordsPerRequest = saramaMetrics{
		metricsName: "records-per-request",
		collector: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_records_per_request",
			Help:      "Distribution of the number of records sent per request for all topics",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{}),
	}

	compressionRatio = saramaMetrics{
		metricsName: "compression-ratio",
		collector: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_compression_ratio",
			Help:      "Distribution of the compression ratio times 100 of record batches for all topics",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{}),
	}
)

func (m saramaMetrics) Update(registry metrics.Registry) {
	// fetch sarama metrics
	rawMetric := registry.Get(m.metricsName)
	if rawMetric == nil {
		log.Warn("sarama metrics not found", zap.String("metricName", m.metricsName))
		return
	}

	var value float64
	switch tp := rawMetric.(type) {
	case metrics.Meter:
		snapshot := tp.Snapshot()
		value = snapshot.RateMean()
		//rate1 := snapshot.Rate1()
		//rate5 := snapshot.Rate5()
		//rate15 := snapshot.Rate15()
		//rateMean := snapshot.RateMean()
	case metrics.Histogram:
		snapshot := tp.Snapshot()
		value = snapshot.Mean()
		snapshot.Mean()
	case metrics.Counter:
		snapshot := tp.Snapshot()
		value = float64(snapshot.Count())
	case metrics.Gauge:
		snapshot := tp.Snapshot()
		value = float64(snapshot.Value())
	}

	// update prometheus metrics
	switch tp := m.collector.(type) {
	case prometheus.Histogram:
		tp.Observe(value)
	default:
		log.Panic("not support prometheus collector type")
	}
}

func printMetrics(w io.Writer, r metrics.Registry) {
	recordSendRateMetric := r.Get("record-send-rate")
	requestLatencyMetric := r.Get("request-latency-in-ms")
	outgoingByteRateMetric := r.Get("outgoing-byte-rate")
	requestsInFlightMetric := r.Get("requests-in-flight")

	if recordSendRateMetric == nil || requestLatencyMetric == nil || outgoingByteRateMetric == nil ||
		requestsInFlightMetric == nil {
		return
	}
	recordSendRate := recordSendRateMetric.(metrics.Meter).Snapshot()
	requestLatency := requestLatencyMetric.(metrics.Histogram).Snapshot()
	requestLatencyPercentiles := requestLatency.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
	outgoingByteRate := outgoingByteRateMetric.(metrics.Meter).Snapshot()
	requestsInFlight := requestsInFlightMetric.(metrics.Counter).Count()
	fmt.Fprintf(w, "%d records sent, %.1f records/sec (%.2f MiB/sec ingress, %.2f MiB/sec egress), "+
		"%.1f ms avg latency, %.1f ms stddev, %.1f ms 50th, %.1f ms 75th, "+
		"%.1f ms 95th, %.1f ms 99th, %.1f ms 99.9th, %d total req. in flight\n",
		recordSendRate.Count(),
		recordSendRate.RateMean(),
		recordSendRate.RateMean()*float64(1000)/1024/1024,
		outgoingByteRate.RateMean()/1024/1024,
		requestLatency.Mean(),
		requestLatency.StdDev(),
		requestLatencyPercentiles[0],
		requestLatencyPercentiles[1],
		requestLatencyPercentiles[2],
		requestLatencyPercentiles[3],
		requestLatencyPercentiles[4],
		requestsInFlight,
	)
}

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchSize.collector)
	//registry.MustRegister(recordSendRateGauge)
	//registry.MustRegister(recordsPerRequestHistogram)
	//registry.MustRegister(compressionRatioHistogram)
}
