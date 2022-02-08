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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
)

type kafkaComponentType int

const (
	kafkaProducer kafkaComponentType = iota
)

type saramaMetrics struct {
	component   kafkaComponentType
	name        string
	description string
}

var (
	batchSize = saramaMetrics{
		component:   kafkaProducer,
		name:        "batch-size",
		description: "Distribution of the number of bytes sent per partition per request for all topics",
	}

	recordSendRate = saramaMetrics{
		component:   kafkaProducer,
		name:        "record-send-rate",
		description: "Records/second sent to all topics",
	}

	recordsPerRequest = saramaMetrics{
		component:   kafkaProducer,
		name:        "records-per-request",
		description: "Distribution of the number of records sent per request for all topics",
	}

	compressionRatio = saramaMetrics{
		component:   kafkaProducer,
		name:        "compression-ratio",
		description: "Distribution of the compression ratio times 100 of record batches for all topics",
	}
)

var (
	// Producer level metrics
	batchSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{})

	recordSendRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		}, []string{})

	recordsPerRequestHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_batch_size",
		Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
	}, []string{})

	compressionRatioHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_batch_size",
		Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
	}, []string{})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchSizeHistogram)
	registry.MustRegister(recordSendRateGauge)
	registry.MustRegister(recordsPerRequestHistogram)
	registry.MustRegister(compressionRatioHistogram)
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
