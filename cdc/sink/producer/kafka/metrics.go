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
)

type kafkaComponentType int

const (
	kafkaBroker kafkaComponentType = iota
	kafkaProducer
)

type saramaMetrics struct {
	component   kafkaComponentType
	name        string
	description string
}

var (
	incomingByteRate = saramaMetrics{
		component:   kafkaBroker,
		name:        "incoming-byte-rate",
		description: "Bytes/second read off all brokers",
	}

	outgoingByteRate = saramaMetrics{
		component:   kafkaBroker,
		name:        "outgoing-byte-rate",
		description: "Bytes/second written off all brokers",
	}

	requestRate = saramaMetrics{
		component:   kafkaBroker,
		name:        "request-rate",
		description: "Requests/second sent to all brokers",
	}

	requestSize = saramaMetrics{
		component:   kafkaBroker,
		name:        "request-size",
		description: "Distribution of the request size in bytes for all brokers",
	}

	requestLatencyInMs = saramaMetrics{
		component:   kafkaBroker,
		name:        "request-latency-in-ms",
		description: "Distribution of the request latency in ms for all brokers",
	}

	responseRate = saramaMetrics{
		component:   kafkaBroker,
		name:        "response-rate",
		description: "Responses/second received from all brokers",
	}

	responseSize = saramaMetrics{
		component:   kafkaBroker,
		name:        "response-size",
		description: "Distribution of the response size in bytes for all brokers",
	}

	requestInFlight = saramaMetrics{
		component:   kafkaBroker,
		name:        "requests-in-flight",
		description: "The current number of in-flight requests awaiting a response for all brokers",
	}
)

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

	// Broker level metrics
	incomingBytesRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		}, []string{})

	outgoingBytesRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		}, []string{})

	requestRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		}, []string{})

	requestSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		}, []string{})

	requestLatencyInMsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_batch_size",
		Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
	}, []string{})

	responseRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		}, []string{})

	responseSizeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_batch_size",
		Help:      "Distribution of the number of bytes sent per partition per request for all topics",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
	}, []string{})

	requestInFlightCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_request_in_flight",
		Help:      "The current number of in-flight requests awaiting a response for all brokers",
	}, []string{})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchSizeHistogram)
	registry.MustRegister(recordSendRateGauge)
	registry.MustRegister(recordsPerRequestHistogram)
	registry.MustRegister(compressionRatioHistogram)

	registry.MustRegister(incomingBytesRateGauge)
	registry.MustRegister(outgoingBytesRateGauge)
	registry.MustRegister(requestRateGauge)
	registry.MustRegister(requestSizeGauge)
	registry.MustRegister(requestLatencyInMsHistogram)
	registry.MustRegister(responseRateGauge)
	registry.MustRegister(responseSizeHistogram)
	registry.MustRegister(requestInFlightCounter)
}
