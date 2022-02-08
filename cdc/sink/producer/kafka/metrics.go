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

//+----------------------------------------------+------------+---------------------------------------------------------------+
//| Name                                         | Type       | Description                                                   |
//+----------------------------------------------+------------+---------------------------------------------------------------+
//| incoming-byte-rate                           | meter      | Bytes/second read off all brokers                             |
//| incoming-byte-rate-for-broker-<broker-id>    | meter      | Bytes/second read off a given broker                          |
//| outgoing-byte-rate                           | meter      | Bytes/second written off all brokers                          |
//| outgoing-byte-rate-for-broker-<broker-id>    | meter      | Bytes/second written off a given broker                       |
//| request-rate                                 | meter      | Requests/second sent to all brokers                           |
//| request-rate-for-broker-<broker-id>          | meter      | Requests/second sent to a given broker                        |
//| request-size                                 | histogram  | Distribution of the request size in bytes for all brokers     |
//| request-size-for-broker-<broker-id>          | histogram  | Distribution of the request size in bytes for a given broker  |
//| request-latency-in-ms                        | histogram  | Distribution of the request latency in ms for all brokers     |
//| request-latency-in-ms-for-broker-<broker-id> | histogram  | Distribution of the request latency in ms for a given broker  |
//| response-rate                                | meter      | Responses/second received from all brokers                    |
//| response-rate-for-broker-<broker-id>         | meter      | Responses/second received from a given broker                 |
//| response-size                                | histogram  | Distribution of the response size in bytes for all brokers    |
//| response-size-for-broker-<broker-id>         | histogram  | Distribution of the response size in bytes for a given broker |
//| requests-in-flight                           | counter    | The current number of in-flight requests awaiting a response  |
//|                                              |            | for all brokers                                               |
//| requests-in-flight-for-broker-<broker-id>    | counter    | The current number of in-flight requests awaiting a response  |
//|                                              |            | for a given broker                                            |
//+----------------------------------------------+------------+---------------------------------------------------------------+
