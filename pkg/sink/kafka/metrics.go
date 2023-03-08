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
	// Counter inc by 1 once a request send, dec by 1 for a response received.
	requestsInFlightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_in_flight_requests",
			Help: "The current number of in-flight requests" +
				" awaiting a response for all brokers.",
		}, []string{"namespace", "changefeed", "broker"})
	// OutgoingByteRateGauge for outgoing events.
	// Meter mark for each request's size in bytes.
	OutgoingByteRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_outgoing_byte_rate",
			Help:      "Bytes/second written off all brokers.",
		}, []string{"namespace", "changefeed", "broker"})
	// RequestRateGauge Meter mark by 1 for each request.
	RequestRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_rate",
			Help:      "Requests/second sent to all brokers.",
		}, []string{"namespace", "changefeed", "broker"})
	// RequestLatencyInMsGauge Histogram update by `requestLatency`.
	// requestLatency := time.Since(response.requestTime).
	RequestLatencyInMsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_latency",
			Help:      "The request latency in ms for all brokers.",
		}, []string{"namespace", "changefeed", "broker"})
	// Histogram update by `compression-ratio`.
	compressionRatioGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_compression_ratio",
			Help:      "The compression ratio times 100 of record batches for all topics.",
		}, []string{"namespace", "changefeed"})
	// Meter mark by 1 once a response received.
	responseRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_response_rate",
			Help:      "Responses/second received from all brokers.",
		}, []string{"namespace", "changefeed", "broker"})

	RetryCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_retry_count",
			Help:      "Kafka Client send request retry count",
		}, []string{"namespace", "changefeed"})

	ErrorCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_err_count",
			Help:      "Kafka Client send request retry count",
		}, []string{"namespace", "changefeed"})

	BatchDurationGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_duration",
			Help:      "Kafka client internal average batch message time cost in milliseconds",
		}, []string{"namespace", "changefeed"})

	BatchMessageCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_message_count",
			Help:      "Kafka client internal average batch message count",
		}, []string{"namespace", "changefeed"})

	BatchSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_batch_size",
			Help:      "Kafka client internal average batch size in bytes",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(compressionRatioGauge)
	registry.MustRegister(OutgoingByteRateGauge)
	registry.MustRegister(RequestRateGauge)
	registry.MustRegister(RequestLatencyInMsGauge)
	registry.MustRegister(requestsInFlightGauge)
	registry.MustRegister(responseRateGauge)

	// only used by kafka sink v2.
	registry.MustRegister(BatchDurationGauge)
	registry.MustRegister(BatchMessageCountGauge)
	registry.MustRegister(BatchSizeGauge)
	registry.MustRegister(RetryCount)
	registry.MustRegister(ErrorCount)
}
