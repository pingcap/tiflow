// Copyright 2021 PingCAP, Inc.
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

package p2p

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	serverStreamCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "message_server",
		Name:      "cur_stream_count",
		Help:      "count of concurrent streams handled by the message server",
	}, []string{"from"})

	serverMessageCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "message_server",
		Name:      "message_count",
		Help:      "count of messages received",
	}, []string{"from"})

	serverMessageBatchHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "message_server",
		Name:      "message_batch_size",
		Help:      "size of message batches received",
		Buckets:   prometheus.LinearBuckets(0, 5, 16),
	}, []string{"from"})

	serverAckCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "message_server",
		Name:      "ack_count",
		Help:      "count of ack messages sent",
	}, []string{"to"})

	serverRepeatedMessageCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "message_server",
		Name:      "repeated_count",
		Help:      "count of received repeated messages",
	}, []string{"from", "topic"})
)

// InitMetrics initializes metrics used by pkg/p2p
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(serverStreamCount)
	registry.MustRegister(serverMessageCount)
	registry.MustRegister(serverMessageBatchHistogram)
	registry.MustRegister(serverAckCount)
	registry.MustRegister(serverRepeatedMessageCount)
}
