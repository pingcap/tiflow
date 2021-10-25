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
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	grpcClientMetrics = grpc_prometheus.NewClientMetrics(func(opts *prometheus.CounterOpts) {
		opts.Namespace = "ticdc"
		opts.Subsystem = "message_client"
	})

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
		Namespace:   "ticdc",
		Subsystem:   "message_server",
		Name:        "message_batch_size",
		Help:        "size of message batches received",
		Buckets:     prometheus.LinearBuckets(0, 5, 16),
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
		Name: "repeated_count",
		Help: "count of received repeated messages",
	}, []string{"from", "topic"})

	clientCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "message_client",
		Name:      "client_count",
		Help:      "count of messaging clients",
	}, []string{"to"})

	clientMessageCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "message_client",
		Name:      "message_count",
		Help:      "count of messages sent",
	}, []string{"to"})

	clientAckCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "message_client",
		Name:      "ack_count",
		Help:      "count of ack messages received",
	}, []string{"from"})
)

func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(grpcClientMetrics)
	registry.MustRegister(serverStreamCount)
	registry.MustRegister(serverMessageCount)
	registry.MustRegister(serverMessageBatchHistogram)
	registry.MustRegister(serverAckCount)
	registry.MustRegister(serverRepeatedMessageCount)
	registry.MustRegister(clientCount)
	registry.MustRegister(clientMessageCount)
	registry.MustRegister(clientAckCount)
}
