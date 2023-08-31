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

package mq

import (
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/sink/kafka/claimcheck"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// WorkerSendMessageDuration records the duration of flushing a group messages.
	WorkerSendMessageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_worker_send_message_duration",
			Help:      "Send Message duration(s) for MQ worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})
	// WorkerBatchSize record the size of each batched messages.
	WorkerBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_worker_batch_size",
			Help:      "Batch size for MQ worker.",
			Buckets:   prometheus.ExponentialBuckets(4, 2, 10), // 4 ~ 2048
		}, []string{"namespace", "changefeed"})
	// WorkerBatchDuration record the time duration cost on batch messages.
	WorkerBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_worker_batch_duration",
			Help:      "Batch duration for MQ worker.",
			Buckets:   prometheus.ExponentialBuckets(0.004, 2, 10), // 4ms ~ 2s
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	serverRegistry = registry

	registry.MustRegister(WorkerSendMessageDuration)
	registry.MustRegister(WorkerBatchSize)
	registry.MustRegister(WorkerBatchDuration)
	claimcheck.InitMetrics(registry)
	codec.InitMetrics(registry)
	kafka.InitMetrics(registry)
}

var serverRegistry *prometheus.Registry

// GetMetricRegistry for add pulsar default metrics
func GetMetricRegistry() *prometheus.Registry {
	// make sure registry is not nil
	if serverRegistry == nil {
		serverRegistry = prometheus.DefaultRegisterer.(*prometheus.Registry)
	}
	return serverRegistry
}
