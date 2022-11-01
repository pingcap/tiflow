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
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics/mq/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// WorkerFlushDuration records the duration of flushing a group messages.
var (
	WorkerFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "mq_worker_flush_duration",
			Help: "Flush duration(s) for MQ worker. " +
				"It contains the grouping, encoding, and asynchronous sending off a batch of data.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~1000s
		}, []string{"namespace", "changefeed"})

	WorkerDispatchedEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sinkv2",
		Name:      "mq_worker_dispatched_event_count",
		Help:      "The number of events dispatched to each MQ worker.",
	}, []string{"namespace", "changefeed", "index"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(WorkerFlushDuration)
	registry.MustRegister(WorkerDispatchedEventCount)
	kafka.InitMetrics(registry)
}
