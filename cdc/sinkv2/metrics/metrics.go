// Copyright 2020 PingCAP, Inc.
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

package metrics

import (
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// rowSizeLowBound is set to 128K, only track data event with size not smaller than it.
const rowSizeLowBound = 128 * 1024

// ---------- Metrics for txn sink and backends. ---------- //
var (
	// ConflictDetectDuration records the duration of detecting conflict.
	ConflictDetectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~1000s
		}, []string{"namespace", "changefeed"})
)

// ---------- Metrics used in Statistics. ---------- //
var (
	// ExecBatchHistogram records batch size of a txn.
	ExecBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_batch_size",
			Help:      "Bucketed histogram of batch size of a txn.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// RowSizeHistogram records the row size of events.
	RowSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "large_row_changed_event_size",
			Help:      "The size of all received row changed events (in bytes).",
			Buckets:   prometheus.ExponentialBuckets(rowSizeLowBound, 2, 10),
		}, []string{"namespace", "changefeed"})

	// ExecDDLHistogram records the exexution time of a DDL.
	ExecDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "ddl_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{"namespace", "changefeed"})

	// ExecutionErrorCounter is the counter of execution errors.
	ExecutionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "execution_error",
			Help:      "Total count of execution errors.",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ConflictDetectDuration)

	registry.MustRegister(ExecBatchHistogram)
	registry.MustRegister(ExecDDLHistogram)
	registry.MustRegister(RowSizeHistogram)
	registry.MustRegister(ExecutionErrorCounter)

	// Register Kafka producer and broker metrics.
	kafka.InitMetrics(registry)
}
