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
	// "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// rowSizeLowBound is set to 128K, only track data event with size not smaller than it.
const largeRowSizeLowBound = 128 * 1024

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

	TxnWorkerFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_worker_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~1000s
		}, []string{"namespace", "changefeed"})

	TxnWorkerBusyRatio = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_worker_busy_ratio",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{"namespace", "changefeed"})

	TxnWorkerHandledRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_worker_handled_rows",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{"namespace", "changefeed", "id"})

	TxnSinkDMLBatchCommit = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_sink_dml_batch_commit",
			Help:      "Duration of committing a DML batch",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1000s
		}, []string{"namespace", "changefeed"})

	TxnSinkDMLBatchCallback = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_sink_dml_batch_callback",
			Help:      "Duration of execuing a batch of callbacks",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1000s
		}, []string{"namespace", "changefeed"})
)

// ---------- Metrics used in Statistics. ---------- //
var (
	// ExecBatchHistogram records batch size of a txn.
	ExecBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "batch_row_count",
			Help:      "Row count number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// LargeRowSizeHistogram records size of large rows.
	LargeRowSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "large_row_size",
			Help:      "The size of all received row changed events (in bytes).",
			Buckets:   prometheus.ExponentialBuckets(largeRowSizeLowBound, 2, 10), // 128K~128M
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecDDLHistogram records the exexution time of a DDL.
	ExecDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "ddl_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecutionErrorCounter is the counter of execution errors.
	ExecutionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "execution_error",
			Help:      "Total count of execution errors.",
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ConflictDetectDuration)
	registry.MustRegister(TxnWorkerFlushDuration)
	registry.MustRegister(TxnWorkerBusyRatio)
	registry.MustRegister(TxnWorkerHandledRows)
	registry.MustRegister(TxnSinkDMLBatchCommit)
	registry.MustRegister(TxnSinkDMLBatchCallback)

	registry.MustRegister(ExecBatchHistogram)
	registry.MustRegister(ExecDDLHistogram)
	registry.MustRegister(LargeRowSizeHistogram)
	registry.MustRegister(ExecutionErrorCounter)

	// Register Kafka producer and broker metrics.
	// kafka.InitMetrics(registry)
}
