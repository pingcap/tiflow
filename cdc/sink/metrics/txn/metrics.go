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

package txn

import "github.com/prometheus/client_golang/prometheus"

// ---------- Metrics for txn sink and backends. ---------- //
var (
	// ConflictDetectDuration records the duration of detecting conflict.
	ConflictDetectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})

	// QueueDuration = ConflictDetectDuration + (queue time in txn workers).
	QueueDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_queue_duration",
			Help:      "Bucketed histogram of queue time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})

	WorkerFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_worker_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"namespace", "changefeed"})

	WorkerBusyRatio = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_worker_busy_ratio",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{"namespace", "changefeed"})

	WorkerHandledRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_worker_handled_rows",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{"namespace", "changefeed", "id"})

	SinkDMLBatchCommit = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_sink_dml_batch_commit",
			Help:      "Duration of committing a DML batch",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1310s
		}, []string{"namespace", "changefeed"})

	SinkDMLBatchCallback = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sinkv2",
			Name:      "txn_sink_dml_batch_callback",
			Help:      "Duration of execuing a batch of callbacks",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1300s
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ConflictDetectDuration)
	registry.MustRegister(QueueDuration)
	registry.MustRegister(WorkerFlushDuration)
	registry.MustRegister(WorkerBusyRatio)
	registry.MustRegister(WorkerHandledRows)
	registry.MustRegister(SinkDMLBatchCommit)
	registry.MustRegister(SinkDMLBatchCallback)
}
