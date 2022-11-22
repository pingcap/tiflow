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
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/prometheus/client_golang/prometheus"
)

// rowSizeLowBound is set to 128K, only track data event with size not smaller than it.
const rowSizeLowBound = 128 * 1024

var (
	// ExecBatchHistogram records batch size of a txn.
	ExecBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_batch_size",
			Help:      "Bucketed histogram of batch size of a txn.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecTxnHistogram records the execution time of a txn.
	ExecTxnHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2 ms */, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// LargeRowSizeHistogram records the row size of events.
	LargeRowSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "large_row_changed_event_size",
			Help:      "The size of all received row changed events (in bytes)",
			Buckets:   prometheus.ExponentialBuckets(rowSizeLowBound, 2, 10),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecDDLHistogram records the exexution time of a DDL.
	ExecDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "ddl_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecutionErrorCounter is the counter of execution errors.
	ExecutionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "total count of execution errors",
		}, []string{"namespace", "changefeed"})

	// ConflictDetectDurationHis records the duration of detecting conflict.
	ConflictDetectDurationHis = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 20),
		}, []string{"namespace", "changefeed"})

	// BucketSizeCounter is the counter of bucket size.
	BucketSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "bucket_size",
			Help:      "size of the DML bucket",
		}, []string{"namespace", "changefeed", "bucket"})

	// TotalRowsCountGauge is the total number of rows that are processed by sink.
	TotalRowsCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "total_rows_count",
			Help:      "The total count of rows that are processed by sink",
		}, []string{"namespace", "changefeed"})

	// TotalFlushedRowsCountGauge is the total count of rows that are flushed to sink.
	TotalFlushedRowsCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "total_flushed_rows_count",
			Help:      "The total count of rows that are flushed by sink",
		}, []string{"namespace", "changefeed"})

	// TableSinkTotalRowsCountCounter is the total count of rows that are processed by sink.
	TableSinkTotalRowsCountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "table_sink_total_rows_count",
			Help:      "The total count of rows that are processed by table sink",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ExecBatchHistogram)
	registry.MustRegister(ExecTxnHistogram)
	registry.MustRegister(ExecDDLHistogram)
	registry.MustRegister(LargeRowSizeHistogram)
	registry.MustRegister(ExecutionErrorCounter)
	registry.MustRegister(ConflictDetectDurationHis)
	registry.MustRegister(BucketSizeCounter)
	registry.MustRegister(TotalRowsCountGauge)
	registry.MustRegister(TotalFlushedRowsCountGauge)
	registry.MustRegister(TableSinkTotalRowsCountCounter)
	codec.InitMetrics(registry)
}
