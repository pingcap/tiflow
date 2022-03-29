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

package sink

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	execBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_batch_size",
			Help:      "Bucketed histogram of batch size of a txn.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"changefeed", "type"}) // type is for `sinkType`
	execTxnHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2 ms */, 2, 18),
		}, []string{"changefeed", "type"}) // type is for `sinkType`
	execDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "ddl_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{"changefeed"})
	executionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "total count of execution errors",
		}, []string{"changefeed"})
	conflictDetectDurationHis = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 20),
		}, []string{"changefeed"})
	bucketSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "bucket_size",
			Help:      "size of the DML bucket",
		}, []string{"changefeed", "bucket"})
	totalRowsCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "total_rows_count",
			Help:      "The total count of rows that are processed by sink",
		}, []string{"changefeed"})
	totalFlushedRowsCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "total_flushed_rows_count",
			Help:      "The total count of rows that are flushed by sink",
		}, []string{"changefeed"})

	tableSinkTotalRowsCountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "table_sink_total_rows_count",
			Help:      "The total count of rows that are processed by table sink",
		}, []string{"changefeed"})

	bufferSinkTotalRowsCountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "buffer_sink_total_rows_count",
			Help:      "The total count of rows that are processed by buffer sink",
		}, []string{"changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(execBatchHistogram)
	registry.MustRegister(execTxnHistogram)
	registry.MustRegister(execDDLHistogram)
	registry.MustRegister(executionErrorCounter)
	registry.MustRegister(conflictDetectDurationHis)
	registry.MustRegister(bucketSizeCounter)
	registry.MustRegister(totalRowsCountGauge)
	registry.MustRegister(totalFlushedRowsCountGauge)
	registry.MustRegister(tableSinkTotalRowsCountCounter)
	registry.MustRegister(bufferSinkTotalRowsCountCounter)
}
