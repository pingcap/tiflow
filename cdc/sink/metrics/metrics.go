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
	"github.com/pingcap/tiflow/cdc/sink/metrics/cloudstorage"
	"github.com/pingcap/tiflow/cdc/sink/metrics/mq"
	"github.com/pingcap/tiflow/cdc/sink/metrics/tablesink"
	"github.com/pingcap/tiflow/cdc/sink/metrics/txn"
	"github.com/prometheus/client_golang/prometheus"
)

// rowSizeLowBound is set to 2K, only track data event with size not smaller than it.
const largeRowSizeLowBound = 2 * 1024

// ---------- Metrics used in Statistics. ---------- //
var (
	// ExecBatchHistogram records batch size of a txn.
	ExecBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_row_count",
			Help:      "Row count number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// ExecWriteBytesGauge records the total number of bytes written by sink.
	TotalWriteBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "write_bytes_total",
			Help:      "Total number of bytes written by sink",
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`

	// LargeRowSizeHistogram records size of large rows.
	LargeRowSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "large_row_size",
			Help:      "The size of all received row changed events (in bytes).",
			Buckets:   prometheus.ExponentialBuckets(largeRowSizeLowBound, 2, 15), // 2K~32M
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
			Help:      "Total count of execution errors.",
		}, []string{"namespace", "changefeed", "type"}) // type is for `sinkType`
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ExecBatchHistogram)
	registry.MustRegister(TotalWriteBytesCounter)
	registry.MustRegister(ExecDDLHistogram)
	registry.MustRegister(LargeRowSizeHistogram)
	registry.MustRegister(ExecutionErrorCounter)

	tablesink.InitMetrics(registry)
	txn.InitMetrics(registry)
	mq.InitMetrics(registry)
	cloudstorage.InitMetrics(registry)
}
