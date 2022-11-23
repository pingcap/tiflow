// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	f = &promutil.PromFactory{}
	// should error.
	tidbExecutionErrorCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "tidb_execution_error",
			Help:      "Total count of tidb execution errors",
		}, []string{"task", "source_id"})

	queryHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of query time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})

	txnHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id", "target_schema", "target_table"})

	stmtHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})

	dataFileGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "data_file_gauge",
			Help:      "data files in total",
		}, []string{"task", "source_id"})

	tableGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "table_gauge",
			Help:      "tables in total",
		}, []string{"task", "source_id"})

	dataSizeGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "data_size_gauge",
			Help:      "data size in total",
		}, []string{"task", "source_id"})

	progressGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "progress",
			Help:      "the processing progress of loader in percentage",
		}, []string{"task", "source_id"})

	// should alert.
	loaderExitWithErrorCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "exit_with_error_count",
			Help:      "counter for loader exits with error",
		}, []string{"task", "source_id"})

	remainingTimeGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "remaining_time",
			Help:      "the remaining time in second to finish load process",
		}, []string{"task", "worker", "source_id", "source_schema", "source_table"})
)

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(tidbExecutionErrorCounter)
	registry.MustRegister(txnHistogram)
	registry.MustRegister(queryHistogram)
	registry.MustRegister(stmtHistogram)
	registry.MustRegister(dataFileGauge)
	registry.MustRegister(tableGauge)
	registry.MustRegister(dataSizeGauge)
	registry.MustRegister(progressGauge)
	registry.MustRegister(loaderExitWithErrorCounter)
	registry.MustRegister(remainingTimeGauge)
}

func (l *Loader) removeLabelValuesWithTaskInMetrics(task string) {
	tidbExecutionErrorCounter.DeletePartialMatch(prometheus.Labels{"task": task})
	txnHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	queryHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	stmtHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	dataFileGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	tableGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	dataSizeGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	progressGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	loaderExitWithErrorCounter.DeletePartialMatch(prometheus.Labels{"task": task})
	remainingTimeGauge.DeletePartialMatch(prometheus.Labels{"task": task})
}
