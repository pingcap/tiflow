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

package writer

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "ticdc"
	subsystem = "redo"
)

var (
	redoWriteBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "write_bytes_total",
		Help:      "Total number of bytes redo log written",
	}, []string{"namespace", "changefeed"})

	redoFsyncDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "fsync_duration_seconds",
		Help:      "The latency distributions of fsync called by redo writer",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 13),
	}, []string{"namespace", "changefeed"})

	redoFlushAllDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "flushall_duration_seconds",
		Help:      "The latency distributions of flushall called by redo writer",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 13),
	}, []string{"namespace", "changefeed"})

	redoTotalRowsCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "total_rows_count",
		Help:      "The total count of rows that are processed by redo writer",
	}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(redoFsyncDurationHistogram)
	registry.MustRegister(redoTotalRowsCountGauge)
	registry.MustRegister(redoWriteBytesGauge)
	registry.MustRegister(redoFlushAllDurationHistogram)
}
