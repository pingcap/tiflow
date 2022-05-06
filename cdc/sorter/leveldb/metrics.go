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

package leveldb

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sorterWriteBytesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "db_write_bytes",
		Help:      "Bucketed histogram of sorter write batch bytes",
		Buckets:   prometheus.ExponentialBuckets(16, 2.0, 20),
	}, []string{"id"})

	sorterWriteDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "db_write_duration_seconds",
		Help:      "Bucketed histogram of sorter write duration",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"id"})

	sorterCompactDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "db_compact_duration_seconds",
		Help:      "Bucketed histogram of sorter manual compact duration",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"id"})

	sorterIterReadDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "db_iter_read_duration_seconds",
		Help:      "Bucketed histogram of db sorter iterator read duration",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"namespace", "id", "call"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(sorterWriteDurationHistogram)
	registry.MustRegister(sorterCompactDurationHistogram)
	registry.MustRegister(sorterWriteBytesHistogram)
	registry.MustRegister(sorterIterReadDurationHistogram)
}
