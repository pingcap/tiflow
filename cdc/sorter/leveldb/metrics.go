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
	sorterWriteBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "total_write_bytes",
		Help:      "The total number of write bytes by the sorter",
	}, []string{"capture", "id"})

	sorterWriteHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "write_duration_seconds",
		Help:      "Bucketed histogram of sorter write duration",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"capture", "id"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(sorterWriteHistogram)
	registry.MustRegister(sorterWriteBytes)
}
