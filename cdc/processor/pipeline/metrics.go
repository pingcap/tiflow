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

package pipeline

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "txn_count",
			Help:      "txn count received/executed by this processor",
		}, []string{"type", "changefeed", "capture"})
	tableMemoryHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "table_memory_consumption",
			Help:      "estimated memory consumption for a table after the sorter",
			Buckets:   prometheus.ExponentialBuckets(1*1024*1024 /* mb */, 2, 10),
		}, []string{"changefeed", "capture"})
)

// InitMetrics registers all metrics used in processor
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(txnCounter)
	registry.MustRegister(tableMemoryHistogram)
}
