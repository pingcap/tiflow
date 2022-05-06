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

package unified

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sorterConsumeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "consume_count",
		Help:      "the number of events consumed by the sorter",
	}, []string{"namespace", "changefeed", "type"})

	sorterMergerStartTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "merger_start_ts_gauge",
		Help:      "the start TS of each merge in the sorter",
	}, []string{"namespace", "changefeed"})

	sorterFlushCountHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "flush_count_histogram",
		Help:      "Bucketed histogram of the number of events in individual flushes performed by the sorter",
		Buckets:   prometheus.ExponentialBuckets(4, 4, 10),
	}, []string{"namespace", "changefeed"})

	sorterMergeCountHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "merge_count_histogram",
		Help:      "Bucketed histogram of the number of events in individual merges performed by the sorter",
		Buckets:   prometheus.ExponentialBuckets(16, 4, 10),
	}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(sorterConsumeCount)
	registry.MustRegister(sorterMergerStartTsGauge)
	registry.MustRegister(sorterFlushCountHistogram)
	registry.MustRegister(sorterMergeCountHistogram)
}
