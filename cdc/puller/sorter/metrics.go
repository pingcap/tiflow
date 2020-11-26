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

package sorter

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sorterEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_event_count",
		Help:      "the number of events output by the sorter",
	}, []string{"capture", "changefeed", "table", "type"})

	sorterResolvedTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_resolved_ts_gauge",
		Help:      "the resolved ts of the sorter",
	}, []string{"capture", "changefeed", "table"})

	sorterMergerStartTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_merger_start_ts_gauge",
		Help:      "the start TS of each merge in the sorter",
	}, []string{"capture", "changefeed", "table"})

	sorterInMemoryDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_in_memory_data_size_gauge",
		Help:      "the amount of pending data stored in-memory by the sorter",
	}, []string{"capture"})

	sorterOnDiskDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_on_disk_data_size_gauge",
		Help:      "the amount of pending data stored on-disk by the sorter",
	}, []string{"capture"})

	sorterOpenFileCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_open_file_count_gauge",
		Help:      "the number of open file descriptors held by the sorter",
	}, []string{"capture"})

	sorterFlushCountHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_flush_count_histogram",
		Help:      "Bucketed histogram of the number of events in individual flushes performed by the sorter",
		Buckets:   prometheus.ExponentialBuckets(4, 4, 10),
	}, []string{"capture", "changefeed", "table"})

	sorterMergeCountHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "sorter_merge_count_histogram",
		Help:      "Bucketed histogram of the number of events in individual merges performed by the sorter",
		Buckets:   prometheus.ExponentialBuckets(16, 4, 10),
	}, []string{"capture", "changefeed", "table"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(sorterEventCount)
	registry.MustRegister(sorterResolvedTsGauge)
	registry.MustRegister(sorterMergerStartTsGauge)
	registry.MustRegister(sorterInMemoryDataSizeGauge)
	registry.MustRegister(sorterOnDiskDataSizeGauge)
	registry.MustRegister(sorterOpenFileCountGauge)
	registry.MustRegister(sorterFlushCountHistogram)
	registry.MustRegister(sorterMergeCountHistogram)
}
