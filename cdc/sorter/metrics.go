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

package sorter

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// SorterEventCount is the metric that counts events output by the sorter.
	SorterEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "event_count",
		Help:      "The number of events output by the sorter",
	}, []string{"capture", "changefeed", "type"})

	// SorterResolvedTsGauge is the metric that records sorter resolved ts.
	SorterResolvedTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "resolved_ts_gauge",
		Help:      "the resolved ts of the sorter",
	}, []string{"capture", "changefeed"})

	// SorterInMemoryDataSizeGauge is the metric that records sorter memory usage.
	SorterInMemoryDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "in_memory_data_size_gauge",
		Help:      "The amount of pending data stored in-memory by the sorter",
	}, []string{"capture", "id"})

	// SorterOnDiskDataSizeGauge is the metric that records sorter disk usage.
	SorterOnDiskDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "on_disk_data_size_gauge",
		Help:      "The amount of pending data stored on-disk by the sorter",
	}, []string{"capture", "id"})

	// SorterOpenFileCountGauge is the metric that records sorter open files.
	SorterOpenFileCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "open_file_count_gauge",
		Help:      "The number of open file descriptors held by the sorter",
	}, []string{"capture", "id"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(SorterEventCount)
	registry.MustRegister(SorterResolvedTsGauge)
	registry.MustRegister(SorterInMemoryDataSizeGauge)
	registry.MustRegister(SorterOnDiskDataSizeGauge)
	registry.MustRegister(SorterOpenFileCountGauge)
}
