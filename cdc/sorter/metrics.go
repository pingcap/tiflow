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
	// InputEventCount is the metric that counts events input to sorter.
	InputEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "input_event_count",
		Help:      "The number of events input to sorter",
	}, []string{"namespace", "changefeed", "type"})

	// OutputEventCount is the metric that counts events output by the sorter.
	OutputEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "output_event_count",
		Help:      "The number of events output by the sorter",
	}, []string{"namespace", "changefeed", "type"})

	// ResolvedTsGauge is the metric that records sorter resolved ts.
	ResolvedTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "resolved_ts_gauge",
		Help:      "the resolved ts of the sorter",
	}, []string{"namespace", "changefeed"})

	// InMemoryDataSizeGauge is the metric that records sorter memory usage.
	InMemoryDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "in_memory_data_size_gauge",
		Help:      "The amount of pending data stored in-memory by the sorter",
	}, []string{"id"})

	// OnDiskDataSizeGauge is the metric that records sorter disk usage.
	OnDiskDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "on_disk_data_size_gauge",
		Help:      "The amount of pending data stored on-disk by the sorter",
	}, []string{"id"})

	// OpenFileCountGauge is the metric that records sorter open files.
	OpenFileCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "open_file_count_gauge",
		Help:      "The number of open file descriptors held by the sorter",
	}, []string{"id"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(InputEventCount)
	registry.MustRegister(OutputEventCount)
	registry.MustRegister(ResolvedTsGauge)
	registry.MustRegister(InMemoryDataSizeGauge)
	registry.MustRegister(OnDiskDataSizeGauge)
	registry.MustRegister(OpenFileCountGauge)
}
