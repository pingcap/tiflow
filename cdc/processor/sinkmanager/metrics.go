// Copyright 2022 PingCAP, Inc.
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

package sinkmanager

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// MemoryQuota indicates memory usage of a changefeed.
	MemoryQuota = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sinkmanager",
			Name:      "memory_quota",
			Help:      "memory quota of the changefeed",
		},
		// type includes total, used, component includes sink and redo.
		[]string{"namespace", "changefeed", "type", "component"})

	// RedoEventCache indicates redo event memory usage of a changefeed.
	RedoEventCache = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sinkmanager",
			Name:      "redo_event_cache",
			Help:      "redo event cache of the changefeed",
		},
		[]string{"namespace", "changefeed"})

	// RedoEventCacheAccess indicates redo event cache hit ratio of a changefeed.
	RedoEventCacheAccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sinkmanager",
			Name:      "redo_event_cache_access",
			Help:      "redo event cache access, including hit and miss",
		},
		// type includes hit and miss.
		[]string{"namespace", "changefeed", "type"})

	// outputEventCount is the metric that counts events output by the sorter.
	outputEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "output_event_count",
		Help:      "The number of events output by the sorter",
	}, []string{"namespace", "changefeed", "type"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(RedoEventCache)
	registry.MustRegister(RedoEventCacheAccess)
	registry.MustRegister(outputEventCount)
}
