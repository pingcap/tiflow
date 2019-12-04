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

package kv

import "github.com/prometheus/client_golang/prometheus"

var (
	regionSplitCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "region_split_count",
			Help:      "The number of region split events since start.",
		})
	scanRegionsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "scan_regions_duration_seconds",
			Help:      "The time it took to finish a scanRegions call.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"captureID"})
	eventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_size_bytes",
			Help:      "Size of KV events.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		}, []string{"captureID"})
)

// InitMetrics registers all metrics in the kv package
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(regionSplitCounter)
	registry.MustRegister(scanRegionsDuration)
	registry.MustRegister(eventSize)
}
