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

import "github.com/prometheus/client_golang/prometheus"

var (
	tableMemoryHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "table_memory_consumption",
			Help:      "estimated memory consumption for a table after the sorter",
			Buckets:   prometheus.ExponentialBuckets(1*1024*1024 /* mb */, 2, 10),
		}, []string{"namespace", "changefeed"})

	// SorterBatchReadSize record each batch read size
	SorterBatchReadSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "sorter_batch_read",
			Help:      "Bucketed histogram of sorter batch read event counts",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 6),
		}, []string{"namespace", "changefeed"})
	// SorterBatchReadDuration record each batch read duration
	SorterBatchReadDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "sorter_batch_read_duration",
			Help:      "Bucketed histogram of sorter batch read duration",
			Buckets:   prometheus.ExponentialBuckets(0.0001 /* 0.1 ms */, 2, 18),
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers metrics the pipeline.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(tableMemoryHistogram)
	registry.MustRegister(SorterBatchReadSize)
	registry.MustRegister(SorterBatchReadDuration)
}
