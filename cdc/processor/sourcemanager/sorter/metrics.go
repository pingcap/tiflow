// Copyright 2023 PingCAP, Inc.
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
	mountWaitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "mount_wait_duration",
		Help:      "Bucketed histogram of mount wait duration",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
	}, []string{"namespace", "changefeed"})

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

	// inMemoryDataSizeGauge is the metric that records sorter memory usage.
	inMemoryDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "in_memory_data_size_gauge",
		Help:      "The amount of pending data stored in-memory by the sorter",
	}, []string{"id"})

	// onDiskDataSizeGauge is the metric that records sorter disk usage.
	onDiskDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "on_disk_data_size_gauge",
		Help:      "The amount of pending data stored on-disk by the sorter",
	}, []string{"id"})

	dbIteratorGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "iterator_count_gauge",
		Help:      "The number of iterator by the db",
	}, []string{"id"})

	dbLevelCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "level_count",
		Help:      "The number of files in each level by the db",
	}, []string{"level", "id"})

	dbWriteDelayCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "write_delay_total",
		Help:      "The total number of db delay",
	}, []string{"id"})

	dbBlockCacheAccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "block_cache_access_total",
		Help:      "The total number of db block cache access",
	}, []string{"id", "type"})

	dbRangeCleanCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "range_clean_count",
		Help:      "The total number of db range clean",
	})
)

/* Some metrics are shared in pipeline sorter and pull-based-sink sort engine */

// CompactionDuration returns sorterCompactDurationHistogram.
func CompactionDuration() *prometheus.HistogramVec {
	return sorterCompactDurationHistogram
}

// WriteDuration returns sorterWriteDurationHistogram.
func WriteDuration() *prometheus.HistogramVec {
	return sorterWriteDurationHistogram
}

// WriteBytes returns sorterWriteBytesHistogram.
func WriteBytes() *prometheus.HistogramVec {
	return sorterWriteBytesHistogram
}

// IterReadDuration returns sorterIterReadDurationHistogram.
func IterReadDuration() *prometheus.HistogramVec {
	return sorterIterReadDurationHistogram
}

// InMemoryDataSize returns inMemoryDataSizeGauge.
func InMemoryDataSize() *prometheus.GaugeVec {
	return inMemoryDataSizeGauge
}

// OnDiskDataSize returns onDiskDataSizeGauge.
func OnDiskDataSize() *prometheus.GaugeVec {
	return onDiskDataSizeGauge
}

// IteratorGauge returns dbIteratorGauge.
func IteratorGauge() *prometheus.GaugeVec {
	return dbIteratorGauge
}

// WriteDelayCount returns dbWriteDelayCount.
func WriteDelayCount() *prometheus.GaugeVec {
	return dbWriteDelayCount
}

// LevelCount returns dbLevelCount.
func LevelCount() *prometheus.GaugeVec {
	return dbLevelCount
}

// BlockCacheAccess returns dbBlockCacheAccess.
func BlockCacheAccess() *prometheus.GaugeVec {
	return dbBlockCacheAccess
}

// RangeCleanCount returns dbRangeCleanCount.
func RangeCleanCount() prometheus.Counter {
	return dbRangeCleanCount
}

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(mountWaitDuration)
	registry.MustRegister(sorterWriteDurationHistogram)
	registry.MustRegister(sorterCompactDurationHistogram)
	registry.MustRegister(sorterWriteBytesHistogram)
	registry.MustRegister(sorterIterReadDurationHistogram)
	registry.MustRegister(inMemoryDataSizeGauge)
	registry.MustRegister(onDiskDataSizeGauge)
	registry.MustRegister(dbIteratorGauge)

	// TODO: Seems these things belong to pebble instead of engine.
	registry.MustRegister(dbLevelCount)
	registry.MustRegister(dbWriteDelayCount)
	registry.MustRegister(dbBlockCacheAccess)
	registry.MustRegister(dbRangeCleanCount)
}
