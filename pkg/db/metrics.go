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

package db

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dbWriteBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "write_bytes_total",
		Help:      "The total number of write bytes by the db",
	}, []string{"id"})

	dbReadBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "read_bytes_total",
		Help:      "The total number of read bytes by the db",
	}, []string{"id"})

	dbSnapshotGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "snapshot_count_gauge",
		Help:      "The number of snapshot by the db",
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

	dbWriteDelayDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "db",
		Name:      "write_delay_seconds",
		Help:      "The duration of db write delay seconds",
	}, []string{"id"})

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
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(dbSnapshotGauge)
	registry.MustRegister(dbIteratorGauge)
	registry.MustRegister(dbLevelCount)
	registry.MustRegister(dbWriteBytes)
	registry.MustRegister(dbReadBytes)
	registry.MustRegister(dbWriteDelayDuration)
	registry.MustRegister(dbWriteDelayCount)
	registry.MustRegister(dbBlockCacheAccess)
}

/* There are some metrics shared with pipeline sorter and pull-based-sink sort engine. */

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
