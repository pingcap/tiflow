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
	sorterDBWriteBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_write_bytes_total",
		Help:      "The total number of write bytes by the leveldb",
	}, []string{"capture", "id"})

	sorterDBReadBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_read_bytes_total",
		Help:      "The total number of read bytes by the leveldb",
	}, []string{"capture", "id"})

	sorterDBSnapshotGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_snapshot_count_gauge",
		Help:      "The number of snapshot by the sorter",
	}, []string{"capture", "id"})

	sorterDBIteratorGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_iterator_count_gauge",
		Help:      "The number of iterator by the sorter",
	}, []string{"capture", "id"})

	sorterDBLevelCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_level_count",
		Help:      "The number of files in each level by the sorter",
	}, []string{"capture", "level", "id"})

	sorterDBWriteDelayDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_write_delay_seconds",
		Help:      "The duration of leveldb write delay seconds",
	}, []string{"capture", "id"})

	sorterDBWriteDelayCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sorter",
		Name:      "leveldb_write_delay_total",
		Help:      "The total number of leveldb delay",
	}, []string{"capture", "id"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(sorterDBSnapshotGauge)
	registry.MustRegister(sorterDBIteratorGauge)
	registry.MustRegister(sorterDBLevelCount)
	registry.MustRegister(sorterDBWriteBytes)
	registry.MustRegister(sorterDBReadBytes)
	registry.MustRegister(sorterDBWriteDelayDuration)
	registry.MustRegister(sorterDBWriteDelayCount)
}
