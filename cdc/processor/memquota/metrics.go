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

package memquota

import (
	"github.com/prometheus/client_golang/prometheus"
)

// MemoryQuota indicates memory usage of a changefeed.
var MemoryQuota = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sinkmanager",
		Name:      "memory_quota",
		Help:      "memory quota of the changefeed",
	},
	// type includes total, used, component includes sink and redo.
	[]string{"namespace", "changefeed", "type", "component"})

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MemoryQuota)
}
