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

package gc

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	minServiceGCSafePointGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "gc",
			Name:      "min_service_gc_safepoint",
			Help:      "The min value all of service GC safepoint",
		})

	cdcGCSafePointGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "gc",
			Name:      "cdc_gc_safepoint",
			Help:      "the value of CDC GC safepoint",
		})
)

// InitMetrics registers all metrics used gc manager
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(minServiceGCSafePointGauge)
	registry.MustRegister(cdcGCSafePointGauge)
}
