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

package filter

import "github.com/prometheus/client_golang/prometheus"

var ignoredDDLEventCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "filter",
		Name:      "ignored_ddl_event_count",
		Help:      "ignored ddl counter of filter",
	}, []string{"type", "namespace", "changefeed"})

var ignoredDMLEventCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "filter",
		Name:      "ignored_dml_event_count",
		Help:      "ignored dml counter of filter",
	}, []string{"type", "namespace", "changefeed"})

var discardedDDLCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "filter",
		Name:      "discarded_ddl_count",
		Help:      "discarded ddl counter of filter",
	}, []string{"type", "namespace", "changefeed"})

// InitMetrics registers the filter related counters.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ignoredDDLEventCounter)
	registry.MustRegister(ignoredDMLEventCounter)
	registry.MustRegister(discardedDDLCounter)
}
