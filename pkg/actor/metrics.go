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

package actor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalWorkers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "number_of_workers",
			Help:      "The total number of workers in an actor system.",
		}, []string{"name"})
	workingWorkers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "number_of_workring_workers",
			Help:      "The number of workring workers in an actor system.",
		}, []string{"name"})
	workingDuration = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "workers_cpu_seconds_total",
			Help:      "Total working time spent in seconds.",
		}, []string{"name"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(totalWorkers)
	registry.MustRegister(workingWorkers)
	registry.MustRegister(workingDuration)
}
