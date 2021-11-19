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
			Name:      "number_of_working_workers",
			Help:      "The number of working workers in an actor system.",
		}, []string{"name"})
	workingDuration = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "worker_cpu_seconds_total",
			Help:      "Total user and system CPU time spent by workers in seconds.",
		}, []string{"name", "id"})
	batchSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "batch",
			Help:      "Bucketed histogram of batch size of an actor system.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		}, []string{"name", "type"})
	pollActorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "poll_duration_seconds",
			Help:      "Bucketed histogram of actor poll time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"name"})
	dropMsgCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "drop_message_total",
			Help:      "The total number of dropped messages in an actor system.",
		}, []string{"name"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(totalWorkers)
	registry.MustRegister(workingWorkers)
	registry.MustRegister(workingDuration)
	registry.MustRegister(batchSizeHistogram)
	registry.MustRegister(pollActorDuration)
	registry.MustRegister(dropMsgCount)
}
