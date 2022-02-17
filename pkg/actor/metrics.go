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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	slowPollThreshold = 100 * time.Millisecond
	// Prometheus collects metrics every 15 seconds, we use a smaller interval
	// to improve accuracy.
	metricsInterval = 5 * time.Second
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
	batchSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "batch_size_total",
			Help:      "Total number of batch size of an actor system.",
		}, []string{"name", "type"})
	pollCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "poll_loop_total",
			Help:      "Total number of poll loop count.",
		}, []string{"name", "type"})
	slowPollActorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "actor",
			Name:      "slow_poll_duration_seconds",
			Help:      "Bucketed histogram of actor poll time (s).",
			Buckets:   prometheus.ExponentialBuckets(slowPollThreshold.Seconds(), 2, 16),
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
	registry.MustRegister(batchSizeCounter)
	registry.MustRegister(pollCounter)
	registry.MustRegister(slowPollActorDuration)
	registry.MustRegister(dropMsgCount)
}
