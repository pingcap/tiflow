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

package owner

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	changefeedBarrierTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "barrier_ts",
			Help:      "barrier ts of changefeeds",
		}, []string{"namespace", "changefeed"})

	changefeedCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of changefeeds",
		}, []string{"namespace", "changefeed"})
	changefeedCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of changefeeds in seconds",
		}, []string{"namespace", "changefeed"})
	changefeedResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts",
			Help:      "resolved ts of changefeeds",
		}, []string{"namespace", "changefeed"})
	changefeedResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of changefeeds in seconds",
		}, []string{"namespace", "changefeed"})
	ownershipCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})
	ownerMaintainTableNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "maintain_table_num",
			Help:      "number of replicated tables maintained in owner",
		}, []string{"namespace", "changefeed", "capture", "type"})
	changefeedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "status",
			Help:      "The status of changefeeds",
		}, []string{"namespace", "changefeed"})
	changefeedTickDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_tick_duration",
			Help:      "Bucketed histogram of owner tick changefeed reactor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{"namespace", "changefeed"})
	changefeedCloseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_close_duration",
			Help:      "Bucketed histogram of owner close changefeed reactor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		})
)

const (
	// total tables that have been dispatched to a single processor
	maintainTableTypeTotal string = "total"
	// tables that are dispatched to a processor and have not been finished yet
	maintainTableTypeWip string = "wip"
	// When heavy operations (such as network IO and serialization) take too much time, the program
	// should print a warning log, and if necessary, the timeout should be exposed externally through
	// monitor.
	changefeedLogsWarnDuration = 1 * time.Second
	schedulerLogsWarnDuration  = 1 * time.Second
)

// InitMetrics registers all metrics used in owner
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(changefeedBarrierTsGauge)
	registry.MustRegister(changefeedCheckpointTsGauge)
	registry.MustRegister(changefeedResolvedTsGauge)
	registry.MustRegister(changefeedCheckpointTsLagGauge)
	registry.MustRegister(changefeedResolvedTsLagGauge)
	registry.MustRegister(ownershipCounter)
	registry.MustRegister(ownerMaintainTableNumGauge)
	registry.MustRegister(changefeedStatusGauge)
	registry.MustRegister(changefeedTickDuration)
	registry.MustRegister(changefeedCloseDuration)
}
