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

package processor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	resolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "resolved_ts",
			Help:      "local resolved ts of processor",
		}, []string{"changefeed", "capture"})
	resolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "resolved_ts_lag",
			Help:      "local resolved ts lag of processor",
		}, []string{"changefeed", "capture"})
	checkpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts",
			Help:      "global checkpoint ts of processor",
		}, []string{"changefeed", "capture"})
	checkpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts_lag",
			Help:      "global checkpoint ts lag of processor",
		}, []string{"changefeed", "capture"})
	syncTableNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "num_of_tables",
			Help:      "number of synchronized table of processor",
		}, []string{"changefeed", "capture"})
	processorErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "exit_with_error_count",
			Help:      "counter for processor exits with error",
		}, []string{"changefeed", "capture"})
)

// InitMetrics registers all metrics used in processor
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(resolvedTsGauge)
	registry.MustRegister(resolvedTsLagGauge)
	registry.MustRegister(checkpointTsGauge)
	registry.MustRegister(checkpointTsLagGauge)
	registry.MustRegister(syncTableNumGauge)
	registry.MustRegister(processorErrorCounter)
}
