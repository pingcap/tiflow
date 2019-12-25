// Copyright 2019 PingCAP, Inc.
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

package cdc

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
	tableResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "table_resolved_ts",
			Help:      "local resolved ts of processor",
		}, []string{"changefeed", "capture", "table"})
	checkpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts",
			Help:      "global checkpoint ts of processor",
		}, []string{"changefeed", "capture"})
	syncTableNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "num_of_tables",
			Help:      "number of synchronized table of processor",
		}, []string{"changefeed", "capture"})
	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "txn_count",
			Help:      "txn count received/executed by this processor",
		}, []string{"type", "changefeed", "capture"})
	updateInfoDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "update_info_duration_seconds",
			Help:      "The time it took to update sub change feed info.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"captureID"})
)

// initProcessorMetrics registers all metrics used in processor
func initProcessorMetrics(registry *prometheus.Registry) {
	registry.MustRegister(resolvedTsGauge)
	registry.MustRegister(tableResolvedTsGauge)
	registry.MustRegister(checkpointTsGauge)
	registry.MustRegister(syncTableNumGauge)
	registry.MustRegister(txnCounter)
	registry.MustRegister(updateInfoDuration)
}
