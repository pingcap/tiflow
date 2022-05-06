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
		}, []string{"namespace", "changefeed"})
	resolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "resolved_ts_lag",
			Help:      "local resolved ts lag of processor",
		}, []string{"namespace", "changefeed"})
	resolvedTsMinTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "min_resolved_table_id",
			Help:      "ID of the minimum resolved table",
		}, []string{"namespace", "changefeed"})
	checkpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts",
			Help:      "global checkpoint ts of processor",
		}, []string{"namespace", "changefeed"})
	checkpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts_lag",
			Help:      "global checkpoint ts lag of processor",
		}, []string{"namespace", "changefeed"})
	checkpointTsMinTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "min_checkpoint_table_id",
			Help:      "ID of the minimum checkpoint table",
		}, []string{"namespace", "changefeed"})
	syncTableNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "num_of_tables",
			Help:      "number of synchronized table of processor",
		}, []string{"namespace", "changefeed"})
	processorErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "exit_with_error_count",
			Help:      "counter for processor exits with error",
		}, []string{"namespace", "changefeed"})
	processorSchemaStorageGcTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "schema_storage_gc_ts",
			Help:      "the TS of the currently maintained oldest snapshot in SchemaStorage",
		}, []string{"namespace", "changefeed"})
	processorTickDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "processor_tick_duration",
			Help:      "Bucketed histogram of processorManager tick processor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{"namespace", "changefeed"})
	processorCloseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "processor",
			Name:      "processor_close_duration",
			Help:      "Bucketed histogram of processorManager close processor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		})
)

// InitMetrics registers all metrics used in processor
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(resolvedTsGauge)
	registry.MustRegister(resolvedTsLagGauge)
	registry.MustRegister(resolvedTsMinTableIDGauge)
	registry.MustRegister(checkpointTsGauge)
	registry.MustRegister(checkpointTsLagGauge)
	registry.MustRegister(checkpointTsMinTableIDGauge)
	registry.MustRegister(syncTableNumGauge)
	registry.MustRegister(processorErrorCounter)
	registry.MustRegister(processorSchemaStorageGcTsGauge)
	registry.MustRegister(processorTickDuration)
	registry.MustRegister(processorCloseDuration)
}
