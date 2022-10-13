// Copyright 2022 PingCAP, Inc.
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

package replication

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	tableGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table",
			Help:      "The total number of tables",
		}, []string{"namespace", "changefeed"})
	tableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_replication_state",
			Help:      "The total number of tables in different replication states",
		}, []string{"namespace", "changefeed", "state"})
	acceptScheduleTaskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task_accept",
			Help:      "The total number of accepted scheduler tasks",
		}, []string{"namespace", "changefeed", "task"})
	runningScheduleTaskGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task_running",
			Help:      "The total number of running scheduler tasks",
		}, []string{"namespace", "changefeed"})
	slowestTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_id",
			Help:      "The table ID of the slowest table",
		}, []string{"namespace", "changefeed"})
	slowestTableCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_checkpoint_ts",
			Help:      "The checkpoint ts of the slowest table",
		}, []string{"namespace", "changefeed"})
	slowestTableResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_resolved_ts",
			Help:      "The resolved ts of the slowest table",
		}, []string{"namespace", "changefeed"})
	slowestTableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_replication_state",
			Help:      "The replication state of the slowest table",
		}, []string{"namespace", "changefeed"})
	slowestTableStageCheckpointTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts",
			Help:      "Checkpoint ts of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	slowestTableStageResolvedTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts",
			Help:      "Resolved ts of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	slowestTableStageCheckpointTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	slowestTableStageResolvedTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag",
			Help:      "Resolved ts lag of each stage of the slowest table",
		}, []string{"namespace", "changefeed", "stage"})
	slowestTableStageCheckpointTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag_histogram",
			Help:      "Histogram of the slowest table checkpoint ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{"namespace", "changefeed", "stage"})
	slowestTableStageResolvedTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag_histogram",
			Help:      "Histogram of the slowest table resolved ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{"namespace", "changefeed", "stage"})
	slowestTableRegionGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_region_count",
			Help:      "The number of regions captured by the slowest table",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics used in scheduler
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(tableGauge)
	registry.MustRegister(tableStateGauge)
	registry.MustRegister(acceptScheduleTaskCounter)
	registry.MustRegister(runningScheduleTaskGauge)
	registry.MustRegister(slowestTableIDGauge)
	registry.MustRegister(slowestTableCheckpointTsGauge)
	registry.MustRegister(slowestTableResolvedTsGauge)
	registry.MustRegister(slowestTableStateGauge)
	registry.MustRegister(slowestTableStageCheckpointTsGaugeVec)
	registry.MustRegister(slowestTableStageResolvedTsGaugeVec)
	registry.MustRegister(slowestTableStageCheckpointTsLagGaugeVec)
	registry.MustRegister(slowestTableStageResolvedTsLagGaugeVec)
	registry.MustRegister(slowestTableStageCheckpointTsLagHistogramVec)
	registry.MustRegister(slowestTableStageResolvedTsLagHistogramVec)
	registry.MustRegister(slowestTableRegionGaugeVec)
}
