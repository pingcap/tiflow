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

package metrics

import (
	"context"
	"time"

	cpu "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/dm/pkg/metricsproxy"
)

// used for ddlPendingCounter, no "Resolved" lock because they will be
// remove quickly and not pending anymore.
const (
	DDLPendingNone     = "None"
	DDLPendingUnSynced = "Un-synced"
	DDLPendingSynced   = "Synced"
)

// used to show error type when handle DDLs.
const (
	InfoErrSyncLock    = "InfoPut - SyncLockError"
	InfoErrHandleLock  = "InfoPut - HandleLockError"
	OpErrRemoveLock    = "OperationPut - RemoveLockError"
	OpErrLockUnSynced  = "OperationPut - LockUnSyncedError"
	OpErrPutNonOwnerOp = "OperationPut - PutNonOwnerOpError"
)

// used to represent worker event error type.
const (
	WorkerEventHandle = "handle"
	WorkerEventWatch  = "watch"
)

var (
	workerState = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "master",
			Name:      "worker_state",
			Help:      "state of worker, -1 - unrecognized, 0 - offline, 1 - free, 2 - bound",
		}, []string{"worker"})

	cpuUsageGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "master",
			Name:      "cpu_usage",
			Help:      "the cpu usage of master",
		})

	ddlPendingCounter = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "master",
			Name:      "ddl_state_number",
			Help:      "number of pending DDL in different states, Un-synced (waiting all upstream), Synced (all upstream finished, waiting all downstream)",
		}, []string{"task", "type"})

	ddlErrCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "master",
			Name:      "shard_ddl_error",
			Help:      "number of shard DDL lock/operation error",
		}, []string{"task", "type"})

	workerEventErrCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "master",
			Name:      "worker_event_error",
			Help:      "number of error related to worker event, during handling or watching",
		}, []string{"type"})

	startLeaderCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "master",
			Name:      "start_leader_counter",
			Help:      "number of this dm-master try to start leader components",
		})
)

func collectMetrics() {
	cpuUsage := cpu.GetCPUPercentage()
	cpuUsageGauge.Set(cpuUsage)
}

// RunBackgroundJob do periodic job.
func RunBackgroundJob(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			collectMetrics()

		case <-ctx.Done():
			return
		}
	}
}

// RegistryMetrics registries metrics for worker.
func RegistryMetrics() {
	registry := prometheus.DefaultRegisterer

	registry.MustRegister(workerState)
	registry.MustRegister(cpuUsageGauge)
	registry.MustRegister(ddlPendingCounter)
	registry.MustRegister(ddlErrCounter)
	registry.MustRegister(workerEventErrCounter)
	registry.MustRegister(startLeaderCounter)
}

// ReportWorkerStage is a setter for workerState.
func ReportWorkerStage(name string, state float64) {
	workerState.WithLabelValues(name).Set(state)
}

// RemoveWorkerState cleans state of deleted worker.
func RemoveWorkerState(name string) {
	workerState.DeleteAllAboutLabels(prometheus.Labels{"worker": name})
}

// ReportDDLPending inc/dec by 1 to ddlPendingCounter.
func ReportDDLPending(task, oldStatus, newStatus string) {
	if oldStatus != DDLPendingNone {
		ddlPendingCounter.WithLabelValues(task, oldStatus).Dec()
	}
	if newStatus != DDLPendingNone {
		ddlPendingCounter.WithLabelValues(task, newStatus).Inc()
	}
}

// RemoveDDLPending removes all counter of this task.
func RemoveDDLPending(task string) {
	ddlPendingCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}

// ReportDDLError is a setter for ddlErrCounter.
func ReportDDLError(task, errType string) {
	ddlErrCounter.WithLabelValues(task, errType).Inc()
}

// ReportWorkerEventErr is a setter for workerEventErrCounter.
func ReportWorkerEventErr(errType string) {
	workerEventErrCounter.WithLabelValues(errType).Inc()
}

// ReportStartLeader increases startLeaderCounter by one.
func ReportStartLeader() {
	startLeaderCounter.Inc()
}

// OnRetireLeader cleans some metrics when retires.
func OnRetireLeader() {
	workerState.Reset()
	ddlErrCounter.Reset()
	ddlPendingCounter.Reset()
	workerEventErrCounter.Reset()
}
