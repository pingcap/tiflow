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

package metrics

import (
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

// for BinlogEventCost metric stage field.
const (
	BinlogEventCostStageDDLExec = "ddl-exec"
	BinlogEventCostStageDMLExec = "dml-exec"

	BinlogEventCostStageGenWriteRows  = "gen-write-rows"
	BinlogEventCostStageGenUpdateRows = "gen-update-rows"
	BinlogEventCostStageGenDeleteRows = "gen-delete-rows"
	BinlogEventCostStageGenQuery      = "gen-query"
)

// Metrics groups syncer's metric variables.
type Metrics struct {
	BinlogReadDurationHistogram     prometheus.Observer
	BinlogEventSizeHistogram        prometheus.Observer
	ConflictDetectDurationHistogram prometheus.Observer
	IdealQPS                        prometheus.Gauge
	BinlogMasterPosGauge            prometheus.Gauge
	BinlogSyncerPosGauge            prometheus.Gauge
	BinlogMasterFileGauge           prometheus.Gauge
	BinlogSyncerFileGauge           prometheus.Gauge
	BinlogEventRowHistogram         prometheus.Observer
	TxnHistogram                    prometheus.Observer
	QueryHistogram                  prometheus.Observer
	SyncerExitWithErrorCounter      prometheus.Counter
	ReplicationLagGauge             prometheus.Gauge
	ReplicationLagHistogram         prometheus.Observer
	RemainingTimeGauge              prometheus.Gauge
	ShardLockResolving              prometheus.Gauge
	FinishedTransactionTotal        prometheus.Counter
	FlushCheckPointsTimeInterval    prometheus.Observer
}

// Proxies provides the ability to clean Metrics values when syncer is closed.
// private members have a corresponding cached variable in Metrics.
type Proxies struct {
	Metrics                         *Metrics
	binlogReadDurationHistogram     *prometheus.HistogramVec
	binlogEventSizeHistogram        *prometheus.HistogramVec
	BinlogEventCost                 *prometheus.HistogramVec
	conflictDetectDurationHistogram *prometheus.HistogramVec
	AddJobDurationHistogram         *prometheus.HistogramVec
	// dispatch/add multiple jobs for one binlog event.
	// NOTE: only observe for DML now.
	DispatchBinlogDurationHistogram *prometheus.HistogramVec
	SkipBinlogDurationHistogram     *prometheus.HistogramVec
	AddedJobsTotal                  *prometheus.CounterVec
	FinishedJobsTotal               *prometheus.CounterVec
	idealQPS                        *prometheus.GaugeVec
	QueueSizeGauge                  *prometheus.GaugeVec
	binlogPosGauge                  *prometheus.GaugeVec
	binlogFileGauge                 *prometheus.GaugeVec
	binlogEventRowHistogram         *prometheus.HistogramVec
	txnHistogram                    *prometheus.HistogramVec
	queryHistogram                  *prometheus.HistogramVec
	StmtHistogram                   *prometheus.HistogramVec
	syncerExitWithErrorCounter      *prometheus.CounterVec
	replicationLagGauge             *prometheus.GaugeVec
	replicationLagHistogram         *prometheus.HistogramVec
	remainingTimeGauge              *prometheus.GaugeVec
	UnsyncedTableGauge              *prometheus.GaugeVec
	shardLockResolving              *prometheus.GaugeVec
	finishedTransactionTotal        *prometheus.CounterVec
	ReplicationTransactionBatch     *prometheus.HistogramVec
	flushCheckPointsTimeInterval    *prometheus.HistogramVec
}

var DefaultMetricsProxies *Proxies

func init() {
	DefaultMetricsProxies = &Proxies{}
	DefaultMetricsProxies.Init(&promutil.PromFactory{})
}

// Init creates Metrics proxy variables from Factory.
func (m *Proxies) Init(f promutil.Factory) {
	m.binlogReadDurationHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "read_binlog_duration",
			Help:      "bucketed histogram of read time (s) for single binlog event from the relay log or master.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})
	m.binlogEventSizeHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_size",
			Help:      "size of a binlog event",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"task", "worker", "source_id"})
	m.BinlogEventCost = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_transform_cost",
			Help:      "cost of binlog event transform",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"stage", "task", "worker", "source_id"})
	m.conflictDetectDurationHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "conflict_detect_duration",
			Help:      "bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})
	m.AddJobDurationHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "add_job_duration",
			Help:      "bucketed histogram of add a job to the queue time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "queueNo", "source_id"})
	m.DispatchBinlogDurationHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "dispatch_binlog_duration",
			Help:      "bucketed histogram of dispatch a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "source_id"})
	m.SkipBinlogDurationHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "skip_binlog_duration",
			Help:      "bucketed histogram of skip a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.0000005, 2, 25), // this should be very fast.
		}, []string{"type", "task", "source_id"})
	m.AddedJobsTotal = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "added_jobs_total",
			Help:      "total number of added jobs",
		}, []string{"type", "task", "queueNo", "source_id", "worker", "target_schema", "target_table"})
	m.FinishedJobsTotal = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_jobs_total",
			Help:      "total number of finished jobs",
		}, []string{"type", "task", "queueNo", "source_id", "worker", "target_schema", "target_table"})
	m.idealQPS = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "ideal_qps",
			Help:      "the highest QPS that can be achieved ideally",
		}, []string{"task", "worker", "source_id"})
	m.QueueSizeGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "queue_size",
			Help:      "remain size of the DML queue",
		}, []string{"task", "queue_id", "source_id"})
	m.binlogPosGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node", "task", "source_id"})
	m.binlogFileGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node", "task", "source_id"})
	m.binlogEventRowHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_row",
			Help:      "number of rows in a binlog event",
			Buckets:   prometheus.LinearBuckets(0, 100, 101), // linear from 0 to 10000, i think this is enough
		}, []string{"worker", "task", "source_id"})
	m.txnHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id"})
	m.queryHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id"})
	m.StmtHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})
	m.syncerExitWithErrorCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "exit_with_error_count",
			Help:      "counter for syncer exits with error",
		}, []string{"task", "source_id"})
	m.replicationLagGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_lag_gauge",
			Help:      "replication lag gauge in second between mysql and syncer",
		}, []string{"task", "source_id", "worker"})
	m.replicationLagHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_lag",
			Help:      "replication lag histogram in second between mysql and syncer",
			Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // exponential from 0.5s to 1024s
		}, []string{"task", "source_id", "worker"})
	m.remainingTimeGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "remaining_time",
			Help:      "the remaining time in second to catch up master",
		}, []string{"task", "source_id", "worker"})
	m.UnsyncedTableGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "unsynced_table_number",
			Help:      "number of unsynced tables in the subtask",
		}, []string{"task", "table", "source_id"})
	m.shardLockResolving = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "shard_lock_resolving",
			Help:      "waiting shard DDL lock to be resolved",
		}, []string{"task", "source_id"})
	m.finishedTransactionTotal = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_transaction_total",
			Help:      "total number of finished transaction",
		}, []string{"task", "worker", "source_id"})
	m.ReplicationTransactionBatch = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_transaction_batch",
			Help:      "number of sql's contained in a transaction that executed to downstream",
			Buckets:   prometheus.LinearBuckets(1, 50, 21), // linear from 1 to 1001
		}, []string{"worker", "task", "source_id", "queueNo", "type"})
	m.flushCheckPointsTimeInterval = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "flush_checkpoints_time_interval",
			Help:      "checkpoint flushed time interval in seconds",
			Buckets:   prometheus.LinearBuckets(1, 50, 21), // linear from 1 to 1001, i think this is enough
		}, []string{"worker", "task", "source_id"})
}

// CacheForOneTask returns a new Proxies with m.Metrics filled. It is used
// to avoid calling WithLabelValues in hot path.
func (m *Proxies) CacheForOneTask(taskName, workerName, sourceID string) *Proxies {
	ret := *m
	ret.Metrics = &Metrics{}
	ret.Metrics.BinlogReadDurationHistogram = m.binlogReadDurationHistogram.WithLabelValues(taskName, sourceID)
	ret.Metrics.BinlogEventSizeHistogram = m.binlogEventSizeHistogram.WithLabelValues(taskName, workerName, sourceID)
	ret.Metrics.ConflictDetectDurationHistogram = m.conflictDetectDurationHistogram.WithLabelValues(taskName, sourceID)
	ret.Metrics.IdealQPS = m.idealQPS.WithLabelValues(taskName, workerName, sourceID)
	ret.Metrics.BinlogMasterPosGauge = m.binlogPosGauge.WithLabelValues("master", taskName, sourceID)
	ret.Metrics.BinlogSyncerPosGauge = m.binlogPosGauge.WithLabelValues("syncer", taskName, sourceID)
	ret.Metrics.BinlogMasterFileGauge = m.binlogFileGauge.WithLabelValues("master", taskName, sourceID)
	ret.Metrics.BinlogSyncerFileGauge = m.binlogFileGauge.WithLabelValues("syncer", taskName, sourceID)
	ret.Metrics.BinlogEventRowHistogram = m.binlogEventRowHistogram.WithLabelValues(workerName, taskName, sourceID)
	ret.Metrics.TxnHistogram = m.txnHistogram.WithLabelValues(taskName, workerName, sourceID)
	ret.Metrics.QueryHistogram = m.queryHistogram.WithLabelValues(taskName, workerName, sourceID)
	ret.Metrics.SyncerExitWithErrorCounter = m.syncerExitWithErrorCounter.WithLabelValues(taskName, sourceID)
	ret.Metrics.ReplicationLagGauge = m.replicationLagGauge.WithLabelValues(taskName, sourceID, workerName)
	ret.Metrics.ReplicationLagHistogram = m.replicationLagHistogram.WithLabelValues(taskName, sourceID, workerName)
	ret.Metrics.RemainingTimeGauge = m.remainingTimeGauge.WithLabelValues(taskName, sourceID, workerName)
	ret.Metrics.ShardLockResolving = m.shardLockResolving.WithLabelValues(taskName, sourceID)
	ret.Metrics.FinishedTransactionTotal = m.finishedTransactionTotal.WithLabelValues(taskName, workerName, sourceID)
	ret.Metrics.FlushCheckPointsTimeInterval = m.flushCheckPointsTimeInterval.WithLabelValues(workerName, taskName, sourceID)
	return &ret
}

// RegisterMetrics registers Proxies.
func (m *Proxies) RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(m.binlogReadDurationHistogram)
	registry.MustRegister(m.binlogEventSizeHistogram)
	registry.MustRegister(m.BinlogEventCost)
	registry.MustRegister(m.binlogEventRowHistogram)
	registry.MustRegister(m.conflictDetectDurationHistogram)
	registry.MustRegister(m.AddJobDurationHistogram)
	registry.MustRegister(m.DispatchBinlogDurationHistogram)
	registry.MustRegister(m.SkipBinlogDurationHistogram)
	registry.MustRegister(m.AddedJobsTotal)
	registry.MustRegister(m.FinishedJobsTotal)
	registry.MustRegister(m.QueueSizeGauge)
	registry.MustRegister(m.binlogPosGauge)
	registry.MustRegister(m.binlogFileGauge)
	registry.MustRegister(m.txnHistogram)
	registry.MustRegister(m.StmtHistogram)
	registry.MustRegister(m.queryHistogram)
	registry.MustRegister(m.syncerExitWithErrorCounter)
	registry.MustRegister(m.replicationLagGauge)
	registry.MustRegister(m.replicationLagHistogram)
	registry.MustRegister(m.remainingTimeGauge)
	registry.MustRegister(m.UnsyncedTableGauge)
	registry.MustRegister(m.shardLockResolving)
	registry.MustRegister(m.idealQPS)
	registry.MustRegister(m.finishedTransactionTotal)
	registry.MustRegister(m.ReplicationTransactionBatch)
	registry.MustRegister(m.flushCheckPointsTimeInterval)
}

// RemoveLabelValuesWithTaskInMetrics cleans all Metrics related to the task.
func (m *Proxies) RemoveLabelValuesWithTaskInMetrics(task string) {
	m.binlogReadDurationHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.binlogEventSizeHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.BinlogEventCost.DeletePartialMatch(prometheus.Labels{"task": task})
	m.binlogEventRowHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.conflictDetectDurationHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.AddJobDurationHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.DispatchBinlogDurationHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.SkipBinlogDurationHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.AddedJobsTotal.DeletePartialMatch(prometheus.Labels{"task": task})
	m.FinishedJobsTotal.DeletePartialMatch(prometheus.Labels{"task": task})
	m.QueueSizeGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	m.binlogPosGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	m.binlogFileGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	m.txnHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.StmtHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.queryHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.syncerExitWithErrorCounter.DeletePartialMatch(prometheus.Labels{"task": task})
	m.replicationLagGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	m.replicationLagHistogram.DeletePartialMatch(prometheus.Labels{"task": task})
	m.remainingTimeGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	m.UnsyncedTableGauge.DeletePartialMatch(prometheus.Labels{"task": task})
	m.shardLockResolving.DeletePartialMatch(prometheus.Labels{"task": task})
	m.idealQPS.DeletePartialMatch(prometheus.Labels{"task": task})
	m.finishedTransactionTotal.DeletePartialMatch(prometheus.Labels{"task": task})
	m.ReplicationTransactionBatch.DeletePartialMatch(prometheus.Labels{"task": task})
	m.flushCheckPointsTimeInterval.DeletePartialMatch(prometheus.Labels{"task": task})
}
