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

	"github.com/pingcap/tiflow/dm/pkg/metricsproxy"
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
	binlogReadDurationHistogram     *metricsproxy.HistogramVecProxy
	binlogEventSizeHistogram        *metricsproxy.HistogramVecProxy
	BinlogEventCost                 *metricsproxy.HistogramVecProxy
	conflictDetectDurationHistogram *metricsproxy.HistogramVecProxy
	AddJobDurationHistogram         *metricsproxy.HistogramVecProxy
	// dispatch/add multiple jobs for one binlog event.
	// NOTE: only observe for DML now.
	DispatchBinlogDurationHistogram *metricsproxy.HistogramVecProxy
	SkipBinlogDurationHistogram     *metricsproxy.HistogramVecProxy
	AddedJobsTotal                  *metricsproxy.CounterVecProxy
	FinishedJobsTotal               *metricsproxy.CounterVecProxy
	idealQPS                        *metricsproxy.GaugeVecProxy
	QueueSizeGauge                  *metricsproxy.GaugeVecProxy
	binlogPosGauge                  *metricsproxy.GaugeVecProxy
	binlogFileGauge                 *metricsproxy.GaugeVecProxy
	binlogEventRowHistogram         *metricsproxy.HistogramVecProxy
	txnHistogram                    *metricsproxy.HistogramVecProxy
	queryHistogram                  *metricsproxy.HistogramVecProxy
	StmtHistogram                   *metricsproxy.HistogramVecProxy
	syncerExitWithErrorCounter      *metricsproxy.CounterVecProxy
	replicationLagGauge             *metricsproxy.GaugeVecProxy
	replicationLagHistogram         *metricsproxy.HistogramVecProxy
	remainingTimeGauge              *metricsproxy.GaugeVecProxy
	UnsyncedTableGauge              *metricsproxy.GaugeVecProxy
	shardLockResolving              *metricsproxy.GaugeVecProxy
	finishedTransactionTotal        *metricsproxy.CounterVecProxy
	ReplicationTransactionBatch     *metricsproxy.HistogramVecProxy
	flushCheckPointsTimeInterval    *metricsproxy.HistogramVecProxy
}

var DefaultMetricsProxies *Proxies

func init() {
	DefaultMetricsProxies = &Proxies{}
	DefaultMetricsProxies.Init(&promutil.PromFactory{})
}

// Init creates Metrics proxy variables from Factory.
func (m *Proxies) Init(f promutil.Factory) {
	m.binlogReadDurationHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "read_binlog_duration",
			Help:      "bucketed histogram of read time (s) for single binlog event from the relay log or master.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})
	m.binlogEventSizeHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_size",
			Help:      "size of a binlog event",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"task", "worker", "source_id"})
	m.BinlogEventCost = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_transform_cost",
			Help:      "cost of binlog event transform",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"stage", "task", "worker", "source_id"})
	m.conflictDetectDurationHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "conflict_detect_duration",
			Help:      "bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})
	m.AddJobDurationHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "add_job_duration",
			Help:      "bucketed histogram of add a job to the queue time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "queueNo", "source_id"})
	m.DispatchBinlogDurationHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "dispatch_binlog_duration",
			Help:      "bucketed histogram of dispatch a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "source_id"})
	m.SkipBinlogDurationHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "skip_binlog_duration",
			Help:      "bucketed histogram of skip a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.0000005, 2, 25), // this should be very fast.
		}, []string{"type", "task", "source_id"})
	m.AddedJobsTotal = metricsproxy.NewCounterVec(f,
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "added_jobs_total",
			Help:      "total number of added jobs",
		}, []string{"type", "task", "queueNo", "source_id", "worker", "target_schema", "target_table"})
	m.FinishedJobsTotal = metricsproxy.NewCounterVec(f,
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_jobs_total",
			Help:      "total number of finished jobs",
		}, []string{"type", "task", "queueNo", "source_id", "worker", "target_schema", "target_table"})
	m.idealQPS = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "ideal_qps",
			Help:      "the highest QPS that can be achieved ideally",
		}, []string{"task", "worker", "source_id"})
	m.QueueSizeGauge = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "queue_size",
			Help:      "remain size of the DML queue",
		}, []string{"task", "queue_id", "source_id"})
	m.binlogPosGauge = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node", "task", "source_id"})
	m.binlogFileGauge = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node", "task", "source_id"})
	m.binlogEventRowHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_row",
			Help:      "number of rows in a binlog event",
			Buckets:   prometheus.LinearBuckets(0, 100, 101), // linear from 0 to 10000, i think this is enough
		}, []string{"worker", "task", "source_id"})
	m.txnHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id"})
	m.queryHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id"})
	m.StmtHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})
	m.syncerExitWithErrorCounter = metricsproxy.NewCounterVec(f,
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "exit_with_error_count",
			Help:      "counter for syncer exits with error",
		}, []string{"task", "source_id"})
	m.replicationLagGauge = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_lag_gauge",
			Help:      "replication lag gauge in second between mysql and syncer",
		}, []string{"task", "source_id", "worker"})
	m.replicationLagHistogram = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_lag",
			Help:      "replication lag histogram in second between mysql and syncer",
			Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // exponential from 0.5s to 1024s
		}, []string{"task", "source_id", "worker"})
	m.remainingTimeGauge = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "remaining_time",
			Help:      "the remaining time in second to catch up master",
		}, []string{"task", "source_id", "worker"})
	m.UnsyncedTableGauge = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "unsynced_table_number",
			Help:      "number of unsynced tables in the subtask",
		}, []string{"task", "table", "source_id"})
	m.shardLockResolving = metricsproxy.NewGaugeVec(f,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "shard_lock_resolving",
			Help:      "waiting shard DDL lock to be resolved",
		}, []string{"task", "source_id"})
	m.finishedTransactionTotal = metricsproxy.NewCounterVec(f,
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_transaction_total",
			Help:      "total number of finished transaction",
		}, []string{"task", "worker", "source_id"})
	m.ReplicationTransactionBatch = metricsproxy.NewHistogramVec(f,
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_transaction_batch",
			Help:      "number of sql's contained in a transaction that executed to downstream",
			Buckets:   prometheus.LinearBuckets(1, 50, 21), // linear from 1 to 1001
		}, []string{"worker", "task", "source_id", "queueNo", "type"})
	m.flushCheckPointsTimeInterval = metricsproxy.NewHistogramVec(f,
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
	m.binlogReadDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.binlogEventSizeHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.BinlogEventCost.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.binlogEventRowHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.conflictDetectDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.AddJobDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.DispatchBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.SkipBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.AddedJobsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.FinishedJobsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.QueueSizeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.binlogPosGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.binlogFileGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.txnHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.StmtHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.queryHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.syncerExitWithErrorCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.replicationLagGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.replicationLagHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.remainingTimeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.UnsyncedTableGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.shardLockResolving.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.idealQPS.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.finishedTransactionTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.ReplicationTransactionBatch.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	m.flushCheckPointsTimeInterval.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}
