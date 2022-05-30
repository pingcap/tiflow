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

package syncer

import (
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

// DMLWorker is used to sync dml.
type DMLWorker struct {
	batch         int
	workerCount   int
	chanSize      int
	multipleRows  bool
	toDBConns     []*dbconn.DBConn
	syncCtx       *tcontext.Context
	logger        log.Logger
	metricProxies *metrics.Proxies

	// for MetricsProxies
	task   string
	source string
	worker string

	// callback func
	// TODO: refine callback func
	successFunc          func(int, int, []*job)
	fatalFunc            func(*job, error)
	lagFunc              func(*job, int)
	updateJobMetricsFunc func(bool, string, *job)

	// channel
	inCh    chan *job
	flushCh chan *job
}

// dmlWorkerWrap creates and runs a dmlWorker instance and returns flush job channel.
func dmlWorkerWrap(inCh chan *job, syncer *Syncer) chan *job {
	chanSize := syncer.cfg.QueueSize / 2
	if syncer.cfg.Compact {
		chanSize /= 2
	}
	dmlWorker := &DMLWorker{
		batch:                syncer.cfg.Batch,
		workerCount:          syncer.cfg.WorkerCount,
		chanSize:             chanSize,
		multipleRows:         syncer.cfg.MultipleRows,
		task:                 syncer.cfg.Name,
		source:               syncer.cfg.SourceID,
		worker:               syncer.cfg.WorkerName,
		logger:               syncer.tctx.Logger.WithFields(zap.String("component", "dml_worker")),
		successFunc:          syncer.successFunc,
		fatalFunc:            syncer.fatalFunc,
		lagFunc:              syncer.updateReplicationJobTS,
		updateJobMetricsFunc: syncer.updateJobMetrics,
		syncCtx:              syncer.syncCtx, // this ctx can be used to cancel all the workers
		metricProxies:        syncer.metricsProxies,
		toDBConns:            syncer.toDBConns,
		inCh:                 inCh,
		flushCh:              make(chan *job),
	}

	go func() {
		dmlWorker.run()
		dmlWorker.close()
	}()
	return dmlWorker.flushCh
}

// close closes outer channel.
func (w *DMLWorker) close() {
	close(w.flushCh)
}

// run distribute jobs by queueBucket.
func (w *DMLWorker) run() {
	jobChs := make([]chan *job, w.workerCount)

	for i := 0; i < w.workerCount; i++ {
		jobChs[i] = make(chan *job, w.chanSize)
		go w.executeJobs(i, jobChs[i])
	}

	defer func() {
		for i := 0; i < w.workerCount; i++ {
			close(jobChs[i])
		}
	}()

	queueBucketMapping := make([]string, w.workerCount)
	for i := 0; i < w.workerCount; i++ {
		queueBucketMapping[i] = queueBucketName(i)
	}
	for j := range w.inCh {
		w.metricProxies.QueueSizeGauge.WithLabelValues(w.task, "dml_worker_input", w.source).Set(float64(len(w.inCh)))
		switch j.tp {
		case flush:
			w.updateJobMetricsFunc(false, adminQueueName, j)
			w.sendJobToAllDmlQueue(j, jobChs, queueBucketMapping)
			j.flushWg.Wait()
			w.updateJobMetricsFunc(true, adminQueueName, j)
			w.flushCh <- j
		case asyncFlush:
			w.updateJobMetricsFunc(false, adminQueueName, j)
			w.sendJobToAllDmlQueue(j, jobChs, queueBucketMapping)
			w.flushCh <- j
		case conflict:
			w.updateJobMetricsFunc(false, adminQueueName, j)
			w.sendJobToAllDmlQueue(j, jobChs, queueBucketMapping)
			j.flushWg.Wait()
			w.updateJobMetricsFunc(true, adminQueueName, j)
		default:
			queueBucket := int(utils.GenHashKey(j.dmlQueueKey)) % w.workerCount
			w.updateJobMetricsFunc(false, queueBucketMapping[queueBucket], j)
			startTime := time.Now()
			w.logger.Debug("queue for key", zap.Int("queue", queueBucket), zap.String("key", j.dmlQueueKey))
			jobChs[queueBucket] <- j
			w.metricProxies.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[queueBucket], w.source).Observe(time.Since(startTime).Seconds())
		}
	}
}

func (w *DMLWorker) sendJobToAllDmlQueue(j *job, jobChs []chan *job, queueBucketMapping []string) {
	// flush for every DML queue
	for i, jobCh := range jobChs {
		startTime := time.Now()
		jobCh <- j
		w.metricProxies.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[i], w.source).Observe(time.Since(startTime).Seconds())
	}
}

// executeJobs execute jobs in same queueBucket
// All the jobs received should be executed consecutively.
func (w *DMLWorker) executeJobs(queueID int, jobCh chan *job) {
	jobs := make([]*job, 0, w.batch)
	workerJobIdx := dmlWorkerJobIdx(queueID)
	queueBucket := queueBucketName(queueID)
	for j := range jobCh {
		w.metricProxies.QueueSizeGauge.WithLabelValues(w.task, queueBucket, w.source).Set(float64(len(jobCh)))

		if j.tp != flush && j.tp != asyncFlush && j.tp != conflict {
			if len(jobs) == 0 {
				// set job TS when received first job of this batch.
				w.lagFunc(j, workerJobIdx)
			}
			jobs = append(jobs, j)
			if len(jobs) < w.batch && len(jobCh) > 0 {
				continue
			}
		}

		failpoint.Inject("syncDMLBatchNotFull", func() {
			if len(jobCh) == 0 && len(jobs) < w.batch {
				w.logger.Info("execute not full job queue")
			}
		})

		w.executeBatchJobs(queueID, jobs)
		if j.tp == conflict || j.tp == flush || j.tp == asyncFlush {
			j.flushWg.Done()
		}

		jobs = jobs[0:0]
		if len(jobCh) == 0 {
			failpoint.Inject("noJobInQueueLog", func() {
				w.logger.Debug("no job in queue, update lag to zero", zap.Int(
					"workerJobIdx", workerJobIdx), zap.Int64("current ts", time.Now().Unix()))
			})
			w.lagFunc(nil, workerJobIdx)
		}
	}
}

// executeBatchJobs execute jobs with batch size.
func (w *DMLWorker) executeBatchJobs(queueID int, jobs []*job) {
	var (
		affect  int
		queries []string
		args    [][]interface{}
		db      = w.toDBConns[queueID]
		err     error
		dmls    = make([]*sqlmodel.RowChange, 0, len(jobs))
	)

	defer func() {
		if err == nil {
			w.successFunc(queueID, len(dmls), jobs)
		} else {
			if len(queries) == len(jobs) {
				w.fatalFunc(jobs[affect], err)
			} else {
				w.logger.Warn("length of queries not equals length of jobs, cannot determine which job failed", zap.Int("queries", len(queries)), zap.Int("jobs", len(jobs)))
				newJob := job{
					startLocation:   jobs[0].startLocation,
					currentLocation: jobs[len(jobs)-1].currentLocation,
				}
				w.fatalFunc(&newJob, err)
			}
		}
	}()

	if len(jobs) == 0 {
		return
	}
	failpoint.Inject("failSecondJob", func() {
		if failExecuteSQLForTest && failOnceForTest.CAS(false, true) {
			w.logger.Info("trigger failSecondJob")
			err = terror.ErrDBExecuteFailed.Delegate(errors.New("failSecondJob"), "mock")
			failpoint.Return()
		}
	})

	queries, args = w.genSQLs(jobs)
	failpoint.Inject("BlockExecuteSQLs", func(v failpoint.Value) {
		t := v.(int) // sleep time
		w.logger.Info("BlockExecuteSQLs", zap.Any("job", jobs[0]), zap.Int("sleep time", t))
		for _, query := range queries {
			if strings.Contains(query, "UPDATE") && strings.Contains(query, "MetricsProxies") {
				t = 10
				w.logger.Info("BlockExecuteSQLs block for update sleep 10s for MetricsProxies it test", zap.Any("query", query))
			}
		}
		time.Sleep(time.Second * time.Duration(t))
	})
	failpoint.Inject("WaitUserCancel", func(v failpoint.Value) {
		t := v.(int)
		time.Sleep(time.Duration(t) * time.Second)
	})
	// use background context to execute sqls as much as possible
	// set timeout to maxDMLConnectionDuration to make sure dmls can be replicated to downstream event if the latency is high
	// if users need to quit this asap, we can support pause-task/stop-task --force in the future
	ctx, cancel := w.syncCtx.WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	affect, err = db.ExecuteSQL(ctx, w.metricProxies, queries, args...)
	failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
		if intVal, ok := val.(int); ok && intVal == 4 && len(jobs) > 0 {
			w.logger.Warn("fail to exec DML", zap.String("failpoint", "SafeModeExit"))
			affect, err = 0, terror.ErrDBExecuteFailed.Delegate(errors.New("SafeModeExit"), "mock")
		}
	})

	failpoint.Inject("ErrorOnLastDML", func(_ failpoint.Value) {
		if len(queries) > len(jobs) {
			w.logger.Error("error on last queries", zap.Int("queries", len(queries)), zap.Int("jobs", len(jobs)))
			affect, err = len(queries)-1, terror.ErrDBExecuteFailed.Delegate(errors.New("ErrorOnLastDML"), "mock")
		}
	})
}

// genSQLs generate SQLs in single row mode or multiple rows mode.
func (w *DMLWorker) genSQLs(jobs []*job) ([]string, [][]interface{}) {
	if w.multipleRows {
		return genDMLsWithSameOp(jobs)
	}

	queries := make([]string, 0, len(jobs))
	args := make([][]interface{}, 0, len(jobs))
	for _, j := range jobs {
		var query string
		var arg []interface{}
		appendQueryAndArg := func() {
			queries = append(queries, query)
			args = append(args, arg)
		}

		switch j.dml.Type() {
		case sqlmodel.RowChangeInsert:
			if j.safeMode {
				query, arg = j.dml.GenSQL(sqlmodel.DMLReplace)
			} else {
				query, arg = j.dml.GenSQL(sqlmodel.DMLInsert)
			}

		case sqlmodel.RowChangeUpdate:
			if j.safeMode {
				query, arg = j.dml.GenSQL(sqlmodel.DMLDelete)
				appendQueryAndArg()
				query, arg = j.dml.GenSQL(sqlmodel.DMLReplace)
			} else {
				query, arg = j.dml.GenSQL(sqlmodel.DMLUpdate)
			}

		case sqlmodel.RowChangeDelete:
			query, arg = j.dml.GenSQL(sqlmodel.DMLDelete)
		}

		appendQueryAndArg()
	}
	return queries, args
}
