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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

type checkpointFlushTask struct {
	snapshotInfo *SnapshotInfo
	// extra sharding ddl sqls
	exceptTables  []*filter.Table
	shardMetaSQLs []string
	shardMetaArgs [][]interface{}
	// async flush job
	asyncflushJob *job
	// error chan for sync flush
	syncFlushErrCh chan error
}

type checkpointFlushWorker struct {
	input              chan *checkpointFlushTask
	cp                 CheckPoint
	execError          *atomic.Error
	afterFlushFn       func(task *checkpointFlushTask) error
	updateJobMetricsFn func(bool, string, *job)
}

// Add add a new flush checkpoint job.
func (w *checkpointFlushWorker) Add(msg *checkpointFlushTask) {
	w.input <- msg
}

// Run read flush tasks from input and execute one by one.
func (w *checkpointFlushWorker) Run(ctx *tcontext.Context) {
	for task := range w.input {
		isAsyncFlush := task.asyncflushJob != nil

		// for async checkpoint flush, it waits all worker finish execute flush job
		// for sync checkpoint flush, it skips waiting here because
		// we waits all worker finish execute flush job before adding flushCPTask into flush worker
		if isAsyncFlush {
			task.asyncflushJob.flushWg.Wait()

			w.updateJobMetricsFn(true, adminQueueName, task.asyncflushJob)
			ctx.L().Info("async flush checkpoint snapshot job has been processed by dml worker, about to flush checkpoint snapshot", zap.Int64("job sequence", task.asyncflushJob.flushSeq), zap.Int("snapshot_id", task.snapshotInfo.id))
		} else {
			ctx.L().Info("about to sync flush checkpoint snapshot", zap.Int("snapshot_id", task.snapshotInfo.id))
		}

		flushLogMsg := "sync flush"
		if isAsyncFlush {
			flushLogMsg = "async flush"
		}

		err := w.execError.Load()
		// TODO: for now, if any error occurred (including user canceled), checkpoint won't be updated. But if we have put
		// optimistic shard info, DM-master may resolved the optimistic lock and let other worker execute DDL. So after this
		// worker resume, it can not execute the DML/DDL in old binlog because of downstream table structure mismatching.
		// We should find a way to (compensating) implement a transaction containing interaction with both etcd and SQL.
		if err != nil && (terror.ErrDBExecuteFailed.Equal(err) || terror.ErrDBUnExpect.Equal(err)) {
			ctx.L().Warn(fmt.Sprintf("error detected when executing SQL job, skip %s checkpoint and shutdown checkpointFlushWorker", flushLogMsg),
				zap.Stringer("globalPos", task.snapshotInfo.globalPos),
				zap.Error(err))

			if !isAsyncFlush {
				task.syncFlushErrCh <- nil
			}
			return
		}

		err = w.cp.FlushPointsExcept(ctx, task.snapshotInfo.id, task.exceptTables, task.shardMetaSQLs, task.shardMetaArgs)
		failpoint.Inject("AsyncCheckpointFlushThrowError", func() {
			if isAsyncFlush {
				ctx.L().Warn("async checkpoint flush error triggered", zap.String("failpoint", "AsyncCheckpointFlushThrowError"))
				err = errors.New("async checkpoint flush throw error")
			}
		})

		if err != nil {
			ctx.L().Warn(fmt.Sprintf("%s checkpoint snapshot failed, ignore this error", flushLogMsg), zap.Any("flushCpTask", task), zap.Error(err))
			// async flush error will be skipped here but sync flush error will raised up
			if !isAsyncFlush {
				task.syncFlushErrCh <- err
			}
			continue
		}

		ctx.L().Info(fmt.Sprintf("%s checkpoint snapshot successfully", flushLogMsg), zap.Int("snapshot_id", task.snapshotInfo.id),
			zap.Stringer("pos", task.snapshotInfo.globalPos))
		if err = w.afterFlushFn(task); err != nil {
			ctx.L().Warn(fmt.Sprintf("%s post-process(afterFlushFn) failed", flushLogMsg), zap.Error(err))
		}

		// async flush error will be skipped here but sync flush error will raised up
		if !isAsyncFlush {
			task.syncFlushErrCh <- err
		}
	}
}

// Close wait all pending job finish and stop this worker.
func (w *checkpointFlushWorker) Close() {
	close(w.input)
}
