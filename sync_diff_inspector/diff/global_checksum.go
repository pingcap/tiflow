// Copyright 2026 PingCAP, Inc.
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

package diff

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type checksumTask struct {
	rangeInfo *splitter.RangeInfo
	seq       int
	count     int64
	checksum  uint64
}

func (df *Diff) shouldUseGlobalChecksum() bool {
	if df.exportFixSQL {
		return false
	}
	return df.upstream.PreferGlobalChecksum() && df.downstream.PreferGlobalChecksum()
}

func (df *Diff) equalByGlobalChecksum(ctx context.Context) error {
	tables := df.downstream.GetTables()
	startTableIndex := 0
	if df.checksumCheckpoint != nil {
		startTableIndex = df.checksumCheckpoint.TableIndex
		if df.checksumCheckpoint.Upstream.Done && df.checksumCheckpoint.Downstream.Done {
			if df.checksumCheckpoint.TableIndex >= len(tables)-1 {
				return nil
			}
			startTableIndex++
		}
	}

	for tableIndex := startTableIndex; tableIndex < len(tables); tableIndex++ {
		tableDiff := tables[tableIndex]
		schema, table := tableDiff.Schema, tableDiff.Table
		progressID := dbutil.TableName(schema, table)
		if tableDiff.IgnoreDataCheck {
			progress.StartTable(progressID, 1, true)
			progress.Inc(progressID)
			continue
		}

		chunkID := &chunk.CID{
			TableIndex:       tableIndex,
			BucketIndexLeft:  0,
			BucketIndexRight: 0,
			ChunkIndex:       0,
			ChunkCnt:         1,
		}

		if !common.AllTableExist(tableDiff.TableLack) {
			progress.StartTable(progressID, 1, true)
			lackRange := &splitter.RangeInfo{
				ChunkRange: &chunk.Range{
					Index: &chunk.CID{TableIndex: tableIndex},
				},
			}
			upCount := df.upstream.GetCountForLackTable(ctx, lackRange)
			downCount := df.downstream.GetCountForLackTable(ctx, lackRange)
			isEqual := upCount == downCount
			if !isEqual {
				progress.FailTable(progressID)
			}
			progress.Inc(progressID)
			df.report.SetTableDataCheckResult(schema, table, isEqual, int(upCount), int(downCount), upCount, downCount, chunkID)
			checkpointState := checkpoints.NewChecksumState(tableIndex)
			checkpointState.Upstream.Done = true
			checkpointState.Downstream.Done = true
			if err := df.flushChecksumCheckpoint(ctx, checkpointState); err != nil {
				log.Warn("failed to save checksum checkpoint", zap.Error(err))
			}
			df.checksumCheckpoint = checkpointState
			continue
		}

		checkpointState := checkpoints.NewChecksumState(tableIndex)
		if df.checksumCheckpoint != nil && df.checksumCheckpoint.TableIndex == tableIndex {
			checkpointState = df.checksumCheckpoint
		}

		var (
			upCount      int64
			upChecksum   uint64
			downCount    int64
			downChecksum uint64
		)

		flushCkpt := func(ctx context.Context) {
			if err := df.flushChecksumCheckpoint(ctx, checkpointState); err != nil {
				log.Warn("fail to flush checksum checkpoint", zap.Error(err))
			}
		}

		progress.StartTable(progressID, 1, false)
		eg, egCtx := errgroup.WithContext(ctx)
		flushDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			defer close(flushDone)

			for {
				select {
				case <-egCtx.Done():
					return
				case <-ticker.C:
					flushCkpt(egCtx)
				}
			}
		}()
		eg.Go(func() (err error) {
			upCount, upChecksum, err = df.getSourceGlobalChecksum(
				egCtx,
				df.upstream,
				tableIndex,
				checkpointState.Upstream,
				progressID,
			)
			return err
		})
		eg.Go(func() (err error) {
			downCount, downChecksum, err = df.getSourceGlobalChecksum(
				egCtx,
				df.downstream,
				tableIndex,
				checkpointState.Downstream,
				progressID,
			)
			return err
		})
		err := eg.Wait()
		<-flushDone

		if err != nil {
			progress.FailTable(progressID)
			df.report.SetTableMeetError(schema, table, err)
			// Retryable checksum execution errors should not be checkpointed as
			// data inequality, otherwise a later successful resume would inherit
			// a stale failed table result from the saved report snapshot.
			flushCkpt(ctx)
			df.checksumCheckpoint = checkpointState
			continue
		}

		equal := upCount == downCount && upChecksum == downChecksum
		if !equal {
			progress.FailTable(progressID)
			log.Debug("global checksum mismatch",
				zap.String("table", dbutil.TableName(schema, table)),
				zap.Int64("upstream count", upCount),
				zap.Int64("downstream count", downCount),
				zap.Uint64("upstream checksum", upChecksum),
				zap.Uint64("downstream checksum", downChecksum),
			)
		}
		progress.UpdateTotal(progressID, 0, true)
		progress.Inc(progressID)
		df.report.SetTableDataCheckResult(schema, table, equal, 0, 0, upCount, downCount, chunkID)

		// Safe without checksumCheckpointMu: eg.Wait() has returned, so the
		// ticker goroutine (the only concurrent reader) has already exited.
		checkpointState.Upstream.Done = true
		checkpointState.Downstream.Done = true
		flushCkpt(ctx)
		df.checksumCheckpoint = checkpointState
	}
	return nil
}

func produceChecksumTasks(
	ctx context.Context,
	iter splitter.ChunkIterator,
	tableIndex int,
	taskCh chan<- checksumTask,
) error {
	seq := 0
	for {
		chunkRange, err := iter.Next()
		if err != nil || chunkRange == nil {
			return errors.Trace(err)
		}
		chunkRange.Index.TableIndex = tableIndex
		select {
		case <-ctx.Done():
			return ctx.Err()
		case taskCh <- checksumTask{seq: seq, rangeInfo: &splitter.RangeInfo{ChunkRange: chunkRange}}:
			seq++
		}
	}
}

func runChecksumWorker(
	ctx context.Context,
	src source.Source,
	taskCh <-chan checksumTask,
	resultCh chan<- checksumTask,
) error {
	for {
		var task checksumTask
		select {
		case <-ctx.Done():
			return ctx.Err()
		case taskInfo, ok := <-taskCh:
			if !ok {
				return nil
			}
			task = taskInfo
		}

		info := src.GetCountAndMD5(ctx, task.rangeInfo)
		if info.Err != nil {
			return errors.Trace(info.Err)
		}

		result := checksumTask{
			seq:       task.seq,
			rangeInfo: task.rangeInfo,
			count:     info.Count,
			checksum:  info.Checksum,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultCh <- result:
		}
	}
}

func drainOrderedChecksumTasks(pending map[int]checksumTask, nextSeq *int, fn func(checksumTask)) {
	for {
		ordered, ok := pending[*nextSeq]
		if !ok {
			return
		}
		fn(ordered)
		delete(pending, *nextSeq)
		*nextSeq++
	}
}

func (df *Diff) getSourceGlobalChecksum(
	ctx context.Context,
	src source.Source,
	tableIndex int,
	state *checkpoints.ChecksumSourceState,
	progressID string,
) (int64, uint64, error) {
	if state.Done {
		return state.Count, state.Checksum, nil
	}

	var startRange *splitter.RangeInfo
	if state.LastRange != nil {
		startRange = &splitter.RangeInfo{ChunkRange: state.LastRange.Clone()}
	}

	iter, err := src.GetChecksumOnlyIterator(ctx, tableIndex, startRange)
	if err != nil {
		return -1, 0, errors.Trace(err)
	}
	defer iter.Close()

	totalCount := state.Count
	totalChecksum := state.Checksum
	concurrency := max(1, df.checkThreadCount)
	taskCh := make(chan checksumTask, concurrency)
	resultCh := make(chan checksumTask, concurrency)
	doneCh := make(chan struct{})

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(taskCh)
		return produceChecksumTasks(egCtx, iter, tableIndex, taskCh)
	})

	for range concurrency {
		eg.Go(func() error {
			return runChecksumWorker(egCtx, src, taskCh, resultCh)
		})
	}

	go func() {
		defer close(doneCh)

		nextSeq := 0
		pending := make(map[int]checksumTask, concurrency)
		for result := range resultCh {
			pending[result.seq] = result
			drainOrderedChecksumTasks(pending, &nextSeq, func(ordered checksumTask) {
				progress.UpdateTotal(progressID, 1, false)
				progress.Inc(progressID)
				totalCount += ordered.count
				totalChecksum ^= ordered.checksum

				df.checksumCheckpointMu.Lock()
				defer df.checksumCheckpointMu.Unlock()
				state.Count = totalCount
				state.Checksum = totalChecksum
				state.LastRange = ordered.rangeInfo.ChunkRange.Clone()
			})
		}
	}()

	err = eg.Wait()
	close(resultCh)
	<-doneCh

	if err != nil {
		return -1, 0, errors.Trace(err)
	}

	return totalCount, totalChecksum, nil
}
