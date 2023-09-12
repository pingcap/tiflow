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

package sinkmanager

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type redoWorker struct {
	changefeedID  model.ChangeFeedID
	sourceManager *sourcemanager.SourceManager
	memQuota      *memquota.MemQuota
	redoDMLMgr    redo.DMLManager
	eventCache    *redoEventCache
	splitTxn      bool
}

func newRedoWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	quota *memquota.MemQuota,
	redoDMLMgr redo.DMLManager,
	eventCache *redoEventCache,
	splitTxn bool,
) *redoWorker {
	return &redoWorker{
		changefeedID:  changefeedID,
		sourceManager: sourceManager,
		memQuota:      quota,
		redoDMLMgr:    redoDMLMgr,
		eventCache:    eventCache,
		splitTxn:      splitTxn,
	}
}

func (w *redoWorker) handleTasks(ctx context.Context, taskChan <-chan *redoTask) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-taskChan:
			if err := w.handleTask(ctx, task); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *redoWorker) handleTask(ctx context.Context, task *redoTask) (finalErr error) {
	lowerBound := task.lowerBound
	upperBound := task.getUpperBound(task.tableSink.getReceivedSorterResolvedTs())
	lowerPhs := oracle.GetTimeFromTS(lowerBound.CommitTs)
	upperPhs := oracle.GetTimeFromTS(upperBound.CommitTs)
	if upperPhs.Sub(lowerPhs) > maxTaskRange {
		upperCommitTs := oracle.GoTimeToTS(lowerPhs.Add(maxTaskRange))
		upperBound = engine.Position{
			StartTs:  upperCommitTs - 1,
			CommitTs: upperCommitTs,
		}
	}

	if !upperBound.IsCommitFence() {
		log.Panic("redo task upperbound must be a ResolvedTs",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Any("upperBound", upperBound))
	}

	var cache *eventAppender
	if w.eventCache != nil {
		cache = w.eventCache.maybeCreateAppender(task.tableID, lowerBound)
	}

	// Events are pushed into redoEventCache if possible. Otherwise, their memory will
	// be released after they are written into redo files. Then we need to release their
	// memory quota, which can be calculated based on rowsSize and cachedSize.
	rowsSize := uint64(0)
	cachedSize := uint64(0)
	rows := make([]*model.RowChangedEvent, 0, 1024)

	availableMemSize := requestMemSize
	usedMemSize := uint64(0)

	// Only used to calculate lowerBound when next time the table is scheduled.
	var lastPos engine.Position

	// To calculate advance the downstream to where.
	emitedCommitTs := uint64(0)  // Has been sent by `redoDMLMgr.UpdateResolvedTs`.
	lastTxnCommitTs := uint64(0) // Can be used in next `redoDMLMgr.UpdateResolvedTs`.
	currTxnCommitTs := uint64(0) // Still in pulling, not complete.

	doEmitBatchEvents := func() error {
		if len(rows) > 0 {
			var releaseMem func()
			if rowsSize-cachedSize > 0 {
				refundMem := rowsSize - cachedSize
				releaseMem = func() {
					w.memQuota.Refund(refundMem)
					log.Debug("MemoryQuotaTracing: refund memory for redo log task",
						zap.String("namespace", w.changefeedID.Namespace),
						zap.String("changefeed", w.changefeedID.ID),
						zap.Int64("tableID", task.tableID),
						zap.Uint64("memory", refundMem))
				}
			}
			err := w.redoDMLMgr.EmitRowChangedEvents(ctx, task.tableID, releaseMem, rows...)
			if err != nil {
				return errors.Trace(err)
			}
			// Should always re-allocate space because EmitRowChangedEvents is asynchronous.
			rows = make([]*model.RowChangedEvent, 0, 1024)
			rowsSize = 0
			cachedSize = 0
		}
		if lastTxnCommitTs > emitedCommitTs {
			if err := w.redoDMLMgr.UpdateResolvedTs(ctx, task.tableID, lastTxnCommitTs); err != nil {
				return errors.Trace(err)
			}
			log.Debug("update resolved ts to redo",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Uint64("resolvedTs", lastTxnCommitTs))
			emitedCommitTs = lastTxnCommitTs
		}
		return nil
	}

	maybeEmitBatchEvents := func(allFinished, txnFinished bool) error {
		// If used memory size exceeds the required limit, do a force acquire.
		memoryHighUsage := availableMemSize < usedMemSize
		if memoryHighUsage {
			w.memQuota.ForceAcquire(usedMemSize - availableMemSize)
			log.Debug("MemoryQuotaTracing: force acquire memory for redo log task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Uint64("memory", usedMemSize-availableMemSize))
			availableMemSize = usedMemSize
		}

		// Do emit in such situations:
		// 1. we use more memory than we required;
		// 2. the pending batch size exceeds maxUpdateIntervalSize;
		// 3. all events are received.
		if memoryHighUsage || rowsSize >= maxUpdateIntervalSize || allFinished {
			if err := doEmitBatchEvents(); err != nil {
				return errors.Trace(err)
			}
		}

		if allFinished {
			return nil
		}
		if usedMemSize >= availableMemSize {
			if txnFinished {
				if w.memQuota.TryAcquire(requestMemSize) {
					availableMemSize += requestMemSize
					log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
						zap.String("namespace", w.changefeedID.Namespace),
						zap.String("changefeed", w.changefeedID.ID),
						zap.Int64("tableID", task.tableID),
						zap.Uint64("memory", requestMemSize))
				}
			} else {
				// NOTE: it's not required to use `forceAcquire` even if splitTxn is false.
				// It's because memory will finally be `refund` after redo-logs are written.
				err := w.memQuota.BlockAcquire(requestMemSize)
				if err != nil {
					return errors.Trace(err)
				}
				availableMemSize += requestMemSize
				log.Debug("MemoryQuotaTracing: block acquire memory for redo log task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", requestMemSize))
			}
		}

		return nil
	}

	iter := w.sourceManager.FetchByTable(task.tableID, lowerBound, upperBound, w.memQuota)
	allEventCount := 0
	defer func() {
		eventCount := newRangeEventCount(lastPos, allEventCount)
		task.tableSink.updateRangeEventCounts(eventCount)

		if err := iter.Close(); err != nil {
			log.Error("redo worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Error(err))
		}

		log.Debug("redo task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Any("lastPos", lastPos),
			zap.Float64("lag", time.Since(oracle.GetTimeFromTS(lastPos.CommitTs)).Seconds()),
			zap.Error(finalErr))

		if finalErr == nil {
			// Otherwise we can't ensure all events before `lastPos` are emitted.
			task.callback(lastPos)
		}

		// There is no more events and some required memory isn't used.
		if availableMemSize > usedMemSize {
			w.memQuota.Refund(availableMemSize - usedMemSize)
			log.Debug("MemoryQuotaTracing: refund memory for redo log task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Uint64("memory", availableMemSize-usedMemSize))
		}
	}()

	for availableMemSize > usedMemSize && !task.isCanceled() {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// There is no more data. It means that we finish this scan task.
		if e == nil {
			lastPos = upperBound
			lastTxnCommitTs = upperBound.CommitTs
			if cache != nil {
				// Still need to update cache upper boundary even if no events.
				cache.pushBatch(nil, 0, lastPos)
			}
			return maybeEmitBatchEvents(true, true)
		}
		allEventCount += 1

		if pos.Valid() {
			lastPos = pos
		}
		if currTxnCommitTs != e.CRTs {
			lastTxnCommitTs = currTxnCommitTs
			currTxnCommitTs = e.CRTs
		}
		if pos.IsCommitFence() {
			lastTxnCommitTs = currTxnCommitTs
		}

		// NOTICE: The event can be filtered by the event filter.
		var x []*model.RowChangedEvent
		var size uint64
		if e.Row != nil {
			// For all rows, we add table replicate ts, so mysql sink can determine safe-mode.
			e.Row.ReplicatingTs = task.tableSink.replicateTs
			x, size = handleRowChangedEvents(w.changefeedID, task.tableID, e)
			usedMemSize += size
			rows = append(rows, x...)
			rowsSize += size
		}
		if cache != nil {
			cached, brokenSize := cache.pushBatch(x, size, pos)
			if cached {
				cachedSize += size
			} else {
				cachedSize -= brokenSize
			}
		}

		if err := maybeEmitBatchEvents(false, pos.Valid()); err != nil {
			return errors.Trace(err)
		}
	}
	// Even if task is canceled we still call this again, to avoid somethings
	// are left and leak forever.
	return doEmitBatchEvents()
}
