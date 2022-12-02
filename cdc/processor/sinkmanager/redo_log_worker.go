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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/redo"
	"go.uber.org/zap"
)

type redoWorker struct {
	changefeedID   model.ChangeFeedID
	sourceManager  *sourcemanager.SourceManager
	memQuota       *memQuota
	redoManager    redo.LogManager
	eventCache     *redoEventCache
	splitTxn       bool
	enableOldValue bool
}

func newRedoWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	quota *memQuota,
	redoManager redo.LogManager,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) *redoWorker {
	return &redoWorker{
		changefeedID:   changefeedID,
		sourceManager:  sourceManager,
		memQuota:       quota,
		redoManager:    redoManager,
		eventCache:     eventCache,
		splitTxn:       splitTxn,
		enableOldValue: enableOldValue,
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

func (w *redoWorker) handleTask(ctx context.Context, task *redoTask) error {
	rows := make([]*model.RowChangedEvent, 0, 1024)
	cache := w.eventCache.getAppender(task.tableID)

	// Events are pushed into redoEventCache if possible. Otherwise, their memory will
	// be released after they are written into redo files. Then we need to release their
	// memory quota, which can be calculated based on rowsSize and cachedSize.
	rowsSize := uint64(0)
	cachedSize := uint64(0)

	availableMemSize := requestMemSize
	usedMemSize := uint64(0)

	var lastPos, lastEmitPos engine.Position
	maybeEmitBatchEvents := func(allFinished, txnFinished bool) error {
		// If used memory size exceeds the required limit, do a force require.
		if usedMemSize > availableMemSize {
			w.memQuota.forceAcquire(usedMemSize - availableMemSize)
			log.Debug("MemoryQuotaTracing: force acquire memory for redo log task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Uint64("memory", usedMemSize-availableMemSize))
			availableMemSize = usedMemSize
		}

		if allFinished {
			// There is no more events and some required memory isn't used.
			if availableMemSize > usedMemSize {
				w.memQuota.refund(availableMemSize - usedMemSize)
				log.Debug("MemoryQuotaTracing: refund memory for redo log task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", availableMemSize-usedMemSize))
			}
		} else if usedMemSize >= availableMemSize {
			if txnFinished {
				if w.memQuota.tryAcquire(requestMemSize) {
					availableMemSize += requestMemSize
					log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
						zap.String("namespace", w.changefeedID.Namespace),
						zap.String("changefeed", w.changefeedID.ID),
						zap.Int64("tableID", task.tableID),
						zap.Uint64("memory", requestMemSize))
				}
			} else {
				w.memQuota.forceAcquire(requestMemSize)
				availableMemSize += requestMemSize
				log.Debug("MemoryQuotaTracing: force acquire memory for redo log task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", requestMemSize))
			}
		}

		if rowsSize >= maxUpdateIntervalSize || allFinished {
			refundMem := rowsSize - cachedSize
			releaseMem := func() { w.memQuota.refund(refundMem) }
			err := w.redoManager.EmitRowChangedEvents(ctx, task.tableID, releaseMem, rows...)
			if err != nil {
				log.Debug("MemoryQuotaTracing: refund memory for redo log task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", rowsSize))
				return errors.Trace(err)
			}
			if lastPos.Valid() && lastPos.Compare(lastEmitPos) != 0 {
				err = w.redoManager.UpdateResolvedTs(ctx, task.tableID, lastPos.CommitTs)
				if err != nil {
					return errors.Trace(err)
				}
				log.Debug("update resolved ts to redo",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("resolvedTs", lastPos.CommitTs))
				lastEmitPos = lastPos
			}
			rowsSize = 0
			cachedSize = 0
			rows = rows[:0]
			if cap(rows) > 1024 {
				rows = make([]*model.RowChangedEvent, 0, 1024)
			}
		}

		return nil
	}

	upperBound := task.getUpperBound()
	iter := w.sourceManager.FetchByTable(task.tableID, task.lowerBound, upperBound)
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error("sink redo worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Error(err))
		}

		log.Debug("redo task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Uint64("commitTs", lastPos.CommitTs),
			zap.Uint64("startTs", lastPos.StartTs))
		task.callback(lastPos)
	}()

	for availableMemSize > usedMemSize {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if e == nil {
			// There is no more data.
			if !lastPos.Valid() {
				lastPos = upperBound
			}
			if err = maybeEmitBatchEvents(true, true); e != nil {
				return errors.Trace(err)
			}
			return nil
		}
		if pos.Valid() {
			lastPos = pos
		}

		task.tableSink.updateReceivedSorterCommitTs(e.CRTs)
		if e.Row == nil {
			// NOTICE: This could happen when the event is filtered by the event filter.
			continue
		}
		// For all rows, we add table replicate ts, so mysql sink can
		// determine when to turn off safe-mode.
		e.Row.ReplicatingTs = task.tableSink.replicateTs

		x, size, err := convertRowChangedEvents(w.changefeedID, task.tableID, w.enableOldValue, e)
		if err != nil {
			return errors.Trace(err)
		}
		usedMemSize += size

		rows = append(rows, x...)
		rowsSize += size
		if cache.pushBatch(x, size, pos.Valid()) {
			cachedSize += size
		} else {
			cachedSize -= cache.cleanBrokenEvents()
		}

		if err = maybeEmitBatchEvents(false, pos.Valid()); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
