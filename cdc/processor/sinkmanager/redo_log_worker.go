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
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/redo"
)

type redoWorker struct {
	changefeedID   model.ChangeFeedID
	mg             entry.MounterGroup
	sortEngine     engine.SortEngine
	memQuota       *memQuota
	redoManager    redo.LogManager
	eventCache     *redoEventCache
	splitTxn       bool
	enableOldValue bool
}

func newRedoWorker(
	changefeedID model.ChangeFeedID,
	mg entry.MounterGroup,
	sortEngine engine.SortEngine,
	quota *memQuota,
	redoManager redo.LogManager,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) *redoWorker {
	return &redoWorker{
		changefeedID:   changefeedID,
		mg:             mg,
		sortEngine:     sortEngine,
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
	// memory quota, which can be calculated based on batchSize and cachedSize.
	batchSize := uint64(0)
	cachedSize := uint64(0)

	memAllocated := true

	var lastPos engine.Position
	maybeEmitBatchEvents := func(allFinished, txnFinished bool) error {
		if batchSize == 0 || (!allFinished && batchSize < requestMemSize) {
			return nil
		}

		releaseMem := func() { w.memQuota.refund(batchSize - cachedSize) }
		err := w.redoManager.EmitRowChangedEvents(ctx, task.tableID, releaseMem, rows...)
		if err != nil {
			return errors.Trace(err)
		}
		if lastPos.Valid() {
			err = w.redoManager.UpdateResolvedTs(ctx, task.tableID, lastPos.CommitTs)
			if err != nil {
				return errors.Trace(err)
			}
		}

		rows = rows[0:]
		if cap(rows) > 1024 {
			rows = make([]*model.RowChangedEvent, 0, 1024)
		}
		batchSize = 0
		cachedSize = 0

		if !allFinished {
			if !txnFinished {
				w.memQuota.forceAcquire(requestMemSize)
			} else {
				memAllocated = w.memQuota.tryAcquire(requestMemSize)
			}
		}
		return nil
	}

	// lowerBound and upperBound are both closed intervals.
	iter := engine.NewMountedEventIter(
		w.sortEngine.FetchByTable(task.tableID, task.lowerBound, task.getUpperBound()),
		w.mg, 256)
	defer iter.Close()
	for memAllocated {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if e == nil {
			// There is no more data.
			if err = maybeEmitBatchEvents(true, true); e != nil {
				return errors.Trace(err)
			}
			return nil
		}
		if pos.Valid() {
			lastPos = pos
		}

		x, size, err := convertRowChangedEvents(w.changefeedID, task.tableID, w.enableOldValue, e)
		if err != nil {
			return errors.Trace(err)
		}

		rows = append(rows, x...)
		batchSize += size
		if cache.pushBatch(x, size, pos.Valid()) {
			cachedSize += size
		} else {
			cachedSize -= cache.cleanBrokenEvents()
		}
		if err = maybeEmitBatchEvents(false, pos.Valid()); err != nil {
			return errors.Trace(err)
		}
	}
	// Can't allocate memory.
	task.callback(lastPos)
	return nil
}
