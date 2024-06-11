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
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type redoWorker struct {
	changefeedID   model.ChangeFeedID
	sourceManager  *sourcemanager.SourceManager
	memQuota       *memquota.MemQuota
	redoDMLManager redo.DMLManager
}

func newRedoWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	quota *memquota.MemQuota,
	redoDMLMgr redo.DMLManager,
) *redoWorker {
	return &redoWorker{
		changefeedID:   changefeedID,
		sourceManager:  sourceManager,
		memQuota:       quota,
		redoDMLManager: redoDMLMgr,
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
	advancer := newRedoLogAdvancer(task, w.memQuota, requestMemSize, w.redoDMLManager)
	// The task is finished and some required memory isn't used.
	defer advancer.cleanup()

	lowerBound, upperBound := validateAndAdjustBound(
		w.changefeedID,
		&task.span,
		task.lowerBound,
		task.getUpperBound(task.tableSink.getReceivedSorterResolvedTs()),
	)
	advancer.lastPos = lowerBound.Prev()

	allEventCount := 0

	iter := w.sourceManager.FetchByTable(task.span, lowerBound, upperBound, w.memQuota)
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error("redo worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &task.span),
				zap.Error(err))
		}
	}()

	defer func() {
		log.Debug("redo task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Any("lastPos", advancer.lastPos),
			zap.Float64("lag", time.Since(oracle.GetTimeFromTS(advancer.lastPos.CommitTs)).Seconds()),
			zap.Error(finalErr))

		if finalErr == nil {
			// Otherwise we can't ensure all events before `lastPos` are emitted.
			task.callback(advancer.lastPos)
		}
	}()

	for advancer.hasEnoughMem() && !task.isCanceled() {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// There is no more data. It means that we finish this scan task.
		if e == nil {
			return advancer.finish(ctx, upperBound)
		}

		allEventCount += 1

		if pos.Valid() {
			advancer.lastPos = pos
		}

		advancer.tryMoveToNextTxn(e.CRTs, pos)

		var x []*model.RowChangedEvent
		var size uint64
		// NOTICE: The event can be filtered by the event filter.
		if e.Row != nil {
			// For all events, we add table replicate ts, so mysql sink can determine safe-mode.
			e.Row.ReplicatingTs = task.tableSink.replicateTs.Load()
			x, size = handleRowChangedEvents(w.changefeedID, task.span, e)
			advancer.appendEvents(x, size)
		}

		err = advancer.tryAdvanceAndAcquireMem(
			ctx,
			false,
			pos.Valid(),
		)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Even if task is canceled we still call this again, to avoid something
	// are left and leak forever.
	return advancer.advance(ctx)
}
