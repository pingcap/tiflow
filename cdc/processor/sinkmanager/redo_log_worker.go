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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type redoWorker struct {
	changefeedID   model.ChangeFeedID
	sourceManager  *sourcemanager.SourceManager
	memQuota       *memquota.MemQuota
	redoDMLManager redo.DMLManager
	eventCache     *redoEventCache
	enableOldValue bool
}

func newRedoWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	quota *memquota.MemQuota,
	redoDMLMgr redo.DMLManager,
	eventCache *redoEventCache,
	enableOldValue bool,
) *redoWorker {
	return &redoWorker{
		changefeedID:   changefeedID,
		sourceManager:  sourceManager,
		memQuota:       quota,
		redoDMLManager: redoDMLMgr,
		eventCache:     eventCache,
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

func (w *redoWorker) emitEvents(
	ctx context.Context,
	span tablepb.Span,
	events []*model.RowChangedEvent,
	eventsSize *uint64,
	cachedSize *uint64,
	lastTxnCommitTs uint64,
	emittedCommitTs *uint64,
) error {
	if len(events) > 0 {
		// releaseMem is used to release the memory quota
		// after the events are written to redo log.
		var releaseMem func()
		refundMem := *eventsSize - *cachedSize
		if refundMem > 0 {
			releaseMem = func() {
				w.memQuota.Refund(refundMem)
				log.Debug("MemoryQuotaTracing: refund memory for redo log task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Stringer("span", &span),
					zap.Uint64("memory", refundMem))
			}
		}
		if err := w.redoDMLManager.EmitRowChangedEvents(ctx, span, releaseMem,
			events...); err != nil {
			return errors.Trace(err)
		}
		*eventsSize = 0
		*cachedSize = 0
	}
	if lastTxnCommitTs > *emittedCommitTs {
		if err := w.redoDMLManager.UpdateResolvedTs(ctx, span, lastTxnCommitTs); err != nil {
			return errors.Trace(err)
		}
		log.Debug("update resolved ts to redo",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("resolvedTs", lastTxnCommitTs))
		*emittedCommitTs = lastTxnCommitTs
	}
	return nil
}

func (w *redoWorker) maybeEmitEvents(
	ctx context.Context,
	span tablepb.Span,
	events []*model.RowChangedEvent,
	availableMemSize *uint64,
	usedMemSize uint64,
	eventsSize *uint64,
	cachedSize *uint64,
	lastTxnCommitTs uint64,
	emittedCommitTs *uint64,
	allFinished bool,
	txnFinished bool,
) (bool, error) {
	var emitted bool
	// If used memory size exceeds the required limit, do a force acquire.
	exceedAvailableMem := *availableMemSize < usedMemSize
	if exceedAvailableMem {
		w.memQuota.ForceAcquire(usedMemSize - *availableMemSize)
		log.Debug("MemoryQuotaTracing: force acquire memory for redo log task",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("memory", usedMemSize-*availableMemSize))
		*availableMemSize = usedMemSize
	}

	// Do emit in such situations:
	// 1. we use more memory than we required;
	// 2. the pending batch size exceeds maxUpdateIntervalSize;
	// 3. all events are received.
	if exceedAvailableMem || *eventsSize >= maxUpdateIntervalSize || allFinished {
		if err := w.emitEvents(
			ctx,
			span,
			events,
			eventsSize,
			cachedSize,
			lastTxnCommitTs,
			emittedCommitTs,
		); err != nil {
			return false, errors.Trace(err)
		}
		emitted = true
	}

	if allFinished {
		return false, nil
	}

	if usedMemSize >= *availableMemSize {
		if txnFinished {
			if w.memQuota.TryAcquire(requestMemSize) {
				*availableMemSize += requestMemSize
				log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Stringer("span", &span),
					zap.Uint64("memory", requestMemSize))
			}
		} else {
			// NOTE: it's not required to use `forceAcquire` even if splitTxn is false.
			// It's because memory will finally be `refund` after redo-logs are written.
			err := w.memQuota.BlockAcquire(requestMemSize)
			if err != nil {
				return false, errors.Trace(err)
			}
			*availableMemSize += requestMemSize
			log.Debug("MemoryQuotaTracing: block acquire memory for redo log task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &span),
				zap.Uint64("memory", requestMemSize))
		}
	}

	return emitted, nil
}

func (w *redoWorker) handleTask(ctx context.Context, task *redoTask) (finalErr error) {
	lowerBound, upperBound := validateAndAdjustBound(
		w.changefeedID,
		&task.span,
		task.lowerBound,
		task.getUpperBound(task.tableSink.getReceivedSorterResolvedTs()),
	)

	var cache *eventAppender
	if w.eventCache != nil {
		cache = w.eventCache.maybeCreateAppender(task.span, lowerBound)
	}

	// Events are pushed into redoEventCache if possible. Otherwise, their memory will
	// be released after they are written into redo files. Then we need to release their
	// memory quota, which can be calculated based on eventsSize and cachedSize.
	eventsSize := uint64(0)
	cachedSize := uint64(0)
	events := make([]*model.RowChangedEvent, 0, 1024)

	// Initialize the memory quota.
	availableMemSize := requestMemSize
	usedMemSize := uint64(0)

	// Only used to calculate lowerBound when next time the table is scheduled.
	var lastPos engine.Position

	// emittedCommitTs is the commitTs of the last event that has been emitted.
	emittedCommitTs := uint64(0)
	// lastTxnCommitTs is the commitTs of the last event that has been received completely.
	lastTxnCommitTs := uint64(0)
	// currTxnCommitTs is the commitTs of the current event.
	currTxnCommitTs := uint64(0)

	iter := w.sourceManager.FetchByTable(task.span, lowerBound, upperBound, w.memQuota)
	allEventCount := 0

	defer func() {
		task.tableSink.updateReceivedSorterCommitTs(lastTxnCommitTs)
		eventCount := newRangeEventCount(lastPos, allEventCount)
		task.tableSink.updateRangeEventCounts(eventCount)

		if err := iter.Close(); err != nil {
			log.Error("redo worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &task.span),
				zap.Error(err))
		}
		log.Debug("redo task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Any("lastPos", lastPos),
			zap.Float64("lag", time.Since(oracle.GetTimeFromTS(lastPos.CommitTs)).Seconds()))

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
				zap.Stringer("span", &task.span),
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

			_, err := w.maybeEmitEvents(
				ctx,
				task.span,
				events,
				&availableMemSize,
				usedMemSize,
				&eventsSize,
				&cachedSize,
				lastTxnCommitTs,
				&emittedCommitTs,
				true,
				true,
			)
			return err
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

		var x []*model.RowChangedEvent
		var size uint64
		// NOTICE: The event can be filtered by the event filter.
		if e.Row != nil {
			// For all events, we add table replicate ts, so mysql sink can determine safe-mode.
			e.Row.ReplicatingTs = task.tableSink.replicateTs
			x, size, err = convertRowChangedEvents(w.changefeedID, task.span, w.enableOldValue, e)
			if err != nil {
				return errors.Trace(err)
			}
			usedMemSize += size
			events = append(events, x...)
			eventsSize += size
		}

		if cache != nil {
			cached, brokenSize := cache.pushBatch(x, size, pos)
			if cached {
				cachedSize += size
			} else {
				cachedSize -= brokenSize
			}
		}

		emitted, err := w.maybeEmitEvents(
			ctx,
			task.span,
			events,
			&availableMemSize,
			usedMemSize,
			&eventsSize,
			&cachedSize,
			lastTxnCommitTs,
			&emittedCommitTs,
			false,
			pos.Valid(),
		)
		if err != nil {
			return errors.Trace(err)
		}

		if emitted {
			events = make([]*model.RowChangedEvent, 0, 1024)
		}
	}
	// Even if task is canceled we still call this again, to avoid somethings
	// are left and leak forever.
	return w.emitEvents(
		ctx,
		task.span,
		events,
		&eventsSize,
		&cachedSize,
		lastTxnCommitTs,
		&emittedCommitTs,
	)
}
