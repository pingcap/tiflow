// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/redo"
	"go.uber.org/zap"
)

type redoLogAdvancer struct {
	// NOTICE: This task is immutable, so please never modify it.
	task *redoTask
	// redoDMLManager is used to write the redo log.
	redoDMLManager redo.DMLManager
	// memQuota is used to acquire memory quota for the redo log writer.
	memQuota *memquota.MemQuota
	// NOTICE: First time to run the task, we have initialized memory quota for the table.
	// It is defaultRequestMemSize.
	availableMem uint64
	// How much memory we have used.
	// This is used to calculate how much memory we need to acquire.
	// Only when usedMem > availableMem we need to acquire memory.
	usedMem uint64
	// Used to record the last written position.
	// We need to use it to update the lower bound of the table sink.
	lastPos sorter.Position
	// Buffer the events to be written to the redo log.
	events []*model.RowChangedEvent

	// emittedCommitTs is used to record the last emitted transaction commit ts.
	emittedCommitTs uint64
	// Used to record the latest written transaction commit ts.
	lastTxnCommitTs uint64

	// pendingTxnSize used to record the size of the uncommitted events.
	pendingTxnSize uint64
	// Used to record the current transaction commit ts.
	currTxnCommitTs uint64
}

func newRedoLogAdvancer(
	task *redoTask,
	memQuota *memquota.MemQuota,
	availableMem uint64,
	redoDMLManager redo.DMLManager,
) *redoLogAdvancer {
	return &redoLogAdvancer{
		task:           task,
		memQuota:       memQuota,
		availableMem:   availableMem,
		events:         make([]*model.RowChangedEvent, 0, bufferSize),
		redoDMLManager: redoDMLManager,
	}
}

// advance tries to emit the events to the redo log manager and
// advance the resolved ts of the redo log manager.
func (a *redoLogAdvancer) advance(ctx context.Context, cachedSize uint64) error {
	if len(a.events) > 0 {
		// releaseMem is used to release the memory quota
		// after the events are written to redo log.
		// It more like a callback function.
		var releaseMem func()
		refundMem := a.pendingTxnSize - cachedSize
		if refundMem > 0 {
			releaseMem = func() {
				a.memQuota.Refund(refundMem)
				log.Debug("MemoryQuotaTracing: refund memory for redo log task",
					zap.String("namespace", a.task.tableSink.changefeed.Namespace),
					zap.String("changefeed", a.task.tableSink.changefeed.ID),
					zap.Stringer("span", &a.task.span),
					zap.Uint64("memory", refundMem))
			}
		}
		if err := a.redoDMLManager.EmitRowChangedEvents(ctx, a.task.span, releaseMem,
			a.events...); err != nil {
			return errors.Trace(err)
		}
		a.events = a.events[:0]
		if cap(a.events) > bufferSize {
			a.events = make([]*model.RowChangedEvent, 0, bufferSize)
		}
		a.pendingTxnSize = 0
	}
	if a.lastTxnCommitTs > a.emittedCommitTs {
		if err := a.redoDMLManager.UpdateResolvedTs(ctx, a.task.span,
			a.lastTxnCommitTs); err != nil {
			return errors.Trace(err)
		}
		log.Debug("update resolved ts to redo",
			zap.String("namespace", a.task.tableSink.changefeed.Namespace),
			zap.String("changefeed", a.task.tableSink.changefeed.ID),
			zap.Stringer("span", &a.task.span),
			zap.Uint64("resolvedTs", a.lastTxnCommitTs))
		a.emittedCommitTs = a.lastTxnCommitTs
	}

	return nil
}

// tryAdvanceAndAcquireMem tries to acquire the memory quota and advance the redo log manager.
// allFetched indicates whether all the events have been fetched. Then we
// do not need to acquire the memory quota anymore.
// txnFinished indicates whether the current transaction has been finished.
// If it is finished, it is OK to wait next round task to advance the table sink.
// Otherwise, we need to advance the redo log at least to the current transaction.
func (a *redoLogAdvancer) tryAdvanceAndAcquireMem(
	ctx context.Context,
	cachedSize uint64,
	allFetched bool,
	txnFinished bool,
) (bool, error) {
	var advanced bool
	// If used memory size exceeds the required limit, do a force acquire to
	// make sure the memory quota is not exceeded or leak.
	// For example, if the memory quota is 100MB, and current usedMem is 90MB,
	// and availableMem is 100MB, then we can get event from the source manager
	// but if the event size is 20MB, we just exceed the available memory quota temporarily.
	// So we need to force acquire the memory quota to make up the difference.
	exceedAvailableMem := a.availableMem < a.usedMem
	if exceedAvailableMem {
		a.memQuota.ForceAcquire(a.usedMem - a.availableMem)
		log.Debug("MemoryQuotaTracing: force acquire memory for redo log task",
			zap.String("namespace", a.task.tableSink.changefeed.Namespace),
			zap.String("changefeed", a.task.tableSink.changefeed.ID),
			zap.Stringer("span", &a.task.span),
			zap.Uint64("memory", a.usedMem-a.availableMem))
		a.availableMem = a.usedMem
	}

	// Do emit in such situations:
	// 1. we use more memory than we required;
	// 2. the pending batch size exceeds maxUpdateIntervalSize;
	// 3. all events are received.
	if exceedAvailableMem || a.pendingTxnSize >= maxUpdateIntervalSize || allFetched {
		if err := a.advance(
			ctx,
			cachedSize,
		); err != nil {
			return false, errors.Trace(err)
		}
		advanced = true
	}

	if allFetched {
		return advanced, nil
	}

	if a.usedMem >= a.availableMem {
		if txnFinished {
			if a.memQuota.TryAcquire(requestMemSize) {
				a.availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
					zap.String("namespace", a.task.tableSink.changefeed.Namespace),
					zap.String("changefeed", a.task.tableSink.changefeed.ID),
					zap.Stringer("span", &a.task.span),
					zap.Uint64("memory", requestMemSize))
			}
		} else {
			// NOTE: it's not required to use `forceAcquire` even if splitTxn is false.
			// It's because memory will finally be `refund` after redo-logs are written.
			if err := a.memQuota.BlockAcquire(requestMemSize); err != nil {
				return false, errors.Trace(err)
			}
			a.availableMem += requestMemSize
			log.Debug("MemoryQuotaTracing: block acquire memory for redo log task",
				zap.String("namespace", a.task.tableSink.changefeed.Namespace),
				zap.String("changefeed", a.task.tableSink.changefeed.ID),
				zap.Stringer("span", &a.task.span),
				zap.Uint64("memory", requestMemSize))
		}
	}

	return advanced, nil
}

func (a *redoLogAdvancer) finish(
	ctx context.Context,
	cachedSize uint64,
	upperBound sorter.Position,
) error {
	a.lastPos = upperBound
	a.lastTxnCommitTs = upperBound.CommitTs
	_, err := a.tryAdvanceAndAcquireMem(
		ctx,
		cachedSize,
		true,
		true,
	)
	return err
}

// tryMoveToNextTxn tries to move to the next transaction.
// 1. If the commitTs is different from the current transaction, it means
// the current transaction is finished. We need to move to the next transaction.
// 2. If current position is a commit fence, it means the current transaction
// is finished. We can safely move to the next transaction early. It would be
// helpful to advance the redo log manager.
func (a *redoLogAdvancer) tryMoveToNextTxn(commitTs model.Ts, pos sorter.Position) {
	if a.currTxnCommitTs != commitTs {
		a.lastTxnCommitTs = a.currTxnCommitTs
		a.currTxnCommitTs = commitTs
	}

	// If the current position is a commit fence, it means the current transaction
	// is finished. We can safely move to the next transaction early.
	// NOTICE: Please do not combine this condition with the previous one.
	// There is a case that the current position is a commit fence, also
	// the commitTs is different from the current transaction.
	// For example:
	// 1. current commitTs is 10
	// 2. commitTs is 11
	// 3. pos is a commit fence (10,11)
	// In this case, we should move to the next transaction.
	// The lastTxnCommitTs should be 11, not 10.
	if pos.IsCommitFence() {
		a.lastTxnCommitTs = a.currTxnCommitTs
	}
}

// appendEvents appends events to the buffer and record the memory usage.
func (a *redoLogAdvancer) appendEvents(events []*model.RowChangedEvent, size uint64) {
	a.events = append(a.events, events...)
	// Record the memory usage.
	a.usedMem += size
	// Record the pending transaction size. It means how many events we do
	// not flush to the redo log manager.
	a.pendingTxnSize += size
}

// hasEnoughMem returns whether the redo log task has enough memory to continue.
func (a *redoLogAdvancer) hasEnoughMem() bool {
	return a.availableMem > a.usedMem
}

// cleanup cleans up the memory usage.
// Refund the memory usage if we do not use it.
func (a *redoLogAdvancer) cleanup() {
	if a.availableMem > a.usedMem {
		a.memQuota.Refund(a.availableMem - a.usedMem)
		log.Debug("MemoryQuotaTracing: refund memory for redo log task",
			zap.String("namespace", a.task.tableSink.changefeed.Namespace),
			zap.String("changefeed", a.task.tableSink.changefeed.ID),
			zap.Stringer("span", &a.task.span),
			zap.Uint64("memory", a.availableMem-a.usedMem))
	}
}
