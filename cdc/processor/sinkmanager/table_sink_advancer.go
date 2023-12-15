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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"go.uber.org/zap"
)

type tableSinkAdvancer struct {
	// NOTICE: This task is immutable, so please never modify it.
	task *sinkTask
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool
	// sinkMemQuota is used to acquire memory quota for the table sink.
	sinkMemQuota *memquota.MemQuota
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
	// Buffer the events to be written to the table sink.
	events []*model.RowChangedEvent

	// Used to record the size of already appended transaction.
	committedTxnSize uint64
	// Used to record the latest written transaction commit ts.
	lastTxnCommitTs uint64

	// Used to record the size of the current transaction.
	pendingTxnSize uint64
	// Used to record the current transaction commit ts.
	currTxnCommitTs uint64
}

func newTableSinkAdvancer(
	task *sinkTask,
	splitTxn bool,
	sinkMemQuota *memquota.MemQuota,
	availableMem uint64,
) *tableSinkAdvancer {
	return &tableSinkAdvancer{
		task:         task,
		splitTxn:     splitTxn,
		sinkMemQuota: sinkMemQuota,
		availableMem: availableMem,
		events:       make([]*model.RowChangedEvent, 0, bufferSize),
	}
}

// advance tries to append the event to the table sink
// and advance the table sink.
// isLastTime indicates whether this is the last time to call advance.
// If it is the last time, and we still have some events in the buffer,
// we need to record the memory usage and append the events to the table sink.
func (a *tableSinkAdvancer) advance(isLastTime bool) (err error) {
	// Append the events to the table sink first.
	if len(a.events) > 0 {
		if err = a.task.tableSink.appendRowChangedEvents(a.events...); err != nil {
			return
		}
		a.events = a.events[:0]
		if cap(a.events) > bufferSize {
			a.events = make([]*model.RowChangedEvent, 0, bufferSize)
		}
	}
	log.Debug("check should advance or not",
		zap.String("namespace", a.task.tableSink.changefeed.Namespace),
		zap.String("changefeed", a.task.tableSink.changefeed.ID),
		zap.Stringer("span", &a.task.span),
		zap.Bool("splitTxn", a.splitTxn),
		zap.Uint64("currTxnCommitTs", a.currTxnCommitTs),
		zap.Uint64("lastTxnCommitTs", a.lastTxnCommitTs),
		zap.Bool("isLastTime", isLastTime))

	// Still got the same commit ts.
	if a.currTxnCommitTs == a.lastPos.CommitTs {
		// All transactions before currTxnCommitTs are resolved.
		if a.lastPos.IsCommitFence() {
			err = advanceTableSink(a.task, a.currTxnCommitTs,
				a.committedTxnSize+a.pendingTxnSize, a.sinkMemQuota)
		} else {
			// This means all events of the current transaction have been fetched, but we can't
			// ensure whether there are more transaction with the same CommitTs or not.
			// So we need to advance the table sink with a batchID. It will make sure that
			// we do not cross the CommitTs boundary.
			err = advanceTableSinkWithBatchID(a.task, a.currTxnCommitTs,
				a.committedTxnSize+a.pendingTxnSize, batchID.Load(), a.sinkMemQuota)
			batchID.Add(1)
		}

		a.committedTxnSize = 0
		a.pendingTxnSize = 0
	} else if a.splitTxn && a.currTxnCommitTs > 0 {
		// We just got a new commit ts. Because we split the transaction,
		// we can advance the table sink with the current commit ts.
		// This will advance some complete transactions before currTxnCommitTs,
		// and one partial transaction with `batchID`.
		err = advanceTableSinkWithBatchID(a.task, a.currTxnCommitTs,
			a.committedTxnSize+a.pendingTxnSize, batchID.Load(), a.sinkMemQuota)

		batchID.Add(1)
		a.committedTxnSize = 0
		a.pendingTxnSize = 0
	} else if !a.splitTxn && a.lastTxnCommitTs > 0 {
		// We just got a new commit ts. Because we don't split the transaction,
		// we **only** advance the table sink by the last transaction commit ts.
		err = advanceTableSink(a.task, a.lastTxnCommitTs,
			a.committedTxnSize, a.sinkMemQuota)
		a.committedTxnSize = 0
		// If it is the last time we call `advance`, but `pendingTxnSize`
		// hasn't been recorded yet. To avoid losing it, record it manually.
		if isLastTime && a.pendingTxnSize > 0 {
			a.sinkMemQuota.Record(a.task.span,
				model.NewResolvedTs(a.currTxnCommitTs), a.pendingTxnSize)
			a.pendingTxnSize = 0
		}
	}
	return
}

// lastTimeAdvance only happens when there is no enough memory quota to
// acquire, and the task is not finished.
// In this case, we need to try to advance the table sink as much as possible.
func (a *tableSinkAdvancer) lastTimeAdvance() error {
	return a.advance(true)
}

// tryAdvanceAndAcquireMem tries to acquire the memory quota and advance the table sink.
// allFetched indicates whether all the events have been fetched. Then we
// do not need to acquire the memory quota anymore.
// txnFinished indicates whether the current transaction has been finished.
// If it is finished, it is OK to wait next round task to advance the table sink.
// Otherwise, we need to advance the table at least to the current transaction.
func (a *tableSinkAdvancer) tryAdvanceAndAcquireMem(
	allFetched bool,
	txnFinished bool,
) error {
	// If used memory size exceeds the required limit, do a force acquire to
	// make sure the memory quota is not exceeded or leak.
	// For example, if the memory quota is 100MB, and current usedMem is 90MB,
	// and availableMem is 100MB, then we can get event from the source manager
	// but if the event size is 20MB, we just exceed the available memory quota temporarily.
	// So we need to force acquire the memory quota to make up the difference.
	exceedAvailableMem := a.availableMem < a.usedMem
	if exceedAvailableMem {
		a.sinkMemQuota.ForceAcquire(a.usedMem - a.availableMem)
		log.Debug("MemoryQuotaTracing: force acquire memory for table sink task",
			zap.String("namespace", a.task.tableSink.changefeed.Namespace),
			zap.String("changefeed", a.task.tableSink.changefeed.ID),
			zap.Stringer("span", &a.task.span),
			zap.Uint64("memory", a.usedMem-a.availableMem))
		a.availableMem = a.usedMem
	}

	// Do emit in such situations:
	// 1. we use more memory than we required;
	// 2. all events are received.
	// 3. the pending batch size exceeds maxUpdateIntervalSize;
	if exceedAvailableMem || allFetched ||
		needEmitAndAdvance(a.splitTxn, a.committedTxnSize, a.pendingTxnSize) {
		if err := a.advance(false); err != nil {
			return errors.Trace(err)
		}
	}

	// All fetched, no need to acquire memory.
	if allFetched {
		return nil
	}

	if a.usedMem >= a.availableMem {
		// We just finished a transaction, and the memory usage is still high.
		// We need try to acquire memory for the next transaction. It is possible
		// we can't acquire memory, but we finish the current transaction. So
		// we can wait for next round.
		if txnFinished {
			if a.sinkMemQuota.TryAcquire(requestMemSize) {
				a.availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: try acquire memory for table sink task",
					zap.String("namespace", a.task.tableSink.changefeed.Namespace),
					zap.String("changefeed", a.task.tableSink.changefeed.ID),
					zap.Stringer("span", &a.task.span),
					zap.Uint64("memory", requestMemSize))
			}
		} else {
			// The transaction is not finished and splitTxn is false, we need to
			// force acquire memory. Because we can't leave rest data
			// to the next round.
			if !a.splitTxn {
				a.sinkMemQuota.ForceAcquire(requestMemSize)
				a.availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: force acquire memory for table sink task",
					zap.String("namespace", a.task.tableSink.changefeed.Namespace),
					zap.String("changefeed", a.task.tableSink.changefeed.ID),
					zap.Stringer("span", &a.task.span),
					zap.Uint64("memory", requestMemSize))
			} else {
				// NOTE: if splitTxn is true it's not required to force acquire memory.
				// We can wait for a while because we already flushed some data to
				// the table sink.
				if err := a.sinkMemQuota.BlockAcquire(requestMemSize); err != nil {
					return errors.Trace(err)
				}
				a.availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: block acquire memory for table sink task",
					zap.String("namespace", a.task.tableSink.changefeed.Namespace),
					zap.String("changefeed", a.task.tableSink.changefeed.ID),
					zap.Stringer("span", &a.task.span),
					zap.Uint64("memory", requestMemSize))
			}
		}
	}
	return nil
}

// tryMoveToNextTxn tries to move to the next transaction.
// If the commitTs is different from the current transaction, it means
// the current transaction is finished. We need to move to the next transaction.
func (a *tableSinkAdvancer) tryMoveToNextTxn(commitTs model.Ts) {
	if a.currTxnCommitTs != commitTs {
		// Record the last transaction commitTs and size.
		a.lastTxnCommitTs = a.currTxnCommitTs
		a.committedTxnSize += a.pendingTxnSize
		// Move to the next transaction.
		a.currTxnCommitTs = commitTs
		a.pendingTxnSize = 0
	}
}

// finish finishes the table sink task.
// It will move the table sink task to the upperBound position.
func (a *tableSinkAdvancer) finish(upperBound sorter.Position) error {
	a.lastPos = upperBound
	a.currTxnCommitTs = upperBound.CommitTs
	a.lastTxnCommitTs = upperBound.CommitTs
	return a.tryAdvanceAndAcquireMem(
		true,
		true,
	)
}

// appendEvents appends events to the buffer and record the memory usage.
func (a *tableSinkAdvancer) appendEvents(events []*model.RowChangedEvent, size uint64) {
	a.events = append(a.events, events...)
	// Record the memory usage.
	a.usedMem += size
	// Record the pending transaction size. It means how many events we do
	// not flush to the table sink.
	a.pendingTxnSize += size
}

// hasEnoughMem returns whether the table sink task has enough memory to continue.
func (a *tableSinkAdvancer) hasEnoughMem() bool {
	return a.availableMem > a.usedMem
}

// cleanup cleans up the memory usage.
// Refund the memory usage if we do not use it.
func (a *tableSinkAdvancer) cleanup() {
	if a.availableMem > a.usedMem {
		a.sinkMemQuota.Refund(a.availableMem - a.usedMem)
		log.Debug("MemoryQuotaTracing: refund memory for table sink task",
			zap.String("namespace", a.task.tableSink.changefeed.Namespace),
			zap.String("changefeed", a.task.tableSink.changefeed.ID),
			zap.Stringer("span", &a.task.span),
			zap.Uint64("memory", a.availableMem-a.usedMem))
	}
}

func advanceTableSinkWithBatchID(
	t *sinkTask,
	commitTs model.Ts,
	size uint64,
	batchID uint64,
	sinkMemQuota *memquota.MemQuota,
) error {
	resolvedTs := model.NewResolvedTs(commitTs)
	resolvedTs.Mode = model.BatchResolvedMode
	resolvedTs.BatchID = batchID
	log.Debug("Advance table sink with batch ID",
		zap.String("namespace", t.tableSink.changefeed.Namespace),
		zap.String("changefeed", t.tableSink.changefeed.ID),
		zap.Stringer("span", &t.span),
		zap.Any("resolvedTs", resolvedTs),
		zap.Uint64("size", size))
	if size > 0 {
		sinkMemQuota.Record(t.span, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}

func advanceTableSink(
	t *sinkTask,
	commitTs model.Ts,
	size uint64,
	sinkMemQuota *memquota.MemQuota,
) error {
	resolvedTs := model.NewResolvedTs(commitTs)
	log.Debug("Advance table sink without batch ID",
		zap.String("namespace", t.tableSink.changefeed.Namespace),
		zap.String("changefeed", t.tableSink.changefeed.ID),
		zap.Stringer("span", &t.span),
		zap.Any("resolvedTs", resolvedTs),
		zap.Uint64("size", size))
	if size > 0 {
		sinkMemQuota.Record(t.span, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}

func needEmitAndAdvance(splitTxn bool, committedTxnSize uint64, pendingTxnSize uint64) bool {
	// If splitTxn is true, we can safely emit all the events in the last transaction
	// and current transaction. So we use `committedTxnSize+pendingTxnSize`.
	splitTxnEmitCondition := splitTxn && committedTxnSize+pendingTxnSize >= maxUpdateIntervalSize
	// If splitTxn is false, we need to emit the events when the size of the
	// transaction is greater than maxUpdateIntervalSize.
	// This could help to reduce the overhead of emit and advance too frequently.
	noSplitTxnEmitCondition := !splitTxn && committedTxnSize >= maxUpdateIntervalSize
	return splitTxnEmitCondition ||
		noSplitTxnEmitCondition
}
