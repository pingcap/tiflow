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
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type sinkWorker struct {
	changefeedID  model.ChangeFeedID
	sourceManager *sourcemanager.SourceManager
	sinkMemQuota  *memquota.MemQuota
	redoMemQuota  *memquota.MemQuota
	eventCache    *redoEventCache
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool
	// enableOldValue indicates whether to enable the old value feature.
	// If it is enabled, we need to deal with the compatibility of the data format.
	enableOldValue bool

	// Metrics.
	metricRedoEventCacheHit  prometheus.Counter
	metricRedoEventCacheMiss prometheus.Counter
}

// newWorker creates a new worker.
func newSinkWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	sinkQuota *memquota.MemQuota,
	redoQuota *memquota.MemQuota,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) *sinkWorker {
	return &sinkWorker{
		changefeedID:   changefeedID,
		sourceManager:  sourceManager,
		sinkMemQuota:   sinkQuota,
		redoMemQuota:   redoQuota,
		eventCache:     eventCache,
		splitTxn:       splitTxn,
		enableOldValue: enableOldValue,

		metricRedoEventCacheHit:  RedoEventCacheAccess.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "hit"),
		metricRedoEventCacheMiss: RedoEventCacheAccess.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "miss"),
	}
}

func (w *sinkWorker) handleTasks(ctx context.Context, taskChan <-chan *sinkTask) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-taskChan:
			err := w.handleTask(ctx, task)
			if err != nil {
				return err
			}
		}
	}
}

func validateAndAdjustBound(changefeedID model.ChangeFeedID,
	task *sinkTask,
) (engine.Position, engine.Position) {
	lowerBound := task.lowerBound
	upperBound := task.getUpperBound(task.tableSink.getReceivedSorterResolvedTs())

	lowerPhs := oracle.GetTimeFromTS(lowerBound.CommitTs)
	upperPhs := oracle.GetTimeFromTS(upperBound.CommitTs)
	// The time range of a task should not exceed maxTaskTimeRange.
	// This would help for reduce changefeed latency.
	if upperPhs.Sub(lowerPhs) > maxTaskTimeRange {
		newUpperCommitTs := oracle.GoTimeToTS(lowerPhs.Add(maxTaskTimeRange))
		upperBound = engine.GenCommitFence(newUpperCommitTs)
	}

	if !upperBound.IsCommitFence() {
		log.Panic("Table sink task upperbound must be a ResolvedTs",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Any("upperBound", upperBound))
	}

	return lowerBound, upperBound
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

func (w *sinkWorker) doEmitAndAdvance(
	isLastTime bool,
	events []*model.RowChangedEvent,
	task *sinkTask,
	currTxnCommitTs uint64,
	lastTxnCommitTs uint64,
	lastPos engine.Position,
	// Mutable parameters!
	batchID *uint64,
	committedTxnSize *uint64,
	pendingTxnSize *uint64,
) (err error) {
	// Append the events to the table sink first.
	if len(events) > 0 {
		task.tableSink.appendRowChangedEvents(events...)
	}
	log.Debug("check should advance or not",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Stringer("span", &task.span),
		zap.Bool("splitTxn", w.splitTxn),
		zap.Uint64("currTxnCommitTs", currTxnCommitTs),
		zap.Uint64("lastTxnCommitTs", lastTxnCommitTs),
		zap.Bool("isLastTime", isLastTime))

	// If the current transaction is the last transaction, we need to emit all events
	// and advance the table sink.
	if currTxnCommitTs == lastPos.CommitTs {
		// All transactions before currTxnCommitTs are resolved.
		if lastPos.IsCommitFence() {
			err = w.advanceTableSink(task, currTxnCommitTs, *committedTxnSize+*pendingTxnSize)
		} else {
			// This means all events of the current transaction have been fetched, but we can't
			// ensure whether there are more transaction with the same CommitTs or not.
			// So we need to advance the table sink with a batchID. It will make sure that
			// we do not cross the CommitTs boundary.
			err = w.advanceTableSinkWithBatchID(task, currTxnCommitTs,
				*committedTxnSize+*pendingTxnSize, *batchID)
			*batchID += 1
		}

		*committedTxnSize = 0
		*pendingTxnSize = 0
	} else if w.splitTxn && currTxnCommitTs > 0 {
		// This branch will advance some complete transactions before currTxnCommitTs,
		// and one partial transaction with `batchID`.
		err = w.advanceTableSinkWithBatchID(task, currTxnCommitTs,
			*committedTxnSize+*pendingTxnSize, *batchID)

		*batchID += 1
		*committedTxnSize = 0
		*pendingTxnSize = 0
	} else if !w.splitTxn && lastTxnCommitTs > 0 {
		// If we don't split the transaction, we **only** advance the table sink
		// by the last transaction commit ts.
		err = w.advanceTableSink(task, lastTxnCommitTs, *committedTxnSize)
		*committedTxnSize = 0
		// If it is the last time we call `doEmitAndAdvance`, but `pendingTxnSize`
		// hasn't been recorded yet. To avoid losing it, record it manually.
		if isLastTime && *pendingTxnSize > 0 {
			w.sinkMemQuota.Record(task.span, model.NewResolvedTs(currTxnCommitTs), *pendingTxnSize)
			*pendingTxnSize = 0
		}
	}
	return
}

func (w *sinkWorker) tryEmitAndAdvance(
	allFinished bool,
	txnFinished bool,
	// Mutable parameters!
	availableMem *uint64,
	usedMem uint64,
	task *sinkTask,
	events []*model.RowChangedEvent,
	currTxnCommitTs uint64,
	lastTxnCommitTs uint64,
	lastPos engine.Position,
	// Mutable parameters!
	batchID *uint64,
	committedTxnSize *uint64,
	pendingTxnSize *uint64,
) (emitted bool, err error) {
	// If used memory size exceeds the required limit, do a force acquire to
	// make sure the memory quota is not exceeded or leak.
	// For example, if the memory quota is 100MB, and current usedMem is 90MB,
	// and availableMem is 100MB, then we can get event from the source manager
	// but if the event size is 20MB, we just exceed the available memory quota temporarily.
	// So we need to force acquire the memory quota to make up the difference.
	exceedAvailableMem := *availableMem < usedMem
	if exceedAvailableMem {
		w.sinkMemQuota.ForceAcquire(usedMem - *availableMem)
		log.Debug("MemoryQuotaTracing: force acquire memory for table sink task",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Uint64("memory", usedMem-*availableMem))
		*availableMem = usedMem
	}

	// Do emit in such situations:
	// 1. we use more memory than we required;
	// 2. all events are received.
	// 3. the pending batch size exceeds maxUpdateIntervalSize;
	if exceedAvailableMem || allFinished ||
		needEmitAndAdvance(w.splitTxn, *committedTxnSize, *pendingTxnSize) {
		if err := w.doEmitAndAdvance(
			false,
			events,
			task,
			currTxnCommitTs,
			lastTxnCommitTs,
			lastPos,
			batchID,
			committedTxnSize,
			pendingTxnSize,
		); err != nil {
			return false, errors.Trace(err)
		}
		emitted = true
	}

	// All finished, no need to acquire memory.
	if allFinished {
		return emitted, nil
	}

	if usedMem >= *availableMem {
		// We just finished a transaction, and the memory usage is still high.
		// We need try to acquire memory for the next transaction. It is possible
		// we can't acquire memory, but we finish the current transaction. So
		// we can wait for next round.
		if txnFinished {
			if w.sinkMemQuota.TryAcquire(requestMemSize) {
				*availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: try acquire memory for table sink task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Stringer("span", &task.span),
					zap.Uint64("memory", requestMemSize))
			}
		} else {
			// The transaction is not finished and splitTxn is false, we need to
			// force acquire memory. Because we can't leave rest data
			// to the next round.
			if !w.splitTxn {
				w.sinkMemQuota.ForceAcquire(requestMemSize)
				*availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: force acquire memory for table sink task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Stringer("span", &task.span),
					zap.Uint64("memory", requestMemSize))
			} else {
				// NOTE: if splitTxn is true it's not required to force acquire memory.
				// We can wait for a while because we already flushed some data to
				// the table sink.
				if err := w.sinkMemQuota.BlockAcquire(requestMemSize); err != nil {
					return emitted, errors.Trace(err)
				}
				*availableMem += requestMemSize
				log.Debug("MemoryQuotaTracing: block acquire memory for table sink task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Stringer("span", &task.span),
					zap.Uint64("memory", requestMemSize))
			}
		}
	}
	return emitted, nil
}

func (w *sinkWorker) handleTask(ctx context.Context, task *sinkTask) (finalErr error) {
	lowerBound, upperBound := validateAndAdjustBound(w.changefeedID, task)

	// First time to run the task, we have initialized memory quota for the table.
	availableMem := requestMemSize
	// How much memory we have used.
	// This is used to calculate how much memory we need to acquire.
	// Only when usedMem > availableMem we need to acquire memory.
	usedMem := uint64(0)

	// batchID is used to advance table sink with a given CommitTs, even if not all
	// transactions with the same CommitTs are collected, regardless of whether splitTxn
	// is enabled or not. We split transactions with the same CommitTs even if splitTxn
	// is false, and it won't break transaction atomicity to downstream.
	batchID := uint64(1)
	if w.eventCache != nil {
		drained, err := w.fetchFromCache(task, &lowerBound, &upperBound, &batchID)
		if err != nil {
			return errors.Trace(err)
		}
		// We have drained all events from the cache, we can return directly.
		// No need to get events from the source manager again.
		if drained {
			w.sinkMemQuota.Refund(availableMem - usedMem)
			log.Debug("MemoryQuotaTracing: refund memory for table sink task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &task.span),
				zap.Uint64("memory", availableMem-usedMem))
			task.callback(lowerBound.Prev())
			return nil
		}
	}

	// Used to record the last written position.
	// We need to use it to update the lower bound of the table sink.
	var lastPos engine.Position
	// Used to record the last written transaction commit ts.
	committedTxnSize := uint64(0)
	// Used to record the last written transaction commit ts.
	lastTxnCommitTs := uint64(0)
	// Used to record the size of the current transaction.
	pendingTxnSize := uint64(0)
	// Used to record the current transaction commit ts.
	currTxnCommitTs := uint64(0)
	events := make([]*model.RowChangedEvent, 0, 1024)

	allEventSize := uint64(0)
	allEventCount := 0
	// lowerBound and upperBound are both closed intervals.
	iter := w.sourceManager.FetchByTable(task.span, lowerBound, upperBound, w.sinkMemQuota)

	defer func() {
		// Collect metrics.
		w.metricRedoEventCacheMiss.Add(float64(allEventSize))
		task.tableSink.receivedEventCount.Add(int64(allEventCount))
		outputEventCount.WithLabelValues(
			task.tableSink.changefeed.Namespace,
			task.tableSink.changefeed.ID,
			"kv",
		).Add(float64(allEventCount))

		// If eventCache is nil, update sorter commit ts and range event count.
		if w.eventCache == nil {
			task.tableSink.updateReceivedSorterCommitTs(currTxnCommitTs)
			eventCount := newRangeEventCount(lastPos, allEventCount)
			task.tableSink.updateRangeEventCounts(eventCount)
		}

		if err := iter.Close(); err != nil {
			log.Error("Sink worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &task.span),
				zap.Error(err))
		}

		log.Debug("Sink task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Bool("splitTxn", w.splitTxn),
			zap.Any("lastPos", lastPos))

		// Otherwise we can't ensure all events before `lastPos` are emitted.
		if finalErr == nil {
			task.callback(lastPos)
		}

		// The task is finished and some required memory isn't used.
		if availableMem > usedMem {
			w.sinkMemQuota.Refund(availableMem - usedMem)
			log.Debug("MemoryQuotaTracing: refund memory for table sink task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &task.span),
				zap.Uint64("memory", availableMem-usedMem))
		}
	}()

	// 1. We have enough memory to collect events.
	// 2. The task is not canceled.
	for availableMem > usedMem && !task.isCanceled() {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		// There is no more data. It means that we finish this scan task.
		if e == nil {
			lastPos = upperBound
			currTxnCommitTs = upperBound.CommitTs
			lastTxnCommitTs = upperBound.CommitTs
			_, err := w.tryEmitAndAdvance(
				true,
				true,
				&availableMem,
				usedMem,
				task,
				events,
				currTxnCommitTs,
				lastTxnCommitTs,
				lastPos,
				&batchID,
				&committedTxnSize,
				&pendingTxnSize,
			)
			return err
		}
		allEventCount += 1

		if pos.Valid() {
			lastPos = pos
		}

		// Meet a new commit ts, we need to emit the previous events.
		if currTxnCommitTs != e.CRTs {
			lastTxnCommitTs = currTxnCommitTs
			currTxnCommitTs = e.CRTs
			committedTxnSize += pendingTxnSize
			pendingTxnSize = 0
			batchID = 1
		}

		// NOTICE: The event can be filtered by the event filter.
		if e.Row != nil {
			// For all rows, we add table replicate ts, so mysql sink can determine safe-mode.
			e.Row.ReplicatingTs = task.tableSink.replicateTs
			x, size, err := convertRowChangedEvents(w.changefeedID, task.span, w.enableOldValue, e)
			if err != nil {
				return err
			}

			events = append(events, x...)
			allEventSize += size
			usedMem += size
			pendingTxnSize += size
		}

		emitted, err := w.tryEmitAndAdvance(
			false,
			pos.Valid(),
			&availableMem,
			usedMem,
			task,
			events,
			currTxnCommitTs,
			lastTxnCommitTs,
			lastPos,
			&batchID,
			&committedTxnSize,
			&pendingTxnSize,
		)
		if err != nil {
			return errors.Trace(err)
		}
		// If we emit the events, we need to clear the events.
		if emitted {
			events = events[:0]
			if cap(events) > 1024 {
				events = make([]*model.RowChangedEvent, 0, 1024)
			}
		}
	}

	return w.doEmitAndAdvance(
		true,
		events,
		task,
		currTxnCommitTs,
		lastTxnCommitTs,
		lastPos,
		&batchID,
		&committedTxnSize,
		&pendingTxnSize,
	)
}

func (w *sinkWorker) fetchFromCache(
	task *sinkTask, // task is read-only here.
	lowerBound *engine.Position,
	upperBound *engine.Position,
	batchID *uint64,
) (cacheDrained bool, err error) {
	newLowerBound := *lowerBound
	newUpperBound := *upperBound

	cache := w.eventCache.getAppender(task.span)
	if cache == nil {
		return
	}
	popRes := cache.pop(*lowerBound, *upperBound)
	if popRes.success {
		newLowerBound = popRes.boundary.Next()
		if len(popRes.events) > 0 {
			task.tableSink.receivedEventCount.Add(int64(popRes.pushCount))
			outputEventCount.WithLabelValues(
				task.tableSink.changefeed.Namespace,
				task.tableSink.changefeed.ID,
				"kv",
			).Add(float64(popRes.pushCount))
			w.metricRedoEventCacheHit.Add(float64(popRes.size))
			task.tableSink.appendRowChangedEvents(popRes.events...)
		}

		// Get a resolvedTs so that we can record it into sink memory quota.
		var resolvedTs model.ResolvedTs
		isCommitFence := popRes.boundary.IsCommitFence()
		if w.splitTxn {
			resolvedTs = model.NewResolvedTs(popRes.boundary.CommitTs)
			if !isCommitFence {
				resolvedTs.Mode = model.BatchResolvedMode
				resolvedTs.BatchID = *batchID
				*batchID += 1
			}
		} else {
			if isCommitFence {
				resolvedTs = model.NewResolvedTs(popRes.boundary.CommitTs)
			} else {
				resolvedTs = model.NewResolvedTs(popRes.boundary.CommitTs - 1)
			}
		}
		// Transfer the memory usage from redoMemQuota to sinkMemQuota.
		w.sinkMemQuota.ForceAcquire(popRes.releaseSize)
		w.sinkMemQuota.Record(task.span, resolvedTs, popRes.releaseSize)
		w.redoMemQuota.Refund(popRes.releaseSize)

		err = task.tableSink.updateResolvedTs(resolvedTs)
		log.Debug("Advance table sink",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Any("resolvedTs", resolvedTs),
			zap.Error(err))
	} else {
		newUpperBound = popRes.boundary.Prev()
	}
	cacheDrained = newLowerBound.Compare(newUpperBound) > 0
	log.Debug("fetchFromCache is performed",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Stringer("span", &task.span),
		zap.Bool("success", popRes.success),
		zap.Int("eventsLen", len(popRes.events)),
		zap.Bool("cacheDrained", cacheDrained),
		zap.Any("lowerBound", lowerBound),
		zap.Any("upperBound", upperBound),
		zap.Any("newLowerBound", newLowerBound),
		zap.Any("newUpperBound", newUpperBound))
	*lowerBound = newLowerBound
	*upperBound = newUpperBound
	return
}

func (w *sinkWorker) advanceTableSinkWithBatchID(t *sinkTask, commitTs model.Ts,
	size uint64, batchID uint64,
) error {
	resolvedTs := model.NewResolvedTs(commitTs)
	resolvedTs.Mode = model.BatchResolvedMode
	resolvedTs.BatchID = batchID
	log.Debug("Advance table sink with batch ID",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Stringer("span", &t.span),
		zap.Any("resolvedTs", resolvedTs),
		zap.Uint64("size", size))
	if size > 0 {
		w.sinkMemQuota.Record(t.span, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}

func (w *sinkWorker) advanceTableSink(t *sinkTask, commitTs model.Ts, size uint64) error {
	resolvedTs := model.NewResolvedTs(commitTs)
	log.Debug("Advance table sink without batch ID",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Stringer("span", &t.span),
		zap.Any("resolvedTs", resolvedTs),
		zap.Uint64("size", size))
	if size > 0 {
		w.sinkMemQuota.Record(t.span, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}
