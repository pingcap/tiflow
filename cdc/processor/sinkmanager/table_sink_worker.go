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
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// batchID is used to distinguish different batches of the same transaction.
// We need to use a global variable because the same commit ts event may be
// processed at different times.
// For example:
//  1. The commit ts is 1000, and the start ts is 998.
//  2. Keep fetching events and flush them to the sink with batch ID 1.
//  3. Because we don't have enough memory quota, we need to flush the events
//     and wait for the next round of processing.
//  4. The next round of processing starts at commit ts 1000, and the start ts
//     is 999.
//  5. The batch ID restarts from 1, and the commit ts still is 1000.
//  6. We flush all the events with commit ts 1000 and batch ID 1 to the sink.
//  7. We release the memory quota of the events earlier because the current
//     round of processing is not finished.
//
// Therefore, we must use a global variable to ensure that the batch ID is
// monotonically increasing.
// We share this variable for all workers, it is OK that the batch ID is not
// strictly increasing one by one.
var batchID atomic.Uint64

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

func (w *sinkWorker) handleTask(ctx context.Context, task *sinkTask) (finalErr error) {
	// We need to use a new batch ID for each task.
	batchID := batchID.Add(1)
	advancer := newTableSinkAdvancer(task, w.splitTxn, w.sinkMemQuota, requestMemSize, batchID)
	// The task is finished and some required memory isn't used.
	defer advancer.cleanup()

	lowerBound, upperBound := validateAndAdjustBound(w.changefeedID, task)
	if w.eventCache != nil {
		drained, err := w.fetchFromCache(task, &lowerBound, &upperBound, &advancer.batchID)
		if err != nil {
			return errors.Trace(err)
		}
		// We have drained all events from the cache, we can return directly.
		// No need to get events from the source manager again.
		if drained {
			task.callback(lowerBound.Prev())
			return nil
		}
	}

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
			task.tableSink.updateReceivedSorterCommitTs(advancer.currTxnCommitTs)
			eventCount := newRangeEventCount(advancer.lastPos, allEventCount)
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
			zap.Any("lastPos", advancer.lastPos))

		// Otherwise we can't ensure all events before `lastPos` are emitted.
		if finalErr == nil {
			task.callback(advancer.lastPos)
		}
	}()

	// 1. We have enough memory to collect events.
	// 2. The task is not canceled.
	for advancer.hasEnoughMem() && !task.isCanceled() {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		// There is no more data. It means that we finish this scan task.
		if e == nil {
			return advancer.finish(upperBound)
		}

		allEventCount += 1

		// Only record the last valid position.
		// If the current txn is not finished, the position is not valid.
		if pos.Valid() {
			advancer.lastPos = pos
		}

		// Meet a new commit ts, we need to emit the previous events.
		advancer.tryMoveToNextTxn(e.CRTs)

		// NOTICE: The event can be filtered by the event filter.
		if e.Row != nil {
			// For all rows, we add table replicate ts, so mysql sink can determine safe-mode.
			e.Row.ReplicatingTs = task.tableSink.replicateTs
			x, size, err := convertRowChangedEvents(w.changefeedID, task.span, w.enableOldValue, e)
			if err != nil {
				return err
			}

			advancer.appendEvents(x, size)
			allEventSize += size
		}

		if err := advancer.tryAdvanceAndAcquireMem(
			false,
			pos.Valid(),
		); err != nil {
			return errors.Trace(err)
		}
	}

	return advancer.lastTimeAdvance()
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
