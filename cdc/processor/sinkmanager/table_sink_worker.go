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
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type sinkWorker struct {
	changefeedID  model.ChangeFeedID
	sourceManager *sourcemanager.SourceManager
	memQuota      *memQuota
	eventCache    *redoEventCache
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool
	// enableOldValue indicates whether to enable the old value feature.
	// If it is enabled, we need to deal with the compatibility of the data format.
	enableOldValue bool

	metricRedoEventCacheHit  prometheus.Counter
	metricRedoEventCacheMiss prometheus.Counter
}

// newWorker creates a new worker.
func newSinkWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	quota *memQuota,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) *sinkWorker {
	return &sinkWorker{
		changefeedID:   changefeedID,
		sourceManager:  sourceManager,
		memQuota:       quota,
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

func (w *sinkWorker) handleTask(ctx context.Context, task *sinkTask) (err error) {
	// First time to run the task, we have initialized memory quota for the table.
	availableMem := int(requestMemSize)
	events := make([]*model.PolymorphicEvent, 0, 1024)
	lowerBound := task.lowerBound
	upperBound := task.getUpperBound(task.tableSink)

	if w.eventCache != nil {
		lowerBound, err = w.fetchFromCache(task, lowerBound, upperBound)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Used to record the last written position.
	// We need to use it to update the lower bound of the table sink.
	var lastPos engine.Position
	// currentCommitTs is used to record the commitTs of the current event.
	currentCommitTs := uint64(0)
	// lastTimeCommitTs is used to record the last time commitTs.
	// Sometimes, we meet the situation that the commitTs of the event is the same.
	// But we can't advance this kind of event, so we need to record the last time commitTs.
	// If the current commitTs is the same as the last time commitTs, we don't need to advance.
	lastTimeCommitTs := uint64(0)
	currentTotalSize := uint64(0)
	batchID := uint64(1)

	// Two functions to simplify the code.
	// It captures some variables in the outer scope.
	appendEventsAndRecordCurrentSize := func(commitTs model.Ts) error {
		// Only less or equal.
		i := sort.Search(len(events), func(i int) bool {
			return events[i].CRTs > commitTs
		})
		resolvedEvents := events[:i]
		if len(resolvedEvents) == 0 {
			return nil
		}
		size, err := w.appendEventsToTableSink(task, resolvedEvents)
		if err != nil {
			return errors.Trace(err)
		}
		currentTotalSize += size
		events = append(make([]*model.PolymorphicEvent, 0, len(events[i:])), events[i:]...)
		return nil
	}
	advanceTableSinkAndResetCurrentSizeWithBatchID := func(commitTs model.Ts) error {
		err := w.advanceTableSinkWithBatchID(task, commitTs, currentTotalSize, batchID)
		if err != nil {
			return errors.Trace(err)
		}
		currentTotalSize = 0
		return nil
	}
	advanceTableSinkAndResetCurrentSize := func(commitTs model.Ts) error {
		err := w.advanceTableSink(task, commitTs, currentTotalSize)
		if err != nil {
			return errors.Trace(err)
		}
		currentTotalSize = 0
		return nil
	}

	// lowerBound and upperBound are both closed intervals.
	allEventSize := 0
	allEventCount := 0
	iter := w.sourceManager.FetchByTable(task.tableID, lowerBound, upperBound)
	defer func() {
		w.metricRedoEventCacheMiss.Add(float64(allEventSize))
		if w.eventCache == nil {
			eventCount := rangeEventCount{pos: lastPos, events: allEventCount}
			task.tableSink.updateRangeEventCounts(eventCount)
		}

		if err := iter.Close(); err != nil {
			log.Error("Sink worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Error(err))
		}

		log.Debug("Sink worker handle task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Bool("splitTxn", w.splitTxn),
			zap.Int("events", allEventCount),
		)
	}()
	for !task.isCanceled() {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// There is no more data. It means that we finish this time scan task.
		if e == nil {
			log.Debug("No more data",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Any("lowerBound", lowerBound),
				zap.Any("upperBound", upperBound),
				zap.Uint64("currentCommitTs", currentCommitTs),
				zap.Uint64("lastTimeCommitTs", lastTimeCommitTs),
				zap.Uint64("currentTotalSize", currentTotalSize),
				zap.Bool("splitTxn", w.splitTxn),
			)
			break
		}
		// If redo log is enabled, we do not need to update this value.
		// Because it already has been updated in the redo log worker.
		if w.eventCache == nil {
			task.tableSink.updateReceivedSorterCommitTs(e.CRTs)
		}
		task.tableSink.receivedEventCount.Add(1)
		allEventCount += 1
		if e.Row == nil {
			// NOTICE: This could happen when the event is filtered by the event filter.
			// Maybe we just ignore the last event. So we need to record the last position.
			if pos.Valid() {
				lastPos = pos
			}
			continue
		}
		eventSize := e.Row.ApproximateBytes()
		allEventSize += eventSize
		for availableMem-eventSize < 0 {
			if !w.splitTxn {
				// If we do not split the transaction, we do not need to wait for the memory quota.
				// The worst case is all workers are exceeding the memory quota.
				// It will cause out of memory. But it is acceptable for now.
				// Because we split the transaction by default.
				w.memQuota.forceAcquire(requestMemSize)
				log.Debug("MemoryQuotaTracing: Force acquire memory for table sink task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", requestMemSize),
					zap.Bool("splitTxn", w.splitTxn),
				)
			} else {
				// Probably we have to wait for the memory quota.
				// It is OK to block here, there are two scenarios:
				// 1. The task is not canceled, so we can just continue to process the data.
				// 2. The task is canceled, it's also OK to block here, because we will refund the memory quota
				//    after breaking the loop.
				err := w.memQuota.blockAcquire(requestMemSize)
				if err != nil {
					return errors.Trace(err)
				}
				log.Debug("MemoryQuotaTracing: Block acquire memory for table sink task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", requestMemSize),
					zap.Bool("splitTxn", w.splitTxn),
				)
			}
			availableMem += int(requestMemSize)
		}
		availableMem -= eventSize
		events = append(events, e)
		currentCommitTs = e.CRTs
		// For all rows, we add table replicate ts, so mysql sink can
		// determine when to turn off safe-mode.
		e.Row.ReplicatingTs = task.tableSink.replicateTs
		// We meet a finished transaction.
		if pos.Valid() {
			lastPos = pos
			if lastTimeCommitTs == 0 {
				// First time meet a finished transaction. So we always need to append the event with current commit ts.
				if err := appendEventsAndRecordCurrentSize(currentCommitTs); err != nil {
					return errors.Trace(err)
				}
			} else {
				if err := appendEventsAndRecordCurrentSize(lastTimeCommitTs); err != nil {
					return errors.Trace(err)
				}
			}
			// We only update the resolved ts when the currentTotalSize reaches the maxUpdateIntervalSize
			// to avoid updating the resolved ts too frequently.
			if currentTotalSize >= maxUpdateIntervalSize && currentCommitTs > lastTimeCommitTs {
				log.Debug("Advance table sink because met a finished transaction",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Any("lowerBound", lowerBound),
					zap.Any("upperBound", upperBound),
					zap.Uint64("currentCommitTs", currentCommitTs),
					zap.Uint64("lastTimeCommitTs", lastTimeCommitTs),
					zap.Uint64("currentTotalSize", currentTotalSize),
					zap.Bool("splitTxn", w.splitTxn),
				)
				if lastTimeCommitTs == 0 {
					// First time meet a finished transaction. So we always need to advance the with current commit ts.
					if err := advanceTableSinkAndResetCurrentSize(currentCommitTs); err != nil {
						return errors.Trace(err)
					}
				} else {
					if err := advanceTableSinkAndResetCurrentSize(lastTimeCommitTs); err != nil {
						return errors.Trace(err)
					}
				}
				lastTimeCommitTs = currentCommitTs
			}
			if w.splitTxn {
				batchID = 1
			}
			// If no more available memory, we should put the table
			// back to the SinkManager and wait for the next round.
			if !w.memQuota.hasAvailable(requestMemSize) {
				log.Debug("No more available memory and meet a finished transaction",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Any("lowerBound", lowerBound),
					zap.Any("upperBound", upperBound),
					zap.Uint64("currentCommitTs", currentCommitTs),
					zap.Uint64("lastTimeCommitTs", lastTimeCommitTs),
					zap.Uint64("currentTotalSize", currentTotalSize),
					zap.Bool("splitTxn", w.splitTxn),
				)
				break
			}
		} else {
			if w.splitTxn {
				// If it is a split transaction, we should append the event with current commit ts.
				// Also need to update the lastTimeCommitTs.
				// For example:
				// 1. We have two transactions, txn1 and txn2, txn1 has two events, txn2 has one event.
				// 2. There are all small transactions, so we do not advance the table sink.
				// 3. We meet the txn3, and it is a big transaction, if we do not update the lastTimeCommitTs,
				//    next time we will only just advance the table sink with txn2's commit ts.
				// This is make no sense, so we should update the lastTimeCommitTs. Then next time we will
				// advance the table sink with txn3's commit ts.(One big transaction + two small transactions).
				lastTimeCommitTs = currentCommitTs
				if err := appendEventsAndRecordCurrentSize(currentCommitTs); err != nil {
					return errors.Trace(err)
				}
				// If we enable splitTxn, we should emit the events to the sink when the batch size is exceeded.
				if currentTotalSize >= maxBigTxnBatchSize {
					log.Debug("Advance table sink because the batch size is exceeded",
						zap.String("namespace", w.changefeedID.Namespace),
						zap.String("changefeed", w.changefeedID.ID),
						zap.Int64("tableID", task.tableID),
						zap.Any("lowerBound", lowerBound),
						zap.Any("upperBound", upperBound),
						zap.Uint64("currentCommitTs", currentCommitTs),
						zap.Uint64("lastTimeCommitTs", lastTimeCommitTs),
						zap.Uint64("currentTotalSize", currentTotalSize),
						zap.Bool("splitTxn", w.splitTxn),
					)
					if err := advanceTableSinkAndResetCurrentSizeWithBatchID(currentCommitTs); err != nil {
						return errors.Trace(err)
					}
					batchID++
				}
			}
		}
	}
	// Do not forget to refund the unused memory quota.
	w.memQuota.refund(uint64(availableMem))
	log.Debug("MemoryQuotaTracing: Refund unused memory for table sink task",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Int64("tableID", task.tableID),
		zap.Uint64("memory", uint64(availableMem)),
		zap.Bool("splitTxn", w.splitTxn),
	)
	if task.isCanceled() {
		// NOTICE: Maybe we have some txn already in the sink, but we do not care about it.
		// Because canceling a task means the whole table sink is closed.
		if currentTotalSize != 0 {
			w.memQuota.refund(currentTotalSize)
			log.Debug("MemoryQuotaTracing: Refund memory for table sink task when canceling",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Uint64("memory", currentTotalSize),
				zap.Bool("splitTxn", w.splitTxn),
			)
		}
		// Clean up the memory quota.
		// This could happen when the table sink is closed.
		// But the table sink task is still processing the data.
		// If the last time we advance the table sink, then we generate a record in the memory quota.
		// So we need to clean up it.
		cleanedBytes := w.memQuota.clean(task.tableID)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when canceling",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Uint64("memory", cleanedBytes),
			zap.Bool("splitTxn", w.splitTxn),
		)

	} else {
		// This happens when there is no workload for this table on upstream.
		if lastTimeCommitTs == 0 {
			lastTimeCommitTs = upperBound.CommitTs
			log.Debug("No more events, set lastTimeCommitTs to upperBound",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Any("lowerBound", lowerBound),
				zap.Any("upperBound", upperBound),
				zap.Uint64("lastTimeCommitTs", lastTimeCommitTs),
				zap.Bool("splitTxn", w.splitTxn),
			)
			err := advanceTableSinkAndResetCurrentSize(lastTimeCommitTs)
			if err != nil {
				return errors.Trace(err)
			}
			lastPos = upperBound
		}
		// This happens when:
		// 1. We just leave the last txn in the events.
		//    Because we only advance the table sink when meet a new commit ts.
		// 2. We append all the events to the table sink, but because we do not reach the update interval,
		//    we do not advance the table sink.
		if len(events) > 0 || currentTotalSize > 0 {
			if err := appendEventsAndRecordCurrentSize(currentCommitTs); err != nil {
				return errors.Trace(err)
			}
			log.Debug("Advance table sink because some events are not flushed",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Any("lowerBound", lowerBound),
				zap.Any("upperBound", upperBound),
				zap.Uint64("currentCommitTs", currentCommitTs),
				zap.Uint64("lastTimeCommitTs", lastTimeCommitTs),
				zap.Uint64("currentTotalSize", currentTotalSize),
				zap.Bool("splitTxn", w.splitTxn),
			)
			if err := advanceTableSinkAndResetCurrentSize(currentCommitTs); err != nil {
				return errors.Trace(err)
			}
		}
		// Add table back.
		task.callback(lastPos)
	}

	return nil
}

func (w *sinkWorker) fetchFromCache(
	task *sinkTask, // task is read-only here.
	lowerBound engine.Position,
	upperBound engine.Position,
) (engine.Position, error) {
	// Is it possible that after fetching something from cache, more events are
	// pushed into cache immediately? It's unlikely and if it happens, new events
	// are only available after resolvedTs has been advanced. So, here just pop one
	// time is ok.
	rawEventCount := 0
	rows, size, pos := w.eventCache.pop(task.tableID, &rawEventCount, upperBound)
	task.tableSink.receivedEventCount.Add(int64(rawEventCount))
	if size > 0 {
		w.metricRedoEventCacheHit.Add(float64(size))
	}
	if len(rows) > 0 {
		task.tableSink.appendRowChangedEvents(rows...)
		w.memQuota.record(task.tableID, model.ResolvedTs{Ts: pos.CommitTs}, size)
		err := task.tableSink.updateResolvedTs(model.ResolvedTs{Ts: pos.CommitTs})
		if err != nil {
			return engine.Position{}, err
		}
		return pos.Next(), nil
	}
	return lowerBound, nil
}

func (w *sinkWorker) appendEventsToTableSink(t *sinkTask, events []*model.PolymorphicEvent) (uint64, error) {
	log.Debug("Append events to table sink",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("commitTs", events[len(events)-1].CRTs),
		zap.Uint64("startTs", events[len(events)-1].StartTs))
	rowChangedEvents, size, err := convertRowChangedEvents(w.changefeedID, t.tableID, w.enableOldValue, events...)
	if err != nil {
		return 0, err
	}
	t.tableSink.appendRowChangedEvents(rowChangedEvents...)
	return size, nil
}

func (w *sinkWorker) advanceTableSinkWithBatchID(t *sinkTask, commitTs model.Ts, size uint64, batchID uint64) error {
	log.Debug("Advance table sink with batch ID",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("commitTs", commitTs),
		zap.Uint64("batchID", batchID),
	)

	resolvedTs := model.NewResolvedTs(commitTs)
	resolvedTs.Mode = model.BatchResolvedMode
	resolvedTs.BatchID = batchID
	if size > 0 {
		w.memQuota.record(t.tableID, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}

func (w *sinkWorker) advanceTableSink(t *sinkTask, commitTs model.Ts, size uint64) error {
	log.Debug("Advance table sink without batch ID",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("commitTs", commitTs),
	)

	resolvedTs := model.NewResolvedTs(commitTs)
	if size > 0 {
		w.memQuota.record(t.tableID, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}
