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
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"go.uber.org/zap"
)

type sinkWorker struct {
	changefeedID model.ChangeFeedID
	mg           entry.MounterGroup
	sortEngine   engine.SortEngine
	memQuota     *memQuota
	eventCache   *redoEventCache
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool
	// enableOldValue indicates whether to enable the old value feature.
	// If it is enabled, we need to deal with the compatibility of the data format.
	enableOldValue bool
}

// newWorker creates a new worker.
func newSinkWorker(
	changefeedID model.ChangeFeedID,
	mg entry.MounterGroup,
	sortEngine engine.SortEngine,
	quota *memQuota,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) *sinkWorker {
	return &sinkWorker{
		changefeedID:   changefeedID,
		mg:             mg,
		sortEngine:     sortEngine,
		memQuota:       quota,
		eventCache:     eventCache,
		splitTxn:       splitTxn,
		enableOldValue: enableOldValue,
	}
}

func (w *sinkWorker) handleTasks(ctx context.Context, taskChan <-chan *sinkTask) (err error) {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-taskChan:
			// First time to run the task, we have initialized memory quota for the table.
			availableMem := int(requestMemSize)
			events := make([]*model.PolymorphicEvent, 0, 1024)

			lowerBound := task.lowerBound
			upperBound := task.getUpperBound()

			if w.eventCache != nil {
				lowerBound, err = w.fetchFromCache(task, lowerBound, upperBound)
				if err != nil {
					return errors.Trace(err)
				}
			}

			// Used to record the last written position.
			// We need to use it to update the lower bound of the table sink.
			var lastPos engine.Position
			lastCommitTs := uint64(0)
			currentTotalSize := uint64(0)
			batchID := uint64(1)

			// Two functions to simplify the code.
			// It captures some variables in the outer scope.
			appendEventsAndRecordCurrentSize := func() error {
				size, err := w.appendEventsToTableSink(task, events)
				if err != nil {
					return errors.Trace(err)
				}
				currentTotalSize += size
				events = events[:0]
				return nil
			}
			advanceTableSinkAndResetCurrentSize := func() error {
				err := w.advanceTableSink(task, lastCommitTs, currentTotalSize, batchID)
				if err != nil {
					return errors.Trace(err)
				}
				currentTotalSize = 0
				return nil
			}

			// lowerBound and upperBound are both closed intervals.
			iter := engine.NewMountedEventIter(
				w.sortEngine.FetchByTable(task.tableID, lowerBound, upperBound),
				w.mg, 256)
			for !task.isCanceled() {
				e, pos, err := iter.Next(ctx)
				if err != nil {
					iter.Close()
					return errors.Trace(err)
				}
				// There is no more data.
				if e == nil {
					break
				}
				if e.Row == nil {
					// NOTICE: This could happen when the event is filtered by the event filter.
					break
				}
				for availableMem-e.Row.ApproximateBytes() < 0 {
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
						)
					} else {
						// Probably we have to wait for the memory quota.
						// It is OK to block here, there are two scenarios:
						// 1. The task is not canceled, so we can just continue to process the data.
						// 2. The task is canceled, it's also OK to block here, because we will refund the memory quota
						//    after breaking the loop.
						err := w.memQuota.blockAcquire(requestMemSize)
						if err != nil {
							iter.Close()
							return errors.Trace(err)
						}
						log.Debug("MemoryQuotaTracing: Block acquire memory for table sink task",
							zap.String("namespace", w.changefeedID.Namespace),
							zap.String("changefeed", w.changefeedID.ID),
							zap.Int64("tableID", task.tableID),
							zap.Uint64("memory", requestMemSize),
						)
					}
					availableMem += int(requestMemSize)
				}
				eventSize := e.Row.ApproximateBytes()
				availableMem -= eventSize
				events = append(events, e)
				lastCommitTs = e.CRTs
				// We meet a finished transaction.
				if pos.Valid() {
					lastPos = pos
					// Always append the events to the sink.
					// Whatever splitTxn is true or false, we should emit the events to the sink as soon as possible.
					if err := appendEventsAndRecordCurrentSize(); err != nil {
						iter.Close()
						return errors.Trace(err)
					}
					// 1) If we need to split the transaction into multiple batches,
					// 	  we have to update the resolved ts as soon as possible.
					// 2) If we do not need to split the transaction into multiple batches,
					//    we only update the resolved ts when the currentTotalSize reaches the maxUpdateIntervalSize
					//    to avoid updating the resolved ts too frequently.
					if w.splitTxn || currentTotalSize >= maxUpdateIntervalSize {
						if err := advanceTableSinkAndResetCurrentSize(); err != nil {
							iter.Close()
							return errors.Trace(err)
						}
					}
					if w.splitTxn {
						batchID = 1
					}
					// If no more available memory, we should put the table
					// back to the SinkManager and wait for the next round.
					if !w.memQuota.hasAvailable(requestMemSize) {
						break
					}
				} else {
					if w.splitTxn {
						if err := appendEventsAndRecordCurrentSize(); err != nil {
							iter.Close()
							return errors.Trace(err)
						}
						// If we enable splitTxn, we should emit the events to the sink when the batch size is exceeded.
						if currentTotalSize >= maxBigTxnBatchSize {
							if err := advanceTableSinkAndResetCurrentSize(); err != nil {
								iter.Close()
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
				)

			} else {
				if lastCommitTs == 0 {
					lastCommitTs = upperBound.CommitTs
					err := advanceTableSinkAndResetCurrentSize()
					if err != nil {
						return errors.Trace(err)
					}
					lastPos = upperBound
				}
				// This means that we append all the events to the table sink.
				// But we have not updated the resolved ts.
				// Because we do not reach the maxUpdateIntervalSize.
				if currentTotalSize != 0 {
					if err := advanceTableSinkAndResetCurrentSize(); err != nil {
						iter.Close()
						return errors.Trace(err)
					}
				}
				// Add table back.
				task.callback(lastPos)
			}
			if err := iter.Close(); err != nil {
				return errors.Trace(err)
			}
		}
	}
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
	rows, size, pos := w.eventCache.pop(task.tableID, upperBound)
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
	rowChangedEvents, size, err := convertRowChangedEvents(w.changefeedID, t.tableID, w.enableOldValue, events...)
	if err != nil {
		return 0, err
	}
	t.tableSink.appendRowChangedEvents(rowChangedEvents...)
	return size, nil
}

func (w *sinkWorker) advanceTableSink(t *sinkTask, commitTs model.Ts, size uint64, batchID uint64) error {
	log.Debug("Advance table sink",
		zap.String("namespace", w.changefeedID.Namespace),
		zap.String("changefeed", w.changefeedID.ID),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("commitTs", commitTs))

	resolvedTs := model.NewResolvedTs(commitTs)
	if w.splitTxn {
		resolvedTs.Mode = model.BatchResolvedMode
		resolvedTs.BatchID = batchID
	}
	if size > 0 {
		w.memQuota.record(t.tableID, resolvedTs, size)
	}
	return t.tableSink.updateResolvedTs(resolvedTs)
}
