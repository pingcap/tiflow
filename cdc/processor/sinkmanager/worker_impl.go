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
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/sorter"
	"go.uber.org/zap"
)

const (
	// defaultRequestMemSize is the default memory usage for a request.
	defaultRequestMemSize = uint64(10 * 1024 * 1024) // 10MB
	// Avoid update resolved ts too frequently, if there are too many small transactions.
	defaultMaxUpdateIntervalSize = uint64(1024 * 256) // 256KB
	// Limit the maximum size of a group of one batch, if there is a big translation.
	defaultMaxBigTxnBatchSize = defaultMaxUpdateIntervalSize * 20 // 5MB
)

// Make these values be variables, so that we can mock them in unit tests.
var (
	requestMemSize        = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
	maxBigTxnBatchSize    = defaultMaxBigTxnBatchSize
)

// Some type assertions.
var (
	_ sinkWorker = (*sinkWorkerImpl)(nil)
	_ redoWorker = (*redoWorkerImpl)(nil)
)

type sinkWorkerImpl struct {
	changefeedID model.ChangeFeedID
	sortEngine   sorter.EventSortEngine
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
	sortEngine sorter.EventSortEngine,
	quota *memQuota,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) sinkWorker {
	return &sinkWorkerImpl{
		changefeedID:   changefeedID,
		sortEngine:     sortEngine,
		memQuota:       quota,
		eventCache:     eventCache,
		splitTxn:       splitTxn,
		enableOldValue: enableOldValue,
	}
}

func (w *sinkWorkerImpl) handleTasks(ctx context.Context, taskChan <-chan *sinkTask) (err error) {
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
			var lastPos sorter.Position
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
			iter := w.sortEngine.FetchByTable(task.tableID, lowerBound, upperBound)
			for !task.isCanceled() {
				e, pos, err := iter.Next()
				if err != nil {
					iter.Close()
					return errors.Trace(err)
				}
				// There is no more data.
				if e == nil {
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

func (w *sinkWorkerImpl) fetchFromCache(
	task *sinkTask, // task is read-only here.
	lowerBound sorter.Position,
	upperBound sorter.Position,
) (sorter.Position, error) {
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
			return sorter.Position{}, err
		}
		return pos.Next(), nil
	}
	return lowerBound, nil
}

func (w *sinkWorkerImpl) appendEventsToTableSink(t *sinkTask, events []*model.PolymorphicEvent) (uint64, error) {
	rowChangedEvents, size, err := convertRowChangedEvents(w.changefeedID, t.tableID, w.enableOldValue, events...)
	if err != nil {
		return 0, err
	}
	t.tableSink.appendRowChangedEvents(rowChangedEvents...)
	return size, nil
}

func (w *sinkWorkerImpl) advanceTableSink(t *sinkTask, commitTs model.Ts, size uint64, batchID uint64) error {
	resolvedTs := model.NewResolvedTs(commitTs)
	if w.splitTxn {
		resolvedTs.Mode = model.BatchResolvedMode
		resolvedTs.BatchID = batchID
	}
	w.memQuota.record(t.tableID, resolvedTs, size)
	return t.tableSink.updateResolvedTs(resolvedTs)
}

type redoWorkerImpl struct {
	changefeedID   model.ChangeFeedID
	sortEngine     sorter.EventSortEngine
	memQuota       *memQuota
	redoManager    redo.LogManager
	eventCache     *redoEventCache
	splitTxn       bool
	enableOldValue bool
}

func newRedoWorker(
	changefeedID model.ChangeFeedID,
	sortEngine sorter.EventSortEngine,
	quota *memQuota,
	redoManager redo.LogManager,
	eventCache *redoEventCache,
	splitTxn bool,
	enableOldValue bool,
) redoWorker {
	return &redoWorkerImpl{
		changefeedID:   changefeedID,
		sortEngine:     sortEngine,
		memQuota:       quota,
		redoManager:    redoManager,
		eventCache:     eventCache,
		splitTxn:       splitTxn,
		enableOldValue: enableOldValue,
	}
}

func (w *redoWorkerImpl) handleTasks(ctx context.Context, taskChan <-chan *redoTask) error {
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

func (w *redoWorkerImpl) handleTask(ctx context.Context, task *redoTask) error {
	rows := make([]*model.RowChangedEvent, 0, 1024)
	cache := w.eventCache.getAppender(task.tableID)

	// Events are pushed into redoEventCache if possible. Otherwise, their memory will
	// be released after they are written into redo files. Then we need to release their
	// memory quota, which can be calculated based on batchSize and cachedSize.
	batchSize := uint64(0)
	cachedSize := uint64(0)

	memAllocated := true

	var lastPos sorter.Position
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
	iter := w.sortEngine.FetchByTable(task.tableID, task.lowerBound, task.getUpperBound())
	defer iter.Close()
	for memAllocated {
		e, pos, err := iter.Next()
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
