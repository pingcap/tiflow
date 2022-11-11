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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/sorter"
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

// Assert that workerImpl implements worker.
var _ worker = (*workerImpl)(nil)

type workerImpl struct {
	changefeedID model.ChangeFeedID
	sortEngine   sorter.EventSortEngine
	// redoManager only has value when the redo log is enabled.
	redoManager redo.LogManager
	memQuota    *memQuota
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool
	// enableOldValue indicates whether to enable the old value feature.
	// If it is enabled, we need to deal with the compatibility of the data format.
	enableOldValue bool
}

// newWorker creates a new worker.
func newWorker(
	changefeedID model.ChangeFeedID,
	sortEngine sorter.EventSortEngine,
	redoManager redo.LogManager,
	quota *memQuota,
	splitTxn bool,
	enableOldValue bool,
) worker {
	return &workerImpl{
		changefeedID:   changefeedID,
		sortEngine:     sortEngine,
		redoManager:    redoManager,
		memQuota:       quota,
		splitTxn:       splitTxn,
		enableOldValue: enableOldValue,
	}
}

func (w *workerImpl) receiveTableSinkTask(ctx context.Context, taskChan <-chan *tableSinkTask) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-taskChan:
			// First time to run the task, we have initialized memory quota for the table.
			availableMem := int(requestMemSize)
			events := make([]*model.PolymorphicEvent, 0, 1024)

			// Used to record the last written position.
			// We need to use it to update the lower bound of the table sink.
			var lastPos sorter.Position
			lastCommitTs := uint64(0)
			currentTotalSize := uint64(0)
			batchID := uint64(1)
			// We have to get the latest value, this is because the barrier ts or resolved ts may be updated.
			// We always want to get the latest value.
			upperBound := task.upperBarrierTsGetter()

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
			iter := w.sortEngine.FetchByTable(task.tableID, task.lowerBound, upperBound)
			for {
				e, pos, err := iter.Next()
				if err != nil {
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
					} else {
						err := w.memQuota.blockAcquire(requestMemSize)
						if err != nil {
							return errors.Trace(err)
						}
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
						return errors.Trace(err)
					}
					// 1) If we need to split the transaction into multiple batches,
					// 	  we have to update the resolved ts as soon as possible.
					// 2) If we do not need to split the transaction into multiple batches,
					//    we only update the resolved ts when the currentTotalSize reaches the maxUpdateIntervalSize
					//    to avoid updating the resolved ts too frequently.
					if w.splitTxn || currentTotalSize >= maxUpdateIntervalSize {
						if err := advanceTableSinkAndResetCurrentSize(); err != nil {
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
							return errors.Trace(err)
						}
						// If we enable splitTxn, we should emit the events to the sink when the batch size is exceeded.
						if currentTotalSize >= maxBigTxnBatchSize {
							if err := advanceTableSinkAndResetCurrentSize(); err != nil {
								return errors.Trace(err)
							}
							batchID++
						}
					}
				}
			}
			// This means that we append all the events to the table sink.
			// But we have not updated the resolved ts.
			// Because we do not reach the maxUpdateIntervalSize.
			if currentTotalSize != 0 {
				if err := advanceTableSinkAndResetCurrentSize(); err != nil {
					return errors.Trace(err)
				}
			}
			// Do not forget to refund the useless memory quota.
			w.memQuota.refund(uint64(availableMem))
			// Add table back.
			task.callback(lastPos)
			if err := iter.Close(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *workerImpl) appendEventsToTableSink(t *tableSinkTask, events []*model.PolymorphicEvent) (uint64, error) {
	rowChangedEvents, size, err := convertRowChangedEvents(w.changefeedID, t.tableID, w.enableOldValue, events...)
	if err != nil {
		return 0, err
	}
	t.tableSink.appendRowChangedEvents(rowChangedEvents...)
	return size, nil
}

func (w *workerImpl) advanceTableSink(t *tableSinkTask, commitTs model.Ts, size uint64, batchID uint64) error {
	resolvedTs := model.NewResolvedTs(commitTs)
	if w.splitTxn {
		resolvedTs.Mode = model.BatchResolvedMode
		resolvedTs.BatchID = batchID
	}
	w.memQuota.record(t.tableID, resolvedTs, size)
	return t.tableSink.updateResolvedTs(resolvedTs)
}
