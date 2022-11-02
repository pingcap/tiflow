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
	defaultRequestMemSize = 10 * 1024 * 1024 // 10MB
	// maxUpdateIntervalSize is the maximum size of events that can be updated in a single batch.
	// It has two purposes:
	// 1. Avoid update resolved ts too frequently, if there are too many small transactions.
	// 2. Limit the maximum size of a group of one batch, if there is a big translation.
	maxUpdateIntervalSize = 1024 * 1024 // 1MB
)

// Assert that workerImpl implements worker.
var _ worker = (*workerImpl)(nil)

type workerImpl struct {
	changefeedID model.ChangeFeedID
	sortEngine   sorter.EventSortEngine
	// redoManager only has value when the redo log is enabled.
	redoManager redo.LogManager
	memQuota    *changefeedMemQuota
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool
	// enableOldValue indicates whether to enable the old value feature.
	// If it is enabled, we need to deal with the compatibility of the data format.
	enableOldValue bool
}

// newWorker creates a new worker.
// nolint:deadcode
func newWorker(
	changefeedID model.ChangeFeedID,
	sortEngine sorter.EventSortEngine,
	redoManager redo.LogManager,
	quota *changefeedMemQuota,
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
			availableMem := defaultRequestMemSize
			events := make([]*model.PolymorphicEvent, 0, 1024)

			// Used to record the last written position.
			// We need to use it to update the lower bound of the table sink.
			var lastPos sorter.Position
			currentTotalSize := uint64(0)
			batchID := uint64(1)
			// We have to get the latest barrier ts, this is because the barrier ts may be updated.
			// We always want to get the latest value.
			currentBarrierTs := task.upperBarrierTs.Load()
			upperBound := sorter.Position{
				StartTs:  currentBarrierTs - 1,
				CommitTs: currentBarrierTs,
			}

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
			advanceTableSinkAndResetCurrentSize := func(event *model.PolymorphicEvent) error {
				err := w.advanceTableSink(task, event.CRTs, currentTotalSize, batchID)
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
					// This means that we append all the events to the table sink.
					// But we have not updated the resolved ts.
					// Because we do not reach the maxUpdateIntervalSize.
					if currentTotalSize != 0 {
						if err := advanceTableSinkAndResetCurrentSize(e); err != nil {
							return errors.Trace(err)
						}
					}
					break
				}
				for availableMem-e.Row.ApproximateBytes() < 0 {
					// This probably cause out of memory.
					// TODO: find a better way to control how many workers
					//       can executed the memory quota at the same time.
					w.memQuota.forceAcquire(defaultRequestMemSize)
					availableMem += defaultRequestMemSize
				}
				availableMem -= e.Row.ApproximateBytes()
				events = append(events, e)
				// We meet a finished transaction.
				if pos.Valid() {
					lastPos = pos
					// Always append the events to the sink.
					// Whatever splitTxn is true or false, we should emit the events to the sink as soon as possible.
					if err := appendEventsAndRecordCurrentSize(); err != nil {
						return errors.Trace(err)
					}
					if w.splitTxn {
						batchID = 1
					}
					// 1) If we need to split the transaction into multiple batches,
					// 	  we have to update the resolved ts as soon as possible.
					// 2) If we do not need to split the transaction into multiple batches,
					//    we only update the resolved ts when the currentTotalSize reaches the maxUpdateIntervalSize
					//    to avoid updating the resolved ts too frequently.
					if w.splitTxn || currentTotalSize >= maxUpdateIntervalSize {
						if err := advanceTableSinkAndResetCurrentSize(e); err != nil {
							return errors.Trace(err)
						}
					}
					// If no more available memory, we should put the table
					// back to the SinkManager and wait for the next round.
					if !w.memQuota.hasAvailable(defaultRequestMemSize) {
						break
					}
				} else {
					if w.splitTxn {
						// If we enable splitTxn, we should emit the events to the sink when the batch size is exceeded.
						if currentTotalSize >= maxUpdateIntervalSize*50 {
							if err := appendEventsAndRecordCurrentSize(); err != nil {
								return errors.Trace(err)
							}
							if err := advanceTableSinkAndResetCurrentSize(e); err != nil {
								return errors.Trace(err)
							}
							batchID++
						}
					}
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
