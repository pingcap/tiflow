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
	availableMem := requestMemSize
	usedMem := uint64(0)

	events := make([]*model.RowChangedEvent, 0, 1024)
	lowerBound := task.lowerBound
	upperBound := task.getUpperBound(task.tableSink)

	if w.eventCache != nil {
		newLowerBound, newUpperBound, err := w.fetchFromCache(task, lowerBound, upperBound)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("adjust task boundaries based on redo event cache",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Any("newLowerBound", newLowerBound),
			zap.Any("newUpperBound", newUpperBound))
	}

	// Used to record the last written position.
	// We need to use it to update the lower bound of the table sink.
	var lastPos engine.Position

	committedTxnSize := uint64(0)
	pendingTxnSize := uint64(0)
	lastTxnCommitTs := uint64(0)
	currTxnCommitTs := uint64(0)
	lastTxnOffset := -1 // end offset of the last transaction in events.
	batchID := uint64(1)

	needEmitAndAdvance := func() bool {
		return (w.splitTxn && committedTxnSize+pendingTxnSize >= maxUpdateIntervalSize) ||
			(!w.splitTxn && committedTxnSize >= maxUpdateIntervalSize)
	}
	doEmitAndAdvance := func(allFinished bool) (err error) {
		if w.splitTxn {
			if len(events) > 0 {
				task.tableSink.appendRowChangedEvents(events...)
				events = events[:0]
				if cap(events) > 1024 {
					events = make([]*model.RowChangedEvent, 0, 1024)
				}
			}
			if currTxnCommitTs == 0 {
				if !lastPos.Valid() {
					return
				}
				// Generally includes 2 cases:
				// 1. nothing is fetched from the iterator;
				// 2. some filtered events are fetched.
				currTxnCommitTs = lastPos.CommitTs
			}
			if allFinished {
				err = w.advanceTableSink(task, currTxnCommitTs, committedTxnSize+pendingTxnSize)
			} else {
				err = w.advanceTableSinkWithBatchID(task, currTxnCommitTs, committedTxnSize+pendingTxnSize, batchID)
				batchID += 1
			}
			committedTxnSize = 0
			pendingTxnSize = 0
		} else {
			if lastTxnOffset >= 0 {
				task.tableSink.appendRowChangedEvents(events[0 : lastTxnOffset+1]...)
				events = events[lastTxnOffset+1:]
			}
			if lastTxnCommitTs == 0 {
				if !lastPos.Valid() {
					return
				}
				// Check lastPos to know whether there could be more transactions
				// with same CommitTs as this one.
				if lastPos.StartTs+1 == lastPos.CommitTs {
					lastTxnCommitTs = lastPos.CommitTs
				} else {
					lastTxnCommitTs = lastPos.CommitTs - 1
				}
			}
			err = w.advanceTableSink(task, lastTxnCommitTs, committedTxnSize)
			committedTxnSize = 0
			lastTxnOffset = -1
		}
		return
	}

	maybeEmitAndAdvance := func(allFinished, txnFinished bool) error {
		memoryHighUsage := availableMem < usedMem
		if memoryHighUsage || allFinished || needEmitAndAdvance() {
			if err := doEmitAndAdvance(allFinished); err != nil {
				return errors.Trace(err)
			}
		}

		if memoryHighUsage {
			w.memQuota.forceAcquire(usedMem - availableMem)
			log.Debug("MemoryQuotaTracing: force acquire memory for table sink task",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Int64("tableID", task.tableID),
				zap.Uint64("memory", usedMem-availableMem))
			availableMem = usedMem
		}

		if allFinished {
			// There is no more events and some required memory isn't used.
			if availableMem > usedMem {
				w.memQuota.refund(availableMem - usedMem)
				log.Debug("MemoryQuotaTracing: refund memory for table sink task",
					zap.String("namespace", w.changefeedID.Namespace),
					zap.String("changefeed", w.changefeedID.ID),
					zap.Int64("tableID", task.tableID),
					zap.Uint64("memory", availableMem-usedMem))
			}
		} else if usedMem >= availableMem {
			if txnFinished {
				if w.memQuota.tryAcquire(requestMemSize) {
					availableMem += requestMemSize
					log.Debug("MemoryQuotaTracing: try acquire memory for table sink task",
						zap.String("namespace", w.changefeedID.Namespace),
						zap.String("changefeed", w.changefeedID.ID),
						zap.Int64("tableID", task.tableID),
						zap.Uint64("memory", requestMemSize))
				}
			} else {
				if w.memQuota.blockAcquire(requestMemSize) == nil {
					availableMem += requestMemSize
					log.Debug("MemoryQuotaTracing: block acquire memory for table sink task",
						zap.String("namespace", w.changefeedID.Namespace),
						zap.String("changefeed", w.changefeedID.ID),
						zap.Int64("tableID", task.tableID),
						zap.Uint64("memory", requestMemSize))
				}
			}
		}
		return nil
	}

	// lowerBound and upperBound are both closed intervals.
	allEventSize := uint64(0)
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

		log.Debug("Sink task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Int64("tableID", task.tableID),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Bool("splitTxn", w.splitTxn),
			zap.Uint64("commitTs", lastPos.CommitTs),
			zap.Uint64("startTs", lastPos.StartTs),
		)
		task.callback(lastPos)
	}()

	for availableMem > usedMem {
		e, pos, err := iter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// There is no more data. It means that we finish this time scan task.
		if e == nil {
			if !lastPos.Valid() {
				lastPos = upperBound
			}
			if err := maybeEmitAndAdvance(true, true); err != nil {
				return errors.Trace(err)
			}
			return nil
		}

		task.tableSink.receivedEventCount.Add(1)
		allEventCount += 1
		if pos.Valid() {
			lastPos = pos
		}

		// If redo log is enabled, we do not need to update this value.
		// Because it already has been updated in the redo log worker.
		if w.eventCache == nil {
			task.tableSink.updateReceivedSorterCommitTs(e.CRTs)
		}

		if e.Row == nil {
			// NOTICE: This could happen when the event is filtered by the event filter.
			// Maybe we just ignore the last event. So we need to record the last position.
			continue
		}

		x, size, err := convertRowChangedEvents(w.changefeedID, task.tableID, w.enableOldValue, e)
		if err != nil {
			return err
		}
		allEventSize += size
		usedMem += size
		if len(x) > 0 {
			currTxnCommitTs = x[0].CommitTs
			if len(events) > 0 && events[len(events)-1].CommitTs != currTxnCommitTs {
				lastTxnCommitTs = events[len(events)-1].CommitTs
				committedTxnSize += pendingTxnSize
				pendingTxnSize = size
				lastTxnOffset = len(events) - 1
				batchID = 1
			} else {
				pendingTxnSize += size
			}
			events = append(events, x...)
			if err := maybeEmitAndAdvance(false, pos.Valid()); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return doEmitAndAdvance(false)
}

func (w *sinkWorker) fetchFromCache(
	task *sinkTask, // task is read-only here.
	lowerBound engine.Position,
	upperBound engine.Position,
) (newLowerBound, newUpperBound engine.Position, err error) {
	newLowerBound = lowerBound
	newUpperBound = upperBound

	cache := w.eventCache.getAppender(task.tableID)
	if cache != nil {
		popRes := cache.pop(lowerBound, upperBound)
		if popRes.success {
			if len(popRes.events) > 0 {
				task.tableSink.receivedEventCount.Add(int64(popRes.pushCount))
				w.metricRedoEventCacheHit.Add(float64(popRes.size))
				task.tableSink.appendRowChangedEvents(popRes.events...)
				w.memQuota.record(task.tableID, model.ResolvedTs{Ts: popRes.boundary.CommitTs}, popRes.releaseSize)
			}
			err = task.tableSink.updateResolvedTs(model.ResolvedTs{Ts: popRes.boundary.CommitTs})
			if err == nil {
				newLowerBound = popRes.boundary.Next()
			}
		} else {
			newUpperBound = popRes.boundary.Prev()
		}
	}
	return
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
