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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// batchID is used to advance table sink with a given CommitTs, even if not all
// transactions with the same CommitTs are collected, regardless of whether splitTxn
// is enabled or not. We split transactions with the same CommitTs even if splitTxn
// is false, and it won't break transaction atomicity to downstream.
// NOTICE:
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
	// splitTxn indicates whether to split the transaction into multiple batches.
	splitTxn bool

	// Metrics.
	metricOutputEventCountKV prometheus.Counter
}

// newSinkWorker creates a new sink worker.
func newSinkWorker(
	changefeedID model.ChangeFeedID,
	sourceManager *sourcemanager.SourceManager,
	sinkQuota *memquota.MemQuota,
	splitTxn bool,
) *sinkWorker {
	return &sinkWorker{
		changefeedID:  changefeedID,
		sourceManager: sourceManager,
		sinkMemQuota:  sinkQuota,
		splitTxn:      splitTxn,

		metricOutputEventCountKV: outputEventCount.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "kv"),
	}
}

func (w *sinkWorker) handleTasks(ctx context.Context, taskChan <-chan *sinkTask) error {
	failpoint.Inject("SinkWorkerTaskHandlePause", func() { <-ctx.Done() })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-taskChan:
			err := w.handleTask(ctx, task)
			failpoint.Inject("SinkWorkerTaskError", func() {
				err = errors.New("SinkWorkerTaskError")
			})
			if err != nil {
				return err
			}
		}
	}
}

func (w *sinkWorker) handleTask(ctx context.Context, task *sinkTask) (finalErr error) {
	// We need to use a new batch ID for each task.
	batchID.Add(1)
	advancer := newTableSinkAdvancer(task, w.splitTxn, w.sinkMemQuota, requestMemSize)
	// The task is finished and some required memory isn't used.
	defer advancer.cleanup()

	lowerBound, upperBound := validateAndAdjustBound(
		w.changefeedID,
		&task.span,
		task.lowerBound,
		task.getUpperBound(task.tableSink.getUpperBoundTs()))
	advancer.lastPos = lowerBound.Prev()

	allEventSize := uint64(0)
	allEventCount := 0

	callbackIsPerformed := false
	performCallback := func(pos sorter.Position) {
		if !callbackIsPerformed {
			task.callback(pos)
			callbackIsPerformed = true
		}
	}

	defer func() {
		// Collect metrics.
		w.metricOutputEventCountKV.Add(float64(allEventCount))

		log.Debug("Sink task finished",
			zap.String("namespace", w.changefeedID.Namespace),
			zap.String("changefeed", w.changefeedID.ID),
			zap.Stringer("span", &task.span),
			zap.Any("lowerBound", lowerBound),
			zap.Any("upperBound", upperBound),
			zap.Bool("splitTxn", w.splitTxn),
			zap.Int("receivedEvents", allEventCount),
			zap.Any("lastPos", advancer.lastPos),
			zap.Float64("lag", time.Since(oracle.GetTimeFromTS(advancer.lastPos.CommitTs)).Seconds()),
			zap.Error(finalErr))

		// Otherwise we can't ensure all events before `lastPos` are emitted.
		if finalErr == nil {
			performCallback(advancer.lastPos)
		} else {
			switch errors.Cause(finalErr).(type) {
			// If it's a warning, close the table sink and wait all pending
			// events have been reported. Then we can continue the table
			// at the checkpoint position.
			case tablesink.SinkInternalError:
				// After the table sink is cleared all pending events are sent out or dropped.
				// So we can re-add the table into sinkMemQuota.
				w.sinkMemQuota.ClearTable(task.tableSink.span)
				performCallback(advancer.lastPos)
				finalErr = nil
			default:
			}
		}
	}()

	// lowerBound and upperBound are both closed intervals.
	iter := w.sourceManager.FetchByTable(task.span, lowerBound, upperBound, w.sinkMemQuota)
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error("Sink worker fails to close iterator",
				zap.String("namespace", w.changefeedID.Namespace),
				zap.String("changefeed", w.changefeedID.ID),
				zap.Stringer("span", &task.span),
				zap.Error(err))
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
			x, size := handleRowChangedEvents(w.changefeedID, task.span, e)
			advancer.appendEvents(x, size)
			allEventSize += size
		}

		if err := advancer.tryAdvanceAndAcquireMem(false, pos.Valid()); err != nil {
			return errors.Trace(err)
		}
	}

	return advancer.lastTimeAdvance()
}
