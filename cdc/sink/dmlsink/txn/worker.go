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

package txn

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics/txn"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/causality"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type worker struct {
	ctx         context.Context
	changefeed  string
	workerCount int

	ID      int
	backend backend

	// Metrics.
	metricConflictDetectDuration prometheus.Observer
	metricQueueDuration          prometheus.Observer
	metricTxnWorkerFlushDuration prometheus.Observer
	metricTxnWorkerBusyRatio     prometheus.Counter
	metricTxnWorkerHandledRows   prometheus.Counter

	// Fields only used in the background loop.
	flushInterval            time.Duration
	hasPending               bool
	postTxnExecutedCallbacks []func()
}

func newWorker(ctx context.Context, changefeedID model.ChangeFeedID,
	ID int, backend backend, workerCount int,
) *worker {
	wid := fmt.Sprintf("%d", ID)

	return &worker{
		ctx:         ctx,
		changefeed:  fmt.Sprintf("%s.%s", changefeedID.Namespace, changefeedID.ID),
		workerCount: workerCount,

		ID:      ID,
		backend: backend,

		metricConflictDetectDuration: txn.ConflictDetectDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricQueueDuration:          txn.QueueDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerFlushDuration: txn.WorkerFlushDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerBusyRatio:     txn.WorkerBusyRatio.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerHandledRows:   txn.WorkerHandledRows.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),

		flushInterval:            backend.MaxFlushInterval(),
		hasPending:               false,
		postTxnExecutedCallbacks: make([]func(), 0, 1024),
	}
}

// Run a loop.
func (w *worker) runLoop(txnCh <-chan causality.TxnWithNotifier[*txnEvent]) error {
	defer func() {
		if err := w.backend.Close(); err != nil {
			log.Info("Transaction dmlSink backend close fail",
				zap.String("changefeedID", w.changefeed),
				zap.Int("workerID", w.ID),
				zap.Error(err))
		}
	}()
	log.Info("Transaction dmlSink worker starts",
		zap.String("changefeedID", w.changefeed),
		zap.Int("workerID", w.ID))

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	needFlush := false
	var flushTimeSlice, totalTimeSlice time.Duration
	overseerTicker := time.NewTicker(time.Second)
	defer overseerTicker.Stop()
	startToWork := time.Now()
	for {
		select {
		case <-w.ctx.Done():
			log.Info("Transaction dmlSink worker exits as canceled",
				zap.String("changefeedID", w.changefeed),
				zap.Int("workerID", w.ID))
			return nil
		case txn := <-txnCh:
			if txn.TxnEvent != nil {
				needFlush = w.onEvent(txn.TxnEvent, txn.PostTxnExecuted)
			}
		case <-ticker.C:
			needFlush = true
		case now := <-overseerTicker.C:
			totalTimeSlice = now.Sub(startToWork)
			busyRatio := int(flushTimeSlice.Seconds() / totalTimeSlice.Seconds() * 1000)
			w.metricTxnWorkerBusyRatio.Add(float64(busyRatio) / float64(w.workerCount))
			startToWork = now
			flushTimeSlice = 0
		}
		if needFlush {
			if err := w.doFlush(&flushTimeSlice); err != nil {
				log.Error("Transaction dmlSink worker exits unexpectly",
					zap.String("changefeedID", w.changefeed),
					zap.Int("workerID", w.ID),
					zap.Error(err))
				return err
			}
			needFlush = false
		}
	}
}

// onEvent is called when a new event is received.
// It returns true if the event is sent to backend.
func (w *worker) onEvent(txn *txnEvent, postTxnExecuted func()) bool {
	w.hasPending = true

	if txn.GetTableSinkState() != state.TableSinkSinking {
		// The table where the event comes from is in stopping, so it's safe
		// to drop the event directly.
		txn.Callback()
		// Still necessary to append the callbacks into the pending list.
		w.postTxnExecutedCallbacks = append(w.postTxnExecutedCallbacks, postTxnExecuted)
		return false
	}

	w.metricConflictDetectDuration.Observe(txn.conflictResolved.Sub(txn.start).Seconds())
	w.metricQueueDuration.Observe(time.Since(txn.start).Seconds())
	w.metricTxnWorkerHandledRows.Add(float64(len(txn.Event.Rows)))
	w.postTxnExecutedCallbacks = append(w.postTxnExecutedCallbacks, postTxnExecuted)
	return w.backend.OnTxnEvent(txn.TxnCallbackableEvent)
}

// doFlush flushes the backend.
// It returns true only if it can no longer be flushed.
func (w *worker) doFlush(flushTimeSlice *time.Duration) error {
	if w.hasPending {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			*flushTimeSlice += elapsed
			w.metricTxnWorkerFlushDuration.Observe(elapsed.Seconds())
		}()

		if err := w.backend.Flush(w.ctx); err != nil {
			log.Warn("Transaction dmlSink backend flush fail",
				zap.String("changefeedID", w.changefeed),
				zap.Int("workerID", w.ID),
				zap.Error(err))
			return err
		}
		// Flush successfully, call callbacks to notify conflict detector.
		for _, postTxnExecuted := range w.postTxnExecutedCallbacks {
			postTxnExecuted()
		}
		w.postTxnExecutedCallbacks = w.postTxnExecutedCallbacks[:0]
		if cap(w.postTxnExecutedCallbacks) > 1024 {
			// Resize the buffer if it's too big.
			w.postTxnExecutedCallbacks = make([]func(), 0, 1024)
		}
	}

	w.hasPending = false
	return nil
}
