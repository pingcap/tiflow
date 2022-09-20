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
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/pingcap/tiflow/pkg/chann"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type txnWithNotifier struct {
	*txnEvent
	wantMore func()
}

type worker struct {
	ctx         context.Context
	changefeed  string
	workerCount int

	ID      int
	txnCh   *chann.Chann[txnWithNotifier]
	stopped chan struct{}
	wg      sync.WaitGroup
	backend backend
	errCh   chan<- error

	// Metrics.
	metricConflictDetectDuration prometheus.Observer
	metricTxnWorkerFlushDuration prometheus.Observer
	metricTxnWorkerBusyRatio     prometheus.Counter
	metricTxnWorkerHandledRows   prometheus.Counter

	// Fields only used in the background loop.
	hasPending        bool
	wantMoreCallbacks []func()
}

func newWorker(ctx context.Context, ID int, backend backend, errCh chan<- error, workerCount int) *worker {
	wid := fmt.Sprintf("%d", ID)
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	return &worker{
		ctx:         ctx,
		changefeed:  fmt.Sprintf("%s.%s", changefeedID.Namespace, changefeedID.ID),
		workerCount: workerCount,

		ID:      ID,
		txnCh:   chann.New[txnWithNotifier](chann.Cap(-1 /*unbounded*/)),
		stopped: make(chan struct{}),
		backend: backend,
		errCh:   errCh,

		metricConflictDetectDuration: metrics.ConflictDetectDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerFlushDuration: metrics.TxnWorkerFlushDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerBusyRatio:     metrics.TxnWorkerBusyRatio.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerHandledRows:   metrics.TxnWorkerHandledRows.WithLabelValues(changefeedID.Namespace, changefeedID.ID, wid),

		hasPending:        false,
		wantMoreCallbacks: make([]func(), 0, 1024),
	}
}

func (w *worker) Add(txn *txnEvent, unlock func()) {
	w.txnCh.In() <- txnWithNotifier{txn, unlock}
}

func (w *worker) Close() {
	close(w.stopped)
	w.wg.Wait()
	w.txnCh.Close()
}

// Run a background loop.
func (w *worker) runBackgroundLoop() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() {
			if err := w.backend.Close(); err != nil {
				log.Info("Transaction sink backend close fail",
					zap.String("changefeedID", w.changefeed),
					zap.Int("workerID", w.ID),
					zap.Error(err))
			}
		}()
		defer func() {
			var r interface{}
			if r = recover(); r == nil {
				return
			}
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Error("Transaction sink worker panics", zap.Reflect("r", r), zap.Stack("stacktrace"))
			w.errCh <- cerror.ErrMySQLWorkerPanic.GenWithStack("Transaction sink worker panics, stack: %v", string(buf))
		}()
		log.Info("Transaction sink worker starts",
			zap.String("changefeedID", w.changefeed),
			zap.Int("workerID", w.ID))

		var flushTimeSlice, totalTimeSlice time.Duration
		overseerTimer := time.NewTicker(time.Second)
		startToWork := time.Now()
		defer overseerTimer.Stop()
	LOOP:
		for {
			if !w.hasPending {
				// There is no pending events, so use a blocking `select`.
				select {
				case <-w.ctx.Done():
					log.Info("Transaction sink worker exits as canceled",
						zap.String("changefeedID", w.changefeed),
						zap.Int("workerID", w.ID))
					return
				case <-w.stopped:
					log.Info("Transaction sink worker exits as closed",
						zap.String("changefeedID", w.changefeed),
						zap.Int("workerID", w.ID))
					return
				case txn := <-w.txnCh.Out():
					w.hasPending = true
					if w.onEvent(txn) && w.doFlush(&flushTimeSlice) {
						break LOOP
					}
				case now := <-overseerTimer.C:
					totalTimeSlice = now.Sub(startToWork)
					busyRatio := int(flushTimeSlice.Seconds() / totalTimeSlice.Seconds() * 1000)
					w.metricTxnWorkerBusyRatio.Add(float64(busyRatio) / float64(w.workerCount))
					startToWork = now
					flushTimeSlice = 0
				}
			} else {
				// Have fetched some events so that do a nonblocking `select`.
				select {
				case txn := <-w.txnCh.Out():
					w.hasPending = true
					if w.onEvent(txn) && w.doFlush(&flushTimeSlice) {
						break LOOP
					}
				default:
					if w.doFlush(&flushTimeSlice) {
						break LOOP
					}
				}
			}
		}
		log.Warn("Transaction sink worker exits unexceptedly",
			zap.String("changefeedID", w.changefeed),
			zap.Int("workerID", w.ID))
	}()
}

func (w *worker) onEvent(txn txnWithNotifier) bool {
	if txn.txnEvent.GetTableSinkState() == state.TableSinkStopping {
		// The table where the event comes from is in stopping so it's safe
		// to drop the event directly.
		txn.txnEvent.Callback()
		// Still necessary to append the wantMore callback into the pending list.
		w.wantMoreCallbacks = append(w.wantMoreCallbacks, txn.wantMore)
		return false
	}

	w.metricConflictDetectDuration.Observe(time.Since(txn.start).Seconds())
	w.metricTxnWorkerHandledRows.Add(float64(len(txn.Event.Rows)))
	w.wantMoreCallbacks = append(w.wantMoreCallbacks, txn.wantMore)
	if w.backend.OnTxnEvent(txn.txnEvent.TxnCallbackableEvent) {
		return true
	}
	return false
}

// doFlush flushes the backend. Returns true if the goroutine can exit.
func (w *worker) doFlush(flushTimeSlice *time.Duration) bool {
	if w.hasPending {
		start := time.Now()
		defer func() {
			elapsed := time.Since(start)
			*flushTimeSlice += elapsed
			w.metricTxnWorkerFlushDuration.Observe(elapsed.Seconds())
		}()

		if err := w.backend.Flush(w.ctx); err != nil {
			log.Warn("Transaction sink backend flush fail",
				zap.String("changefeedID", w.changefeed),
				zap.Int("workerID", w.ID),
				zap.Error(err))
			select {
			case <-w.ctx.Done():
			case w.errCh <- err:
			}
			return true
		}
		// Flush successfully, call callbacks to notify conflict detector.
		for _, wantMore := range w.wantMoreCallbacks {
			wantMore()
		}
		w.wantMoreCallbacks = w.wantMoreCallbacks[:0]
		if cap(w.wantMoreCallbacks) > 1024 {
			// Resize the buffer if it's too big.
			w.wantMoreCallbacks = make([]func(), 0, 1024)
		}
	}

	w.hasPending = false
	return false
}
