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
	ctx        context.Context
	changefeed string

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

	// Fields only used in the background loop.
	flushInterval     time.Duration
	timer             *time.Timer
	wantMoreCallbacks []func()
}

func newWorker(ctx context.Context, ID int, backend backend, errCh chan<- error) *worker {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	return &worker{
		ctx:        ctx,
		changefeed: fmt.Sprintf("%s.%s", changefeedID.Namespace, changefeedID.ID),

		ID:      ID,
		txnCh:   chann.New[txnWithNotifier](chann.Cap(-1 /*unbounded*/)),
		stopped: make(chan struct{}),
		backend: backend,
		errCh:   errCh,

		metricConflictDetectDuration: metrics.ConflictDetectDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerFlushDuration: metrics.TxnWorkerFlushDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTxnWorkerBusyRatio:     metrics.TxnWorkerBusyRatio.WithLabelValues(changefeedID.Namespace, changefeedID.ID),

		flushInterval: backend.MaxFlushInterval(),
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

		w.timer = time.NewTimer(w.flushInterval)
		w.wantMoreCallbacks = make([]func(), 0, 1024)

		var flushTimeSlice, totalTimeSlice time.Duration
		overseerTimer := time.NewTicker(2 * time.Second)
		startToWork := time.Now()
	LOOP:
		for {
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
				if w.onEvent(txn) && w.doFlush(&flushTimeSlice) {
					break LOOP
				}
			case <-w.timer.C:
				if w.doFlush(&flushTimeSlice) {
					break LOOP
				}
			case now := <-overseerTimer.C:
				totalTimeSlice = now.Sub(startToWork)
				busyRatio := int(flushTimeSlice.Seconds() / totalTimeSlice.Seconds() * 1000)
				w.metricTxnWorkerBusyRatio.Add(float64(busyRatio))
				startToWork = now
				flushTimeSlice = 0
			}
		}
		log.Warn("Transaction sink worker exits unexceptedly",
			zap.String("changefeedID", w.changefeed),
			zap.Int("workerID", w.ID))
	}()
}

func (w *worker) onEvent(txn txnWithNotifier) bool {
	w.metricConflictDetectDuration.Observe(time.Since(txn.start).Seconds())
	w.wantMoreCallbacks = append(w.wantMoreCallbacks, txn.wantMore)
	if w.backend.OnTxnEvent(txn.txnEvent.TxnCallbackableEvent) {
		if !w.timer.Stop() {
			<-w.timer.C
		}
		return true
	}
	return false
}

// doFlush flushes the backend. Returns true if the goroutine can exit.
func (w *worker) doFlush(flushTimeSlice *time.Duration) bool {
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
		w.wantMoreCallbacks = make([]func(), 0, 1024)
	}

	w.timer.Reset(w.flushInterval)
	return false
}
