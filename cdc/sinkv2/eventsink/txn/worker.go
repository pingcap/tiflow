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

	// Fields only used in the background loop.
	timer         *time.Timer
	flushInterval time.Duration
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
		wantMoreCallbacks := make([]func(), 0, 1024)
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
				metrics.ConflictDetectDuration.Observe(time.Since(txn.start).Seconds())
				wantMoreCallbacks = append(wantMoreCallbacks, txn.wantMore)
				if w.backend.OnTxnEvent(txn.txnEvent.TxnCallbackableEvent) {
					if w.doFlush() {
						break LOOP
					}
				} else {
					continue LOOP
				}
			case <-w.timer.C:
				if w.doFlush() {
					break LOOP
				}
			}
			// Flush successfully, call callbacks to notify conflict detector.
			for _, wantMore := range wantMoreCallbacks {
				wantMore()
			}
			wantMoreCallbacks = wantMoreCallbacks[:0]
			if cap(wantMoreCallbacks) > 1024 {
				wantMoreCallbacks = make([]func(), 0, 1024)
			}
		}
		log.Warn("Transaction sink worker exits unexceptedly",
			zap.String("changefeedID", w.changefeed),
			zap.Int("workerID", w.ID))
	}()
}

// doFlush flushes the backend. Returns true if the goroutine can exit.
func (w *worker) doFlush() bool {
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
	if !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
	w.timer.Reset(w.flushInterval)
	return false
}
