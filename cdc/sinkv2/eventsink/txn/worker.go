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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

type txnWithNotifier struct {
	*txnEvent
	wantMore func()
}

type worker struct {
	ID      int
	txnCh   *chann.Chann[txnWithNotifier]
	stopped chan struct{}
	wg      sync.WaitGroup
	backend backend
	errCh   chan<- error

	// Fields only used in the background loop.
	timer *time.Timer
}

func newWorker(ID int, backend backend, errCh chan<- error) *worker {
	return &worker{
		ID:      ID,
		txnCh:   chann.New[txnWithNotifier](chann.Cap(-1 /*unbounded*/)),
		stopped: make(chan struct{}),
		backend: backend,
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
				log.Info("transaction sink backend close fail",
					zap.Error(err))
			}
		}()
		w.timer = time.NewTimer(w.backend.MaxFlushInterval())
		for {
			select {
			case <-w.stopped:
				log.Info("Transaction sink backend worker exits expectedly",
					zap.Int("workerID", w.ID))
				return
			case txn := <-w.txnCh.Out():
				metrics.ConflictDetectDuration.Observe(time.Since(txn.start).Seconds())
				txn.wantMore()
				if w.backend.OnTxnEvent(txn.txnEvent.TxnCallbackableEvent) && w.doFlush() {
					log.Warn("Transaction sink backend exits unexceptedly")
					return
				}
			case <-w.timer.C:
				if w.doFlush() {
					log.Warn("Transaction sink backend exits unexceptedly")
					return
				}
			}
		}
	}()
}

// doFlush flushes the backend. Returns true if the goroutine can exit.
func (w *worker) doFlush() bool {
	// TODO: support to cancel the worker when performing some blocking operations.
	ctx := context.Background()
	if err := w.backend.Flush(ctx); err != nil {
		log.Warn("txn sink worker flush fail", zap.Error(err))
		select {
		case w.errCh <- err:
		case <-ctx.Done():
		}
		return true
	}
	if !w.timer.Stop() {
		<-w.timer.C
	}
	w.timer.Reset(w.backend.MaxFlushInterval())
	return false
}
