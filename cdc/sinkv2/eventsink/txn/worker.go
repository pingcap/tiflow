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
	"sync"
	"time"

	"github.com/pingcap/tiflow/pkg/chann"
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

	// Fields only used in the background loop.
	timer *time.Timer
}

func newWorker(ID int, backend backend) *worker {
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
func (w *worker) Run() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.timer = time.NewTimer(w.backend.maxFlushInterval())
		for {
			select {
			case <-w.stopped:
				return
			case txn, ok := <-w.txnCh.Out():
				if !ok {
					break
				}
				txn.wantMore()
				if w.backend.onTxnEvent(txn.txnEvent.TxnCallbackableEvent) && w.doFlush() {
					break
				}
			case <-w.timer.C:
				if w.doFlush() {
					break
				}
			}
		}
	}()
}

func (w *worker) doFlush() bool {
	if err := w.backend.flush(); err != nil {
		// TODO: handle err.
		return true
	}
	if !w.timer.Stop() {
		<-w.timer.C
	}
	w.timer.Reset(w.backend.maxFlushInterval())
	return false
}
