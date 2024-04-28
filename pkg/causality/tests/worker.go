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

package tests

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality"
)

type txnForTest struct {
	keys []uint64
	done func()
}

func (t *txnForTest) OnConflictResolved() {}

func (t *txnForTest) GenSortedDedupKeysHash(numSlots uint64) []uint64 {
	return t.keys
}

func (t *txnForTest) Finish(err error) {
	if t.done != nil {
		t.done()
	}
}

//nolint:unused
type txnWithUnlock struct {
	*txnForTest
}

type workerForTest struct {
	txnQueue *containers.SliceQueue[txnWithUnlock]
	wg       sync.WaitGroup
	closeCh  chan struct{}
	execFunc func(*txnForTest) error
}

func newWorkerForTest(txnCh <-chan causality.TxnWithNotifier[*txnForTest]) *workerForTest {
	ret := &workerForTest{
		txnQueue: containers.NewSliceQueue[txnWithUnlock](),
		closeCh:  make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.run(txnCh)
	}()

	return ret
}

func (w *workerForTest) Close() {
	close(w.closeCh)
	w.wg.Wait()
}

func (w *workerForTest) run(txnCh <-chan causality.TxnWithNotifier[*txnForTest]) {
	for {
		select {
		case <-w.closeCh:
			return
		case txn := <-txnCh:
			var err error
			if w.execFunc != nil {
				err = errors.Trace(w.execFunc(txn.TxnEvent))
			}
			txn.PostTxnExecuted()

			// Finish must be called after unlock,
			// because the conflictTestDriver needs to make sure
			// that all conflicts have been resolved before exiting.
			txn.TxnEvent.Finish(err)
		}
	}
}
