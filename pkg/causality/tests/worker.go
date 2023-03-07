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
)

type txnForTest struct {
	keys []uint64
	done func()
}

func (t *txnForTest) OnConflictResolved() {}

func (t *txnForTest) ConflictKeys(numSlots uint64) []uint64 {
	return t.keys
}

func (t *txnForTest) Finish(err error) {
	if t.done != nil {
		t.done()
	}
}

type txnWithUnlock struct {
	*txnForTest
	unlock func()
}

type workerForTest struct {
	txnQueue *containers.SliceQueue[txnWithUnlock]
	wg       sync.WaitGroup
	closeCh  chan struct{}
	execFunc func(*txnForTest) error
}

func newWorkerForTest() *workerForTest {
	ret := &workerForTest{
		txnQueue: containers.NewSliceQueue[txnWithUnlock](),
		closeCh:  make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.run()
	}()

	return ret
}

func (w *workerForTest) Add(txn *txnForTest, unlock func()) {
	w.txnQueue.Push(txnWithUnlock{txnForTest: txn, unlock: unlock})
}

func (w *workerForTest) Close() {
	close(w.closeCh)
	w.wg.Wait()
}

func (w *workerForTest) run() {
outer:
	for {
		select {
		case <-w.closeCh:
			return
		case <-w.txnQueue.C:
		}

		for {
			txn, ok := w.txnQueue.Pop()
			if !ok {
				continue outer
			}

			var err error
			if w.execFunc != nil {
				err = errors.Trace(w.execFunc(txn.txnForTest))
			}
			txn.unlock()

			// Finish must be called after unlock,
			// because the conflictTestDriver needs to make sure
			// that all conflicts have been resolved before exiting.
			txn.Finish(err)
		}
	}
}
