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
	"time"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality"
)

type txnForTest struct {
	keys []int64
}

func (t *txnForTest) ConflictKeys() []int64 {
	return t.keys
}

func (t *txnForTest) Finish(err error) {
	// no-op
}

type workerForTest struct {
	txnQueue *containers.SliceQueue[*txnForTest]
	wg       sync.WaitGroup
	closeCh  chan struct{}
}

func newWorkerForTest() *workerForTest {
	ret := &workerForTest{
		txnQueue: containers.NewSliceQueue[*txnForTest](),
		closeCh:  make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.run()
	}()

	return ret
}

func (w *workerForTest) Add(txn *causality.OutTxnEvent[*txnForTest]) error {

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

			txn.Finish(nil)
			time.Sleep(10 * time.Millisecond)
		}
	}
}
