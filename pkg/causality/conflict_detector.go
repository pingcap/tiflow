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

package causality

import (
	"sync"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality/internal"
)

type ConflictDetector[Txn txnEvent, Worker worker[Txn]] struct {
	workers workerGroup[Txn, Worker]
	slots   *internal.Slots[*internal.Node[Txn]]

	finishedTxnQueue containers.SliceQueue[finishedTxn[Txn]]
	nextWorkerID     int64

	wg      sync.WaitGroup
	closeCh chan struct{}
}

type finishedTxn[Txn txnEvent] struct {
	txn  Txn
	node *internal.Node[Txn]
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector[Txn txnEvent, Worker worker[Txn]](
	workers workerGroup[Txn, Worker],
	numSlots int64,
) *ConflictDetector[Txn, Worker] {
	ret := &ConflictDetector[Txn, Worker]{
		workers: workers,
		slots:   internal.NewSlots[*internal.Node[Txn]](numSlots),
		closeCh: make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()

		ret.runFinishHandler()
	}()
	return ret
}

// Add pushes a transaction to the ConflictDetector.
func (d *ConflictDetector[Txn, Worker]) Add(txn Txn) error {
	node := internal.NewNode(txn)
	d.slots.Add(node, txn.ConflictKeys(), func(other *internal.Node[Txn]) {
		node.DependOn(other)
	})
	node.AsyncResolve(func(workerID int64) {
		err := d.sendToWorker(&OutTxnEvent[Txn]{
			Txn: txn,
			Callback: func(errIn error) {
				d.finishedTxnQueue.Push(finishedTxn[Txn]{
					txn:  txn,
					node: node,
				})
				txn.Finish(errIn)
			},
		}, node, workerID)

		if err != nil {
			txn.Finish(err)
		}
	})
	return nil
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Txn, Worker]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Txn, Worker]) runFinishHandler() {
selectLoop:
	for {
		select {
		case <-d.closeCh:
			return
		case <-d.finishedTxnQueue.C:
		}

		for {
			finishedTxn, ok := d.finishedTxnQueue.Pop()
			if !ok {
				continue selectLoop
			}

			d.slots.Remove(finishedTxn.node, finishedTxn.txn.ConflictKeys())
			finishedTxn.node.Remove()
			finishedTxn.node.Free()
		}
	}
}

// sendToWorker should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Txn, Worker]) sendToWorker(txn *OutTxnEvent[Txn], node *internal.Node[Txn], workerID int64) error {
	if workerID == -1 {
		workerID = d.nextWorkerID
		d.nextWorkerID++
	}

	node.AssignTo(workerID)
	worker := d.workers.GetWorkerByID(workerID)
	if err := worker.Add(txn); err != nil {
		return err
	}
	return nil
}
