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

	"go.uber.org/atomic"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality/internal"
)

type ConflictDetector[Worker worker[Txn], Txn txnEvent] struct {
	workers []Worker
	slots   *internal.Slots[*internal.Node[Txn]]

	finishedTxnQueue *containers.SliceQueue[finishedTxn[Txn]]
	resolvedTxnQueue *containers.SliceQueue[resolvedTxn[Txn]]

	nextWorkerID atomic.Int64

	wg      sync.WaitGroup
	closeCh chan struct{}
}

type finishedTxn[Txn txnEvent] struct {
	txn  Txn
	node *internal.Node[Txn]
}

type resolvedTxn[Txn txnEvent] struct {
	txn      Txn
	node     *internal.Node[Txn]
	workerID int64
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector[Worker worker[Txn], Txn txnEvent](
	workers []Worker,
	numSlots int64,
) *ConflictDetector[Worker, Txn] {
	ret := &ConflictDetector[Worker, Txn]{
		workers:          workers,
		slots:            internal.NewSlots[*internal.Node[Txn]](numSlots),
		finishedTxnQueue: containers.NewSliceQueue[finishedTxn[Txn]](),
		resolvedTxnQueue: containers.NewSliceQueue[resolvedTxn[Txn]](),
		closeCh:          make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.runResolvedHandler()
	}()

	return ret
}

// Add pushes a transaction to the ConflictDetector.
func (d *ConflictDetector[Worker, Txn]) Add(txn Txn) error {
	node := internal.NewNode(txn)
	d.slots.Add(node, txn.ConflictKeys(), func(other *internal.Node[Txn]) {
		node.DependOn(other)
	})
	node.OnNoConflict(func(workerID int64) {
		d.resolvedTxnQueue.Push(resolvedTxn[Txn]{
			txn:      txn,
			node:     node,
			workerID: workerID,
		})
	})
	return nil
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Worker, Txn]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Worker, Txn]) runResolvedHandler() {
selectLoop:
	for {
		select {
		case <-d.closeCh:
			return
		case <-d.resolvedTxnQueue.C:
			for {
				resolvedTxn, ok := d.resolvedTxnQueue.Pop()
				if !ok {
					continue selectLoop
				}

				d.sendToWorker(&OutTxnEvent[Txn]{
					Txn: resolvedTxn.txn,
					Callback: func(errIn error) {
						d.finishedTxnQueue.Push(finishedTxn[Txn]{
							txn:  resolvedTxn.txn,
							node: resolvedTxn.node,
						})
						resolvedTxn.txn.Finish(errIn)
					},
				}, resolvedTxn.node, resolvedTxn.workerID)
			}
		case <-d.finishedTxnQueue.C:
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
}

// sendToWorker should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Worker, Txn]) sendToWorker(txn *OutTxnEvent[Txn], node *internal.Node[Txn], workerID int64) {
	if workerID == -1 {
		workerID = d.nextWorkerID.Add(1) % int64(len(d.workers))
	}

	node.AssignTo(workerID)
	worker := d.workers[workerID]
	worker.Add(txn)
}
