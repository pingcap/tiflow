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

	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality/internal"
)

type ConflictDetector[Txn txnEvent, Worker worker[Txn]] struct {
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
func NewConflictDetector[Txn txnEvent, Worker worker[Txn]](
	workers []Worker,
	numSlots int64,
) *ConflictDetector[Txn, Worker] {
	ret := &ConflictDetector[Txn, Worker]{
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
func (d *ConflictDetector[Txn, Worker]) Add(txn Txn) error {
	node := internal.NewNode(txn)
	d.slots.Add(node, txn.ConflictKeys(), func(other *internal.Node[Txn]) {
		//log.Info("dependency detected",
		//	zap.Int64("node-id-a", node.ID()),
		//	zap.Int64("node-id-b", other.ID()))
		node.DependOn(other)
	})
	node.AsyncResolve(func(workerID int64) {
		d.resolvedTxnQueue.Push(resolvedTxn[Txn]{
			txn:      txn,
			node:     node,
			workerID: workerID,
		})
	})
	return nil
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Txn, Worker]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Txn, Worker]) runResolvedHandler() {
selectLoop:
	for {
		select {
		case <-d.closeCh:
			return
		case <-d.resolvedTxnQueue.C:
			log.Info("resolvedHandler run")
			for {
				resolvedTxn, ok := d.resolvedTxnQueue.Pop()
				if !ok {
					continue selectLoop
				}

				d.sendToWorker(&OutTxnEvent[Txn]{
					Txn: resolvedTxn.txn,
					Callback: func(errIn error) {
						log.Info("node finished", zap.Int64("node-id", resolvedTxn.node.ID()))
						d.finishedTxnQueue.Push(finishedTxn[Txn]{
							txn:  resolvedTxn.txn,
							node: resolvedTxn.node,
						})
						resolvedTxn.txn.Finish(errIn)
					},
				}, resolvedTxn.node, resolvedTxn.workerID)
			}
		case <-d.finishedTxnQueue.C:
			log.Info("finishHandler run")
			for {
				finishedTxn, ok := d.finishedTxnQueue.Pop()
				if !ok {
					continue selectLoop
				}

				log.Info("node removed", zap.Int64("node-id", finishedTxn.node.ID()))
				d.slots.Remove(finishedTxn.node, finishedTxn.txn.ConflictKeys())
				finishedTxn.node.Remove()
				finishedTxn.node.Free()
			}
		}
	}
}

// sendToWorker should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Txn, Worker]) sendToWorker(txn *OutTxnEvent[Txn], node *internal.Node[Txn], workerID int64) {
	if workerID == -1 {
		workerID = d.nextWorkerID.Add(1) % int64(len(d.workers))
	}

	node.AssignTo(workerID)
	worker := d.workers[workerID]
	log.Info("sendToWorker",
		zap.Int64("node-id", node.ID()),
		zap.Int64("worker-id", workerID))
	worker.Add(txn)
}
