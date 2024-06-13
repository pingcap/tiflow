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

	"github.com/pingcap/tiflow/pkg/causality/internal"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/atomic"
)

// ConflictDetector implements a logic that dispatches transaction
// to different workers in a way that transactions modifying the same
// keys are never executed concurrently and have their original orders
// preserved.
type ConflictDetector[Worker worker[Txn], Txn txnEvent] struct {
	workers []Worker

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots    *internal.Slots[*internal.Node]
	numSlots uint64

	// nextWorkerID is used to dispatch transactions round-robin.
	nextWorkerID atomic.Int64

	// Used to run a background goroutine to GC or notify nodes.
	notifiedNodes *chann.DrainableChann[func()]
	garbageNodes  *chann.DrainableChann[txnFinishedEvent]
	wg            sync.WaitGroup
	closeCh       chan struct{}
}

type txnFinishedEvent struct {
	node           *internal.Node
	sortedKeysHash []uint64
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector[Worker worker[Txn], Txn txnEvent](
	workers []Worker,
	numSlots uint64,
) *ConflictDetector[Worker, Txn] {
	ret := &ConflictDetector[Worker, Txn]{
		workers:       workers,
		slots:         internal.NewSlots[*internal.Node](numSlots),
		numSlots:      numSlots,
		notifiedNodes: chann.NewDrainableChann[func()](),
		garbageNodes:  chann.NewDrainableChann[txnFinishedEvent](),
		closeCh:       make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.runBackgroundTasks()
	}()

	return ret
}

// Add pushes a transaction to the ConflictDetector.
//
// NOTE: if multiple threads access this concurrently,
// Txn.GenSortedDedupKeysHash must be sorted by the slot index.
func (d *ConflictDetector[Worker, Txn]) Add(txn Txn) {
	sortedKeysHash := txn.GenSortedDedupKeysHash(d.numSlots)
	node := internal.NewNode()
	node.OnResolved = func(workerID int64) {
		// This callback is called after the transaction is executed.
		postTxnExecuted := func() {
			// After this transaction is executed, we can remove the node from the graph,
			// and resolve related dependencies for these transacitons which depend on this
			// executed transaction.
			node.Remove()

			// Send this node to garbageNodes to GC it from the slots if this node is still
			// occupied related slots.
			d.garbageNodes.In() <- txnFinishedEvent{node, sortedKeysHash}
		}
		// Send this txn to related worker as soon as all dependencies are resolved.
		d.sendToWorker(txn, postTxnExecuted, workerID)
	}
	node.RandWorkerID = func() int64 { return d.nextWorkerID.Add(1) % int64(len(d.workers)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes.In() <- callback }
	d.slots.Add(node, sortedKeysHash)
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Worker, Txn]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Worker, Txn]) runBackgroundTasks() {
	defer func() {
		d.notifiedNodes.CloseAndDrain()
		d.garbageNodes.CloseAndDrain()
	}()
	for {
		select {
		case <-d.closeCh:
			return
		case notifyCallback := <-d.notifiedNodes.Out():
			if notifyCallback != nil {
				notifyCallback()
			}
		case event := <-d.garbageNodes.Out():
			if event.node != nil {
				d.slots.Free(event.node, event.sortedKeysHash)
			}
		}
	}
}

// sendToWorker should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Worker, Txn]) sendToWorker(txn Txn, postTxnExecuted func(), workerID int64) {
	if workerID < 0 {
		panic("must assign with a valid workerID")
	}
	txn.OnConflictResolved()
	worker := d.workers[workerID]
	worker.Add(txn, postTxnExecuted)
}
