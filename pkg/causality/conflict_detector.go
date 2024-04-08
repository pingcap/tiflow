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
	"github.com/pingcap/tiflow/pkg/causality/internal"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// ConflictDetector implements a logic that dispatches transaction
// to different workers in a way that transactions modifying the same
// keys are never executed concurrently and have their original orders
// preserved.
type ConflictDetector[Txn txnEvent] struct {
	workers []worker[Txn]

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
func NewConflictDetector[Txn txnEvent](
	numSlots uint64, opt WorkerOption,
) *ConflictDetector[Txn] {
	ret := &ConflictDetector[Txn]{
		workers:       make([]worker[Txn], opt.WorkerCount),
		slots:         internal.NewSlots[*internal.Node](numSlots),
		numSlots:      numSlots,
		notifiedNodes: chann.NewDrainableChann[func()](),
		garbageNodes:  chann.NewDrainableChann[txnFinishedEvent](),
		closeCh:       make(chan struct{}),
	}

	for i := 0; i < opt.WorkerCount; i++ {
		ret.workers[i] = newWorker[Txn](opt)
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
func (d *ConflictDetector[Txn]) Add(txn Txn) {
	sortedKeysHash := txn.GenSortedDedupKeysHash(d.numSlots)
	node := internal.NewNode()
	node.TrySendToWorker = func(workerID int64) bool {
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
		return d.sendToWorker(txn, postTxnExecuted, workerID)
	}
	node.RandWorkerID = func() int64 { return d.nextWorkerID.Add(1) % int64(len(d.workers)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes.In() <- callback }
	node.WorkerCount = int64(len(d.workers))
	d.slots.Add(node, sortedKeysHash)
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Txn]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Txn]) runBackgroundTasks() {
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
func (d *ConflictDetector[Txn]) sendToWorker(txn Txn, postTxnExecuted func(), workerID int64) bool {
	if workerID < 0 {
		log.Panic("must assign with a valid workerID", zap.Int64("workerID", workerID))
	}
	txn.OnConflictResolved()
	worker := d.workers[workerID]
	return worker.Add(TxnWithNotifier[Txn]{txn, postTxnExecuted})
}

// GetWorkerOutputChann returns the output channel of the worker.
func (d *ConflictDetector[Txn]) GetWorkerOutputChann(workerID int64) <-chan TxnWithNotifier[Txn] {
	if workerID < 0 {
		log.Panic("must assign with a valid workerID", zap.Int64("workerID", workerID))
	}
	return d.workers[workerID].Out()
}
