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

// SingleConflictDispathType describes how to dispatch a single conflict event.
// If all dependences of an event are dispatched to one worker, we say the event
// has a single conflict.
type SingleConflictDispathType int

const (
	// SingleConflictDispatchSerialize means can only dispatch a single event when
	// all dependences are committed.
	SingleConflictDispatchSerialize SingleConflictDispathType = SingleConflictDispathType(iota)
	// SingleConflictDispatchPipeline means we can dispatch a single event directly.
	SingleConflictDispatchPipeline SingleConflictDispathType
)

type Config struct {
	OnSingleConflict SingleConflictDispathType
	NumSlots         uint64
}

// ConflictDetector implements a logic that dispatches transaction
// to different workers in a way that transactions modifying the same
// keys are never executed concurrently and have their original orders
// preserved.
type ConflictDetector[Worker worker[Txn], Txn txnEvent] struct {
	cfg     Config
	workers []Worker

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots *internal.Slots[*internal.Node]

	// nextWorkerID is used to dispatch transactions round-robin.
	nextWorkerID atomic.Int64

	// Used to run a background goroutine to GC or notify nodes.
	notifiedNodes *chann.DrainableChann[func()]
	garbageNodes  *chann.DrainableChann[txnFinishedEvent]
	wg            sync.WaitGroup
	closeCh       chan struct{}
}

type txnFinishedEvent struct {
	node         *internal.Node
	conflictKeys []uint64
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector[Worker worker[Txn], Txn txnEvent](
	workers []Worker,
	cfg Config,
) *ConflictDetector[Worker, Txn] {
	ret := &ConflictDetector[Worker, Txn]{
		cfg:           cfg,
		workers:       workers,
		slots:         internal.NewSlots[*internal.Node](cfg.NumSlots),
		notifiedNodes: chann.NewAutoDrainChann[func()](),
		garbageNodes:  chann.NewAutoDrainChann[txnFinishedEvent](),
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
// NOTE: if multiple threads access this concurrently, Txn.ConflictKeys must be sorted.
func (d *ConflictDetector[Worker, Txn]) Add(txn Txn) {
	conflictKeys := txn.ConflictKeys(d.cfg.NumSlots)
	node := internal.NewNode(d.cfg.OnSingleConflict == SingleConflictDispatchPipeline)
	node.OnResolved = func(workerID int64, allDependeesCommitted bool) {
		unlock := func() {
			node.Remove()
			d.garbageNodes.In() <- txnFinishedEvent{node, conflictKeys}
		}
		txn.OnConflictResolved(allDependeesCommitted)
		d.sendToWorker(txn, unlock, workerID)
	}
	node.RandWorkerID = func() int64 { return d.nextWorkerID.Add(1) % int64(len(d.workers)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes.In() <- callback }
	d.slots.Add(node, conflictKeys)
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
				d.slots.Free(event.node, event.conflictKeys)
			}
		}
	}
}

// sendToWorker should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Worker, Txn]) sendToWorker(txn Txn, unlock func(), workerID int64) {
	if workerID < 0 {
		panic("must assign with a valid workerID")
	}
	worker := d.workers[workerID]
	worker.Add(txn, unlock)
}
