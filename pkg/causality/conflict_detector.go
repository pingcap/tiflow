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
	"time"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/causality/internal"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

// Metrics contains some metrics of the associated ConflictDetector.
type Metrics struct {
	// Busy ratio of the associated ConflictDetector's background goroutine.
	// BackgroundWorkerBusyRatio.Add(100) means in the last 1 second, 100ms
	// is used for handling tasks.
	BackgroundWorkerBusyRatio prometheus.Counter
}

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
	notifiedNodes *containers.SliceQueue[func()]
	garbageNodes  *containers.SliceQueue[txnFinishedEvent]
	wg            sync.WaitGroup
	closeCh       chan struct{}

	metrics *Metrics
}

type txnFinishedEvent struct {
	node         *internal.Node
	conflictKeys []uint64
}

// NewConflictDetector creates a new ConflictDetector with a changefeedID in ctx.
func NewConflictDetector[Worker worker[Txn], Txn txnEvent](
	workers []Worker,
	numSlots uint64,
) *ConflictDetector[Worker, Txn] {
	return NewConflictDetectorWithMetrics[Worker, Txn](workers, numSlots, nil)
}

// NewConflictDetectorWithMetrics is like NewConflictDetector but is associated with
// a given Metrics.
func NewConflictDetectorWithMetrics[Worker worker[Txn], Txn txnEvent](
	workers []Worker,
	numSlots uint64,
	metrics *Metrics,
) *ConflictDetector[Worker, Txn] {
	ret := &ConflictDetector[Worker, Txn]{
		workers:       workers,
		slots:         internal.NewSlots[*internal.Node](numSlots),
		numSlots:      numSlots,
		notifiedNodes: containers.NewSliceQueue[func()](),
		garbageNodes:  containers.NewSliceQueue[txnFinishedEvent](),
		closeCh:       make(chan struct{}),
		metrics:       metrics,
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
	conflictKeys := txn.ConflictKeys(d.numSlots)
	node := internal.NewNode()
	node.OnResolved = func(workerID int64) {
		unlock := func() {
			node.Remove()
			d.garbageNodes.Push(txnFinishedEvent{node, conflictKeys})
		}
		d.sendToWorker(txn, unlock, workerID)
	}
	node.RandWorkerID = func() int64 { return d.nextWorkerID.Add(1) % int64(len(d.workers)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes.Push(callback) }
	d.slots.Add(node, conflictKeys)
}

// Close closes the ConflictDetector.
func (d *ConflictDetector[Worker, Txn]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ConflictDetector[Worker, Txn]) runBackgroundTasks() {
	var notifiedNodesDuration, garbageNodesDuration time.Duration
	overseerTimer := time.NewTicker(time.Second)
	defer overseerTimer.Stop()
	startToWork := time.Now()
	for {
		select {
		case <-d.closeCh:
			return
		case <-d.notifiedNodes.C:
			start := time.Now()
			for {
				notifiyCallback, ok := d.notifiedNodes.Pop()
				if !ok {
					break
				}
				notifiyCallback()
			}
			notifiedNodesDuration += time.Since(start)
		case <-d.garbageNodes.C:
			start := time.Now()
			for {
				event, ok := d.garbageNodes.Pop()
				if !ok {
					break
				}
				d.slots.Free(event.node, event.conflictKeys)
			}
			garbageNodesDuration += time.Since(start)
		case now := <-overseerTimer.C:
			if d.metrics != nil {
				totalTimeSlice := now.Sub(startToWork)
				totalHandleSlice := notifiedNodesDuration + garbageNodesDuration
				busyRatio := totalHandleSlice.Seconds() / totalTimeSlice.Seconds() * 1000
				d.metrics.BackgroundWorkerBusyRatio.Add(busyRatio)
				startToWork = now
				notifiedNodesDuration = 0
				garbageNodesDuration = 0
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
