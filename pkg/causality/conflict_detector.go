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
// to different worker cache channels in a way that transactions
// modifying the same keys are never executed concurrently and
// have their original orders preserved. Transactions in different
// channels can be executed concurrently.
type ConflictDetector[Txn txnEvent] struct {
	// resolvedTxnCaches are used to cache resolved transactions.
	resolvedTxnCaches []txnCache[Txn]

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots    *internal.Slots[*internal.Node]
	numSlots uint64

	// nextWorkerID is used to dispatch transactions round-robin.
	nextWorkerID atomic.Int64

	// Used to run a background goroutine to GC or notify nodes.
	notifiedNodes *chann.DrainableChann[func()]
	garbageNodes  *chann.DrainableChann[*internal.Node]
	wg            sync.WaitGroup
	closeCh       chan struct{}
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector[Txn txnEvent](
	numSlots uint64, opt TxnCacheOption,
) *ConflictDetector[Txn] {
	ret := &ConflictDetector[Txn]{
		resolvedTxnCaches: make([]txnCache[Txn], opt.Count),
		slots:             internal.NewSlots[*internal.Node](numSlots),
		numSlots:          numSlots,
		notifiedNodes:     chann.NewAutoDrainChann[func()](),
		garbageNodes:      chann.NewAutoDrainChann[*internal.Node](),
		closeCh:           make(chan struct{}),
	}
	for i := 0; i < opt.Count; i++ {
		ret.resolvedTxnCaches[i] = newTxnCache[Txn](opt)
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
	node := internal.NewNode(sortedKeysHash)
	txnWithNotifier := TxnWithNotifier[Txn]{
		TxnEvent: txn,
		PostTxnExecuted: func() {
			// After this transaction is executed, we can remove the node from the graph,
			// and resolve related dependencies for these transacitons which depend on this
			// executed transaction.
			node.Remove()

			// Send this node to garbageNodes to GC it from the slots if this node is still
			// occupied related slots.
			d.garbageNodes.In() <- node
		},
	}
	node.TrySendToTxnCache = func(cacheID int64) bool {
		// Try sending this txn to related cache as soon as all dependencies are resolved.
		return d.sendToCache(txnWithNotifier, cacheID)
	}
	node.RandCacheID = func() int64 { return d.nextWorkerID.Add(1) % int64(len(d.resolvedTxnCaches)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes.In() <- callback }
	d.slots.Add(node)
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
		case node := <-d.garbageNodes.Out():
			if node != nil {
				d.slots.Free(node)
			}
		}
	}
}

// sendToCache should not call txn.Callback if it returns an error.
func (d *ConflictDetector[Txn]) sendToCache(txn TxnWithNotifier[Txn], id int64) bool {
	if id < 0 {
		log.Panic("must assign with a valid cacheID", zap.Int64("cacheID", id))
	}

	// Note OnConflictResolved must be called before add to cache. Otherwise, there will
	// be a data race since the txn may be read before the OnConflictResolved is called.
	txn.TxnEvent.OnConflictResolved()
	cache := d.resolvedTxnCaches[id]
	ok := cache.add(txn)
	return ok
}

// GetOutChByCacheID returns the output channel by cacheID.
// Note txns in single cache should be executed sequentially.
func (d *ConflictDetector[Txn]) GetOutChByCacheID(id int64) <-chan TxnWithNotifier[Txn] {
	if id < 0 {
		log.Panic("must assign with a valid cacheID", zap.Int64("cacheID", id))
	}
	return d.resolvedTxnCaches[id].out()
}
