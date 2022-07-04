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

package internal

import (
	"fmt"
	"sync"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	workerID = int64
)

const (
	unassigned    = workerID(-1)
	invalidNodeID = int64(-1)
)

var (
	nextNodeID = atomic.NewInt64(0)
	nodePool   = &sync.Pool{}

	// btreeFreeList is a shared free list used by all
	// btrees in order to lessen the burden of GC.
	//
	// Experiment shows increasing the capacity beyond 1024 yields little
	// performance improvement.
	btreeFreeList = btree.NewFreeListG[*Node](1024)
)

// Node is a node in the dependency graph used
// in conflict detection.
type Node struct {
	id int64 // immutable

	mu sync.Mutex
	// conflictCounts stores counts of nodes that the current node depend on,
	// grouped by the worker ID they are assigned to.
	conflictCounts map[workerID]int

	// dependers is an ordered set for all nodes that
	// conflict with the current node.
	//
	// Notes:
	// (1) An ordered data structure is preferred because
	//     if we can unblock conflicting transactions in the
	//     order that they have come in, the out-of-order-ness
	//     observed downstream will be less than what would have been
	//     if an unordered set were used.
	// (2) Google's btree package is selected because it seems to be
	//     the most popular production-grade ordered set implementation in Go.
	dependers *btree.BTreeG[*Node]

	assignedTo workerID
	onResolved func(id workerID)

	resolved atomic.Bool
}

// NewNode creates a new node.
func NewNode() (ret *Node) {
	defer func() {
		ret.id = nextNodeID.Add(1)
		ret.assignedTo = unassigned
	}()

	if obj := nodePool.Get(); obj != nil {
		return obj.(*Node)
	}
	return new(Node)
}

// Free must be called if a node is no longer used.
// We are using sync.Pool to lessen the burden of GC.
func (n *Node) Free() {
	if n.id == invalidNodeID {
		panic("double free")
	}

	n.id = invalidNodeID
	n.conflictCounts = nil
	n.assignedTo = unassigned
	n.onResolved = nil
	n.resolved.Store(false)

	nodePool.Put(n)
}

// DependOn marks n as dependent upon target.
func (n *Node) DependOn(target *Node) {
	// Lock target first because we are always
	// locking an earlier transaction first, so
	// that there will be not deadlocking.
	target.mu.Lock()
	defer target.mu.Unlock()

	if target.id == n.id {
		panic("you cannot depend on yourself")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Make sure that the conflictCounts have
	// been created.
	// Creating these maps is done lazily because we want to
	// optimize for the case where there are little conflicts.
	n.lazyCreateMap()

	if target.getOrCreateDependers().Has(n) {
		// Return here to ensure idempotency.
		return
	}

	target.dependers.ReplaceOrInsert(n)
	n.conflictCounts[target.assignedTo]++
}

// AssignTo assigns a node to a worker.
func (n *Node) AssignTo(workerID int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.assignedTo != unassigned {
		panic(fmt.Sprintf(
			"assigning an already assigned node: id %d, worker %d",
			n.id, n.assignedTo))
	}

	n.assignedTo = workerID

	if n.dependers != nil {
		n.dependers.Ascend(func(node *Node) bool {
			node.mu.Lock()
			defer node.mu.Unlock()

			node.conflictCounts[unassigned]--
			if node.conflictCounts[unassigned] == 0 {
				delete(node.conflictCounts, unassigned)
			}
			node.conflictCounts[workerID]++
			node.notifyMaybeResolved()

			return true
		})
	}
}

// Remove should be called after the transaction corresponding
// to the node is finished.
func (n *Node) Remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.conflictCounts) != 0 {
		panic("conflictNumber > 0")
	}

	if n.dependers != nil {
		n.dependers.Ascend(func(node *Node) bool {
			node.mu.Lock()
			defer node.mu.Unlock()

			node.conflictCounts[n.assignedTo]--
			if node.conflictCounts[n.assignedTo] == 0 {
				delete(node.conflictCounts, n.assignedTo)
			}
			node.notifyMaybeResolved()
			return true
		})
		n.dependers.Clear(true)
	}
}

// Equals tells whether two pointers to nodes are equal.
func (n *Node) Equals(other *Node) bool {
	return n.id == other.id
}

// OnNoConflict guarantees that fn is called
// when the node has either no unfinished dependencies
// or all unfinished dependencies that the node has are
// assigned to the same worker.
func (n *Node) OnNoConflict(fn func(id workerID)) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.onResolved != nil || n.resolved.Load() {
		panic("OnNoConflict is already called")
	}

	workerNum, ok := n.tryResolve()
	if ok {
		n.resolved.Store(true)
		fn(workerNum)
		return
	}

	n.onResolved = fn
}

// notifyMaybeResolved must be called with n.mu taken.
// It should be called if n.conflictCounts is updated.
func (n *Node) notifyMaybeResolved() {
	workerNum, ok := n.tryResolve()
	if !ok {
		return
	}

	if n.onResolved != nil {
		if !n.resolved.Swap(true) {
			n.onResolved(workerNum)
		}
	}
}

// tryResolve must be called with n.mu locked.
// Returns (_, false) if there is a conflict,
// returns (-1, true) if there is no conflict,
// returns (N, true) if only worker N can be used.
func (n *Node) tryResolve() (int64, bool) {
	conflictNumber := len(n.conflictCounts)
	if conflictNumber == 0 {
		// No conflict at all
		return -1, true
	}
	if conflictNumber == 1 {
		_, ok := n.conflictCounts[unassigned]
		if ok {
			// All conflicts are unassigned. So
			// no resolution is available.
			return 0, false
		}

		// Use for loop to retrieve the only key.
		for workerNum := range n.conflictCounts {
			// Only conflicting with one worker, i.e., workerNum.
			return workerNum, true
		}
	}
	// Conflicting with at least one worker and unassigned nodes,
	// or conflicting with at least two workers. In both cases,
	// no resolution is available.
	return 0, false
}

func (n *Node) lazyCreateMap() {
	if n.conflictCounts == nil {
		n.conflictCounts = make(map[workerID]int, 4)
	}
}

func (n *Node) getOrCreateDependers() *btree.BTreeG[*Node] {
	if n.dependers == nil {
		n.dependers = btree.NewWithFreeListG(8, func(a, b *Node) bool {
			return a.id < b.id
		}, btreeFreeList)
	}
	return n.dependers
}

// dependerCount returns the number of dependers the node has.
// NOTE: dependerCount is used for unit tests only.
func (n *Node) dependerCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dependers == nil {
		return 0
	}
	return n.dependers.Len()
}

// assignedWorkerID returns the worker ID that the node has been assigned to.
// NOTE: assignedWorkerID is used for unit tests only.
func (n *Node) assignedWorkerID() workerID {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.assignedTo
}
