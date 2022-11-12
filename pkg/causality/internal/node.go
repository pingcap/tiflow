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
	"sync"
	stdatomic "sync/atomic"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	workerID = int64
)

const (
	unassigned    = workerID(-2)
	assignedToAny = workerID(-1)
	invalidNodeID = int64(-1)
)

var (
	nextNodeID = atomic.NewInt64(0)

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
	// Immutable fields.
	id int64

	// Set the callback that the node is resolved.
	OnResolved func(id workerID)
	// Set the id generator to get a random ID.
	RandWorkerID func() workerID
	// Set the callback that the node is notified.
	OnNotified func(callback func())

	// Following fields are used for notifying a node's dependers lock-free.
	totalDependees    int32
	resolvedDependees int32
	removedDependees  int32
	resolvedList      []int64

	// Following fields are protected by `mu`.
	mu sync.Mutex

	assignedTo workerID
	removed    bool

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
}

// NewNode creates a new node.
func NewNode() (ret *Node) {
	defer func() {
		ret.id = genNextNodeID()
		ret.OnResolved = nil
		ret.RandWorkerID = nil
		ret.totalDependees = 0
		ret.resolvedDependees = 0
		ret.removedDependees = 0
		ret.resolvedList = nil
		ret.assignedTo = unassigned
		ret.removed = false
	}()

	ret = new(Node)
	return
}

// NodeID implements interface internal.SlotNode.
func (n *Node) NodeID() int64 {
	return n.id
}

// DependOn implements interface internal.SlotNode.
func (n *Node) DependOn(unresolvedDeps map[int64]*Node, resolvedDeps int) {
	resolvedDependees, removedDependees := int32(0), int32(0)

	depend := func(target *Node) {
		if target == nil {
			// For a given Node, every dependency corresponds to a target.
			// If target is nil it means the dependency doesn't conflict
			// with any other nodes. However it's still necessary to track
			// it because Node.tryResolve needs to know it.
			resolvedDependees = stdatomic.AddInt32(&n.resolvedDependees, 1)
			stdatomic.StoreInt64(&n.resolvedList[resolvedDependees-1], assignedToAny)
			removedDependees = stdatomic.AddInt32(&n.removedDependees, 1)
			return
		}

		if target.id == n.id {
			panic("you cannot depend on yourself")
		}
		// Lock target and insert `n` into target.dependers.
		target.mu.Lock()
		defer target.mu.Unlock()

		if target.assignedTo != unassigned {
			// The target has already been assigned to a worker.
			resolvedDependees = stdatomic.AddInt32(&n.resolvedDependees, 1)
			stdatomic.StoreInt64(&n.resolvedList[resolvedDependees-1], target.assignedTo)
		}
		if target.removed {
			// The target has already been removed.
			removedDependees = stdatomic.AddInt32(&n.removedDependees, 1)
		} else if _, exist := target.getOrCreateDependers().ReplaceOrInsert(n); exist {
			// Should never depend on a target redundantly.
			panic("should never exist")
		}
	}

	// Re-allocate ID in `DependOn` instead of creating the node, because the node can be
	// pending in slots after it's created.
	n.id = genNextNodeID()

	// `totalDependees` and `resolvedList` must be initialized before depending on any targets.
	n.totalDependees = int32(len(unresolvedDeps) + resolvedDeps)
	n.resolvedList = make([]int64, 0, n.totalDependees)
	for i := 0; i < int(n.totalDependees); i++ {
		n.resolvedList = append(n.resolvedList, unassigned)
	}

	for _, target := range unresolvedDeps {
		depend(target)
	}
	for i := 0; i < resolvedDeps; i++ {
		depend(nil)
	}

	n.maybeResolve(resolvedDependees, removedDependees)
}

// Remove implements interface internal.SlotNode.
func (n *Node) Remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.removed = true
	if n.dependers != nil {
		// `mu` must be holded during accessing dependers.
		n.dependers.Ascend(func(node *Node) bool {
			removedDependees := stdatomic.AddInt32(&node.removedDependees, 1)
			node.maybeResolve(0, removedDependees)
			return true
		})
		n.dependers.Clear(true)
		n.dependers = nil
	}
}

// Free implements interface internal.SlotNode.
// It must be called if a node is no longer used.
// We are using sync.Pool to lessen the burden of GC.
func (n *Node) Free() {
	if n.id == invalidNodeID {
		panic("double free")
	}

	n.id = invalidNodeID
	n.OnResolved = nil
	n.RandWorkerID = nil

	// TODO: reuse node if necessary. Currently it's impossible if async-notify is used.
	// The reason is a node can step functions `assignTo`, `Remove`, `Free`, then `assignTo`.
	// again. In the last `assignTo`, it can never know whether the node has been reused
	// or not.
}

// assignTo assigns a node to a worker. Returns `true` on success.
func (n *Node) assignTo(workerID int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.assignedTo != unassigned {
		// Already resolved by some other guys.
		return false
	}

	n.assignedTo = workerID
	if n.OnResolved != nil {
		n.OnResolved(workerID)
		n.OnResolved = nil
	}

	if n.dependers != nil {
		// `mu` must be holded during accessing dependers.
		n.dependers.Ascend(func(node *Node) bool {
			resolvedDependees := stdatomic.AddInt32(&node.resolvedDependees, 1)
			stdatomic.StoreInt64(&node.resolvedList[resolvedDependees-1], n.assignedTo)
			node.maybeResolve(resolvedDependees, 0)
			return true
		})
	}

	return true
}

func (n *Node) maybeResolve(resolvedDependees, removedDependees int32) {
	if workerNum, ok := n.tryResolve(resolvedDependees, removedDependees); ok {
		if workerNum < 0 {
			panic("Node.tryResolve must return a valid worker ID")
		}
		if n.OnNotified != nil {
			n.OnNotified(func() { n.assignTo(workerNum) })
		} else {
			n.assignTo(workerNum)
		}
	}
}

// tryResolve try to find a worker to assign the node to.
// Returns (_, false) if there is a conflict,
// returns (rand, true) if there is no conflict,
// returns (N, true) if only worker N can be used.
func (n *Node) tryResolve(resolvedDependees, removedDependees int32) (int64, bool) {
	assignedTo, resolved := n.doResolve(resolvedDependees, removedDependees)
	if resolved && assignedTo == assignedToAny {
		assignedTo = n.RandWorkerID()
	}
	return assignedTo, resolved
}

func (n *Node) doResolve(resolvedDependees, removedDependees int32) (int64, bool) {
	if n.totalDependees == 0 {
		// No conflicts, can select any workers.
		return assignedToAny, true
	}

	if resolvedDependees == n.totalDependees {
		firstDep := stdatomic.LoadInt64(&n.resolvedList[0])
		hasDiffDep := false
		for i := 1; i < int(n.totalDependees); i++ {
			curr := stdatomic.LoadInt64(&n.resolvedList[i])
			if firstDep != curr {
				hasDiffDep = true
				break
			}
		}
		if !hasDiffDep {
			// If all dependees are assigned to one same worker, we can assign
			// this node to the same worker directly.
			return firstDep, true
		}
	}

	// All dependees are removed, so assign the node to any worker is fine.
	if removedDependees == n.totalDependees {
		return assignedToAny, true
	}

	return unassigned, false
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

func genNextNodeID() int64 {
	return nextNodeID.Add(1)
}
