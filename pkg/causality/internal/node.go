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
	stdatomic "sync/atomic"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	workerID = int64
)

const (
	unassigned    = workerID(-2)
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

	// SendToWorker is used to send the node to a worker.
	SendToWorker func(id workerID)
	// RandWorkerID is used to select a worker randomly.
	RandWorkerID func() workerID

	// Following fields are used for notifying a node's dependers lock-free.
	totalDependencies   int32
	removedDependencies int32

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
		ret.SendToWorker = nil
		ret.RandWorkerID = nil
		ret.totalDependencies = 0
		ret.removedDependencies = 0
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
func (n *Node) DependOn(dependencyNodes map[int64]*Node, noDependencyKeyCnt int) {
	depend := func(target *Node) {
		if target.id == n.id {
			panic("cannot depend on yourself")
		}

		// The target node might be removed or modified in other places, for example
		// after its corresponding transaction has been executed.
		target.mu.Lock()
		defer target.mu.Unlock()

		// Add the node to the target's dependers if the target is not removed.
		if target.removed {
			// The target has already been removed.
			stdatomic.AddInt32(&n.removedDependencies, 1)
		} else if _, exist := target.getOrCreateDependers().ReplaceOrInsert(n); exist {
			// Should never depend on a target redundantly.
			panic("should never exist")
		}
	}

	// Re-allocate ID in `DependOn` instead of creating the node, because the node can be
	// pending in slots after it's created.
	// ?: why gen new ID here?
	n.id = genNextNodeID()

	// `totalDependcies` must be initialized before depending on any targets.
	n.totalDependencies = int32(len(dependencyNodes) + noDependencyKeyCnt)
	n.removedDependencies = int32(noDependencyKeyCnt)

	for _, node := range dependencyNodes {
		depend(node)
	}

	n.maybeReadyToRun()
}

// Remove implements interface internal.SlotNode.
// Remove will be called after related transaction is executed.
func (n *Node) Remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.removed = true
	if n.dependers != nil {
		// `mu` must be holded during accessing dependers.
		n.dependers.Ascend(func(node *Node) bool {
			stdatomic.AddInt32(&node.removedDependencies, 1)
			node.maybeReadyToRun()
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
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.id == invalidNodeID {
		panic("double free")
	}

	n.id = invalidNodeID
	n.SendToWorker = nil
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
		// Already handled by some other guys.
		return false
	}

	n.assignedTo = workerID
	if n.SendToWorker != nil {
		n.SendToWorker(workerID)
		n.SendToWorker = nil
	}

	return true
}

func (n *Node) maybeReadyToRun() {
	if ok := n.checkReadiness(); ok {
		// Assign the node to the worker directly.
		n.assignTo(n.RandWorkerID())
	}
}

// checkReadiness check if all dependencies have been removed.
// Returns false if there are some conflicts, returns true if all dependencies
// are removed.
func (n *Node) checkReadiness() bool {
	if n.totalDependencies == 0 {
		// No conflicts, can select any workers.
		return true
	}

	removedDependencies := stdatomic.LoadInt32(&n.removedDependencies)
	if removedDependencies > n.totalDependencies {
		panic(fmt.Sprintf("removedDependencies %d > totalDependencies %d which is not expected",
			removedDependencies, n.totalDependencies))
	}
	return removedDependencies == n.totalDependencies
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
