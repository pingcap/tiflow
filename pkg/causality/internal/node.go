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

	"github.com/tidwall/btree"
	"go.uber.org/atomic"
)

type (
	workerID = int64
	nodeID   = int64
)

const (
	unassigned = workerID(-1)
)

var (
	nextNodeID = atomic.NewInt64(0)
	nodePool   = &sync.Pool{}
)

// Node is a node in the dependency graph used
// in conflict detection.
type Node struct {
	id int64 // immutable

	mu             sync.Mutex
	conflictCounts map[workerID]int
	dependers      btree.Map[nodeID, *Node]
	assignedTo     workerID
	onResolved     func(id workerID)
	resolved       atomic.Bool
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

// ID returns the node's ID. For debug only.
func (n *Node) ID() int64 {
	return n.id
}

// Free must be called if a node is no longer used.
// We are using sync.Pool to lessen the burden of GC.
func (n *Node) Free() {
	if n.id == -1 {
		panic("double free")
	}

	n.id = -1
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

	// Make sure that the dependers and conflictCounts have
	// been created.
	// Creating these maps is done lazily because we want to
	// optimize for the case where there are little conflicts.
	target.lazyCreateMap()
	n.lazyCreateMap()

	if _, ok := target.dependers.Get(n.id); ok {
		return
	}
	target.dependers.Set(n.id, n)
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
	n.dependers.Scan(func(_ nodeID, node *Node) bool {
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

// Remove should be called after the transaction corresponding
// to the node is finished.
func (n *Node) Remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.conflictCounts) != 0 {
		panic("conflictNumber > 0")
	}
	n.dependers.Scan(func(_ nodeID, node *Node) bool {
		node.mu.Lock()
		defer node.mu.Unlock()

		node.conflictCounts[n.assignedTo]--
		if node.conflictCounts[n.assignedTo] == 0 {
			delete(node.conflictCounts, n.assignedTo)
		}
		node.notifyMaybeResolved()
		return true
	})

	n.dependers = btree.Map[nodeID, *Node]{}
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

	if n.onResolved != nil {
		panic("OnNoConflict is already called")
	}

	workerNum, ok := n.tryResolve()
	if ok {
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
		n.conflictCounts = make(map[workerID]int)
	}
}
