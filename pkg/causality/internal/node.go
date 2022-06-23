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
	// Note: We are using generic Nodes, so that
	// ideally we need one pool for each type parameter.
	// But since we are using generics mainly for readability,
	// it does not seem necessary.
	// TODO think about whether to abandon generics.
	nodePool = &sync.Pool{}
)

type Node[T any] struct {
	id int64 // immutable

	mu             sync.Mutex
	conflictCounts map[workerID]int
	dependers      map[nodeID]*Node[T]
	assignedTo     workerID
	onResolved     func(id workerID)

	data T
}

func NewNode[T any](data T) (ret *Node[T]) {
	defer func() {
		ret.id = nextNodeID.Add(1)
		ret.data = data
	}()

	if obj := nodePool.Get(); obj != nil {
		return obj.(*Node[T])
	}
	return new(Node[T])
}

func (n *Node[T]) Free() {
	n.id = -1
	n.conflictCounts = nil
	n.dependers = nil

	var zeroData T
	n.data = zeroData

	nodePool.Put(n)
}

func (n *Node[T]) Data() T {
	return n.data
}

func (n *Node[T]) DependOn(target *Node[T]) {
	// Lock target first because we are always
	// locking an earlier transaction first, so
	// that there will be not deadlocking.
	target.mu.Lock()
	defer target.mu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	target.dependers[n.id] = n
	n.conflictCounts[target.assignedTo]++
}

func (n *Node[T]) AssignTo(workerID int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.assignedTo != unassigned {
		panic(fmt.Sprintf(
			"assigning an already assigned node: id %d, worker %d",
			n.id, n.assignedTo))
	}

	n.assignedTo = workerID
	for _, node := range n.dependers {
		// Use a closure to make it possible to use deferred unlock.
		func() {
			node.mu.Lock()
			defer node.mu.Unlock()

			node.conflictCounts[unassigned]--
			node.conflictCounts[workerID]++

			node.notifyMaybeResolved()
		}()
	}
}

func (n *Node[T]) Remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, node := range n.dependers {
		// Use a closure to make it possible to use deferred unlock.
		func() {
			node.mu.Lock()
			defer node.mu.Unlock()

			node.conflictCounts[n.assignedTo]--
			node.notifyMaybeResolved()
		}()
	}
}

func (n *Node[T]) Equals(other *Node[T]) bool {
	return n.id == other.id
}

func (n *Node[T]) AsyncResolve(fn func(id workerID)) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.onResolved != nil {
		panic("AsyncResolve is already called")
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
func (n *Node[T]) notifyMaybeResolved() {
	workerNum, ok := n.tryResolve()
	if !ok {
		return
	}

	if n.onResolved != nil {
		n.onResolved(workerNum)
	}
}

// tryResolve must be called with n.mu locked.
// Returns (_, false) if there is a conflict,
// returns (-1, true) if there is no conflict,
// returns (N, true) if only worker N can be used.
func (n *Node[T]) tryResolve() (int64, bool) {
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
