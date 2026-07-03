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
	"sync/atomic"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type (
	cacheID = int64
)

const (
	unassigned    = cacheID(-2)
	assignedToAny = cacheID(-1)
)

var (
	nextNodeID = atomic.Int64{}

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
	id                  int64
	sortedDedupKeysHash []uint64

	// Called when all dependencies are resolved.
	TrySendToTxnCache func(id cacheID) bool
	// Set the id generator to get a random ID.
	RandCacheID func() cacheID
	// Set the callback that the node is notified.
	OnNotified func(callback func())

	// Following fields are used for notifying a node's dependers lock-free.
	totalDependencies    int32
	removedDependencies  int32
	resolvedDependencies int32
	resolvedList         []int64

	// Following fields are protected by `mu`.
	mu sync.Mutex

	assignedTo cacheID
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

func (n *Node) nodeID() int64 {
	return n.id
}

func (n *Node) dependOn(dependencyNodes map[int64]*Node) {
	resolvedDependencies := int32(0)

	depend := func(target *Node) {
		if target.id == n.id {
			log.Panic("node cannot depend on itself")
		}

		// The target node might be removed or modified in other places, for example
		// after its corresponding transaction has been executed.
		target.mu.Lock()
		defer target.mu.Unlock()

		if target.assignedTo != unassigned {
			// The target has already been assigned to a cache.
			// In this case, record the cache ID in `resolvedList`, and this node
			// probably can be sent to the same cache and executed sequentially.
			resolvedDependencies = atomic.AddInt32(&n.resolvedDependencies, 1)
			atomic.StoreInt64(&n.resolvedList[resolvedDependencies-1], target.assignedTo)
		}

		// Add the node to the target's dependers if the target has not been removed.
		if target.removed {
			// The target has already been removed.
			atomic.AddInt32(&n.removedDependencies, 1)
		} else if _, exist := target.getOrCreateDependers().ReplaceOrInsert(n); exist {
			// Should never depend on a target redundantly.
			log.Panic("should never exist")
		}
	}

	// `totalDependencies` and `resolvedList` must be initialized before depending on any targets.
	n.totalDependencies = int32(len(dependencyNodes))
	n.resolvedList = make([]int64, 0, n.totalDependencies)
	for i := 0; i < int(n.totalDependencies); i++ {
		n.resolvedList = append(n.resolvedList, unassigned)
	}

	for _, node := range dependencyNodes {
		depend(node)
	}

	n.maybeResolve()
}

func (n *Node) remove() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.removed = true
	if n.dependers != nil {
		// `mu` must be holded during accessing dependers.
		n.dependers.Ascend(func(node *Node) bool {
			atomic.AddInt32(&node.removedDependencies, 1)
			node.OnNotified(node.maybeResolve)
			return true
		})
		n.dependers.Clear(true)
		n.dependers = nil
	}
}

// tryAssignTo assigns a node to a cache. Returns `true` on success.
func (n *Node) tryAssignTo(cacheID int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.assignedTo != unassigned {
		// Already resolved by some other guys.
		return true
	}

	if n.TrySendToTxnCache != nil {
		ok := n.TrySendToTxnCache(cacheID)
		if !ok {
			return false
		}
		n.TrySendToTxnCache = nil
	}
	n.assignedTo = cacheID

	if n.dependers != nil {
		// `mu` must be holded during accessing dependers.
		n.dependers.Ascend(func(node *Node) bool {
			resolvedDependencies := atomic.AddInt32(&node.resolvedDependencies, 1)
			atomic.StoreInt64(&node.resolvedList[resolvedDependencies-1], n.assignedTo)
			node.OnNotified(node.maybeResolve)
			return true
		})
	}

	return true
}

func (n *Node) maybeResolve() {
	if cacheID, ok := n.tryResolve(); ok {
		if cacheID == unassigned {
			log.Panic("invalid cache ID", zap.Uint64("cacheID", uint64(cacheID)))
		}

		if cacheID != assignedToAny {
			n.tryAssignTo(cacheID)
			return
		}

		cacheID := n.RandCacheID()
		if !n.tryAssignTo(cacheID) {
			// If the cache is full, we need to try to assign to another cache.
			n.OnNotified(n.maybeResolve)
		}
	}
}

// tryResolve try to find a cache to assign the node to.
// Returns (_, false) if there is a conflict,
// returns (rand, true) if there is no conflict,
// returns (N, true) if only cache N can be used.
func (n *Node) tryResolve() (int64, bool) {
	if n.totalDependencies == 0 {
		// No conflicts, can select any caches.
		return assignedToAny, true
	}

	removedDependencies := atomic.LoadInt32(&n.removedDependencies)
	if removedDependencies == n.totalDependencies {
		// All dependcies are removed, so assign the node to any cache is fine.
		return assignedToAny, true
	}

	resolvedDependencies := atomic.LoadInt32(&n.resolvedDependencies)
	if resolvedDependencies == n.totalDependencies {
		firstDep := atomic.LoadInt64(&n.resolvedList[0])
		hasDiffDep := false
		for i := 1; i < int(n.totalDependencies); i++ {
			curr := atomic.LoadInt64(&n.resolvedList[i])
			// In DependOn, depend(nil) set resolvedList[i] to assignedToAny
			// for these no dependecy keys.
			if curr == assignedToAny {
				continue
			}
			if firstDep != curr {
				hasDiffDep = true
				break
			}
		}
		if !hasDiffDep && firstDep != unassigned {
			// If all dependency nodes are assigned to the same cache, we can assign
			// this node to the same cache directly, and they will execute sequentially.
			// On the other hand, if dependency nodes are assigned to different caches,
			// This node has to wait all dependency txn executed and all depencecy nodes
			// are removed.
			return firstDep, true
		}
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

// assignedWorkerID returns the cache ID that the node has been assigned to.
// NOTE: assignedWorkerID is used for unit tests only.
func (n *Node) assignedWorkerID() cacheID {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.assignedTo
}

func genNextNodeID() int64 {
	return nextNodeID.Add(1)
}
