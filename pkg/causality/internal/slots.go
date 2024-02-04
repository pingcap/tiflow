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
	"math"
	"sync"
)

type slot[E SlotNode[E]] struct {
	nodes map[uint64]E
	mu    sync.Mutex
}

// SlotNode describes objects that can be compared for equality.
type SlotNode[T any] interface {
	// NodeID tells the node's ID.
	NodeID() int64
	// Construct a dependency on `others`.
	DependOn(dependencyNodes map[int64]T, noDependencyKeyCnt int)
	// Remove the node itself and notify all dependers.
	Remove()
	// Free the node itself and remove it from the graph.
	Free()
}

// Slots implements slot-based conflict detection.
// It holds references to E, which can be used to build
// a DAG of dependency.
type Slots[E SlotNode[E]] struct {
	slots    []slot[E]
	numSlots uint64
}

// NewSlots creates a new Slots.
func NewSlots[E SlotNode[E]](numSlots uint64) *Slots[E] {
	slots := make([]slot[E], numSlots)
	for i := uint64(0); i < numSlots; i++ {
		slots[i].nodes = make(map[uint64]E, 8)
	}
	return &Slots[E]{
		slots:    slots,
		numSlots: numSlots,
	}
}

// Add adds an elem to the slots and calls DependOn for elem.
func (s *Slots[E]) Add(elem E, hashes []uint64) {
	dependencyNodes := make(map[int64]E, len(hashes))
	noDependecyCnt := 0

	var lastSlot uint64 = math.MaxUint64
	for _, hash := range hashes {
		// lock the slot that the node belongs to.
		slotIdx := getSlot(hash, s.numSlots)
		if lastSlot != slotIdx {
			s.slots[slotIdx].mu.Lock()
			lastSlot = slotIdx
		}

		// If there is a node occpuied the same hash slot, we may have conflict with it.
		// Add the conflict node to the dependencyNodes.
		if prevNode, ok := s.slots[slotIdx].nodes[hash]; ok {
			prevID := prevNode.NodeID()
			// If there are multiple hashes conflicts with the same node, we only need to
			// depend on the node once.
			dependencyNodes[prevID] = prevNode
		} else {
			noDependecyCnt += 1
		}
		// Add this node to the slot, make sure new coming nodes with the same hash should
		// depend on this node.
		s.slots[slotIdx].nodes[hash] = elem
	}

	// Construct the dependency graph based on collected `dependencyNodes` and with corresponding
	// slots locked.
	elem.DependOn(dependencyNodes, noDependecyCnt)

	// Lock those slots one by one and then unlock them one by one, so that
	// we can avoid 2 transactions get executed interleaved.
	lastSlot = math.MaxUint64
	for _, hash := range hashes {
		slotIdx := getSlot(hash, s.numSlots)
		if lastSlot != slotIdx {
			s.slots[slotIdx].mu.Unlock()
			lastSlot = slotIdx
		}
	}
}

// Free removes an element from the Slots.
func (s *Slots[E]) Free(elem E, hashes []uint64) {
	for _, hash := range hashes {
		slotIdx := getSlot(hash, s.numSlots)
		s.slots[slotIdx].mu.Lock()
		// Remove the node from the slot.
		// If the node is not in the slot, it means the node has been replaced by new node with the same hash,
		// in this case we don't need to remove it from the slot.
		if tail, ok := s.slots[slotIdx].nodes[hash]; ok && tail.NodeID() == elem.NodeID() {
			delete(s.slots[slotIdx].nodes, hash)
		}
		s.slots[slotIdx].mu.Unlock()
	}
	elem.Free()
}

func getSlot(hash, numSlots uint64) uint64 {
	return hash % numSlots
}
