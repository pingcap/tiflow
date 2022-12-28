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
	DependOn(unresolvedDeps map[int64]T, resolvedDeps int)
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
func (s *Slots[E]) Add(elem E, keys []uint64) {
	unresolvedDeps := make(map[int64]E, len(keys))
	resolvedDeps := 0

	var lastSlot uint64 = math.MaxUint64
	for _, key := range keys {
		slotIdx := getSlot(key, s.numSlots)
		if lastSlot != slotIdx {
			s.slots[slotIdx].mu.Lock()
			lastSlot = slotIdx
		}
		if tail, ok := s.slots[slotIdx].nodes[key]; ok {
			prevID := tail.NodeID()
			unresolvedDeps[prevID] = tail
		} else {
			resolvedDeps += 1
		}
		s.slots[slotIdx].nodes[key] = elem
	}
	elem.DependOn(unresolvedDeps, resolvedDeps)

	// Lock those slots one by one and then unlock them one by one, so that
	// we can avoid 2 transactions get executed interleaved.
	lastSlot = math.MaxUint64
	for _, key := range keys {
		slotIdx := getSlot(key, s.numSlots)
		if lastSlot != slotIdx {
			s.slots[slotIdx].mu.Unlock()
			lastSlot = slotIdx
		}
	}
}

// Free removes an element from the Slots.
func (s *Slots[E]) Free(elem E, keys []uint64) {
	for _, key := range keys {
		slotIdx := getSlot(key, s.numSlots)
		s.slots[slotIdx].mu.Lock()
		if tail, ok := s.slots[slotIdx].nodes[key]; ok && tail.NodeID() == elem.NodeID() {
			delete(s.slots[slotIdx].nodes, key)
		}
		s.slots[slotIdx].mu.Unlock()
	}
	elem.Free()
}

func getSlot(key, numSlots uint64) uint64 {
	return key % numSlots
}
