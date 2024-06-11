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
)

type slot struct {
	nodes map[uint64]*Node
	mu    sync.Mutex
}

// Slots implements slot-based conflict detection.
// It holds references to Node, which can be used to build
// a DAG of dependency.
type Slots struct {
	slots    []slot
	numSlots uint64
}

// NewSlots creates a new Slots.
func NewSlots(numSlots uint64) *Slots {
	slots := make([]slot, numSlots)
	for i := uint64(0); i < numSlots; i++ {
		slots[i].nodes = make(map[uint64]*Node, 8)
	}
	return &Slots{
		slots:    slots,
		numSlots: numSlots,
	}
}

// AllocNode allocates a new node and initializes it with the given hashes.
// TODO: reuse node if necessary. Currently it's impossible if async-notify is used.
// The reason is a node can step functions `assignTo`, `Remove`, `free`, then `assignTo`.
// again. In the last `assignTo`, it can never know whether the node has been reused
// or not.
func (s *Slots) AllocNode(hashes []uint64) *Node {
	return &Node{
		id:           genNextNodeID(),
		conflictKeys: hashes,
		assignedTo:   unassigned,
	}
}

// Add adds an elem to the slots and calls DependOn for elem.
func (s *Slots) Add(elem *Node) {
	dependencyNodes := make(map[int64]*Node, len(elem.conflictKeys))

	for _, hash := range elem.conflictKeys {
		slotIdx := getSlot(hash, s.numSlots)
		s.slots[slotIdx].mu.Lock()

		// If there is a node occpuied the same hash slot, we may have conflict with it.
		// Add the conflict node to the dependencyNodes.
		if prevNode, ok := s.slots[slotIdx].nodes[hash]; ok && prevNode.nodeID() != elem.nodeID() {
			prevID := prevNode.nodeID()
			// If there are multiple hashes conflicts with the same node, we only need to
			// depend on the node once.
			dependencyNodes[prevID] = prevNode
		}
		// Add this node to the slot, make sure new coming nodes with the same hash should
		// depend on this node.
		s.slots[slotIdx].nodes[hash] = elem

		s.slots[slotIdx].mu.Unlock()
	}

	// Construct the dependency graph based on collected `dependencyNodes` and with corresponding
	// slots locked.
	elem.dependOn(dependencyNodes)
}

// Remove removes an element from the Slots.
func (s *Slots) Remove(elem *Node) {
	elem.remove()
	for _, hash := range elem.conflictKeys {
		slotIdx := getSlot(hash, s.numSlots)
		s.slots[slotIdx].mu.Lock()
		// Remove the node from the slot.
		// If the node is not in the slot, it means the node has been replaced by new node with the same hash,
		// in this case we don't need to remove it from the slot.
		if tail, ok := s.slots[slotIdx].nodes[hash]; ok && tail.nodeID() == elem.nodeID() {
			delete(s.slots[slotIdx].nodes, hash)
		}
		s.slots[slotIdx].mu.Unlock()
	}
}

func getSlot(hash, numSlots uint64) uint64 {
	return hash % numSlots
}
