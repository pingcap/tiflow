// Copyright 2020 PingCAP, Inc.
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

package frontier

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	t.Parallel()
	var heap fibonacciHeap
	target := uint64(15000)

	for i := 0; i < 5000; i++ {
		heap.Insert(uint64(10001) + target + 1)
	}
	heap.Insert(target)

	require.Equal(t, target, heap.GetMinKey())
}

func TestRandomUpdateTs(t *testing.T) {
	t.Parallel()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 20000)
	expectedMin := uint64(math.MaxUint64)
	for i := range nodes {
		key := 10000 + uint64(rand.Intn(len(nodes)/2))
		nodes[i] = heap.Insert(key)
		if expectedMin > key {
			expectedMin = key
		}
	}

	var key uint64
	lastOp := "Init"
	for i := 0; i < 100000; i++ {
		min := heap.GetMinKey()
		require.Equal(t, expectedMin, min,
			"seed:%d, lastOperation: %s, expected: %d, actual: %d",
			seed, lastOp, expectedMin, min)
		idx := rand.Intn(len(nodes))
		delta := rand.Uint64() % 10000
		if rand.Intn(2) == 0 {
			key = nodes[idx].key + delta
			heap.UpdateKey(nodes[idx], key)
			lastOp = "Increase"
		} else {
			if delta > nodes[idx].key {
				delta = nodes[idx].key
			}
			key = nodes[idx].key - delta
			heap.UpdateKey(nodes[idx], key)
			lastOp = "Decrease"
		}
		if expectedMin > key {
			expectedMin = key
		}
	}
}

func TestRemoveNode(t *testing.T) {
	t.Parallel()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 200000)
	nodesMap := make(map[*fibonacciHeapNode]struct{})
	expectedMin := uint64(math.MaxUint64)
	for i := range nodes {
		nodes[i] = heap.Insert(10000 + uint64(rand.Intn(len(nodes)/2)))
		nodesMap[nodes[i]] = struct{}{}
		if nodes[i].key < expectedMin {
			expectedMin = nodes[i].key
		}
	}

	preKey := expectedMin + 1
	for i := range nodes {
		min := heap.GetMinKey()
		if preKey == expectedMin {
			expectedMin = uint64(math.MaxUint64)
			for n := range nodesMap {
				if expectedMin > n.key {
					expectedMin = n.key
				}
			}
		}
		require.Equal(t, expectedMin, min, "seed:%d", seed)
		preKey = nodes[i].key
		heap.Remove(nodes[i])
		delete(nodesMap, nodes[i])
	}
	for _, n := range nodes {
		if !isRemoved(n) {
			t.Fatal("all of the node shoule be removed")
		}
	}
}

func isRemoved(n *fibonacciHeapNode) bool {
	return n.left == nil && n.right == nil && n.children == nil && n.parent == nil
}

func (x *fibonacciHeap) Entries(fn func(n *fibonacciHeapNode) bool) {
	heapNodeIterator(x.root, fn)
}

func heapNodeIterator(n *fibonacciHeapNode, fn func(n *fibonacciHeapNode) bool) {
	firstStep := true

	for next := n; next != nil && (next != n || firstStep); next = next.right {
		firstStep = false
		if !fn(next) {
			return
		}
		if next.children != nil {
			heapNodeIterator(next.children, fn)
		}
	}
}
