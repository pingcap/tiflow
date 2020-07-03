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

	"github.com/pingcap/check"
)

type tsHeapSuite struct{}

var _ = check.Suite(&tsHeapSuite{})

func (s *tsHeapSuite) TestInsert(c *check.C) {
	var heap fibonacciHeap
	target := uint64(15000)

	for i := 0; i < 5000; i++ {
		heap.Insert(uint64(10001) + target + 1)
	}
	heap.Insert(target)

	c.Assert(heap.GetMinKey(), check.Equals, target)
}

func (s *tsHeapSuite) TestUpdateTs(c *check.C) {
	rand.Seed(0xdeadbeaf)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 50000)
	for i := range nodes {
		nodes[i] = heap.Insert(10000 + uint64(rand.Intn(len(nodes)/2)))
	}
	for i := range nodes {
		min := heap.GetMinKey()
		expectedMin := uint64(math.MaxUint64)
		for _, n := range nodes {
			if expectedMin > n.key {
				expectedMin = n.key
			}
		}
		c.Assert(min, check.Equals, expectedMin)
		if rand.Intn(2) == 0 {
			heap.UpdateKey(nodes[i], nodes[i].key+uint64(10000))
		} else {
			heap.UpdateKey(nodes[i], nodes[i].key-uint64(10000))
		}
	}
}

func (s *tsHeapSuite) TestRemoveNode(c *check.C) {
	rand.Seed(0xdeadbeaf)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 50000)
	for i := range nodes {
		nodes[i] = heap.Insert(10000 + uint64(rand.Intn(len(nodes)/2)))
	}

	for i := range nodes {
		min := heap.GetMinKey()
		expectedMin := uint64(math.MaxUint64)
		for _, n := range nodes {
			if isRemoved(n) {
				continue
			}
			if expectedMin > n.key {
				expectedMin = n.key
			}
		}
		c.Assert(min, check.Equals, expectedMin)
		heap.Remove(nodes[i])
	}
	for _, n := range nodes {
		if !isRemoved(n) {
			c.Fatal("all of the node shoule be removed")
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
