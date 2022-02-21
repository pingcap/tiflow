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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type tsHeapSuite struct{}

var _ = check.Suite(&tsHeapSuite{})

func (s *tsHeapSuite) TestInsert(c *check.C) {
	defer testleak.AfterTest(c)()
	var heap fibonacciHeap
	target := uint64(15000)

	for i := 0; i < 5000; i++ {
		heap.Insert(uint64(10001) + target + 1)
	}
	heap.Insert(target)

	c.Assert(heap.GetMinKey(), check.Equals, target)
}

func (s *tsHeapSuite) TestUpdateTs(c *check.C) {
	defer testleak.AfterTest(c)()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 2000)
	expectedMin := uint64(math.MaxUint64)
	for i := range nodes {
		key := 10000 + uint64(rand.Intn(len(nodes)/2))
		nodes[i] = heap.Insert(key)
		if expectedMin > key {
			expectedMin = key
		}
	}

	var key uint64
	for i := range nodes {
		min := heap.GetMinKey()
		c.Assert(min, check.Equals, expectedMin, check.Commentf("seed:%d", seed))
		if rand.Intn(2) == 0 {
			key = nodes[i].key + uint64(10000)
			heap.UpdateKey(nodes[i], key)
		} else {
			key = nodes[i].key - uint64(10000)
			heap.UpdateKey(nodes[i], key)
		}
		if expectedMin > key {
			expectedMin = key
		}
	}
}

func (s *tsHeapSuite) TestRemoveNode(c *check.C) {
	defer testleak.AfterTest(c)()
	seed := time.Now().Unix()
	rand.Seed(seed)
	var heap fibonacciHeap
	nodes := make([]*fibonacciHeapNode, 2000)
	expectedMin := uint64(math.MaxUint64)
	for i := range nodes {
		nodes[i] = heap.Insert(10000 + uint64(rand.Intn(len(nodes)/2)))
		if nodes[i].key < expectedMin {
			expectedMin = nodes[i].key
		}
	}

	preKey := expectedMin + 1
	for i := range nodes {
		min := heap.GetMinKey()
		if preKey == expectedMin {
			expectedMin = uint64(math.MaxUint64)
			for _, n := range nodes {
				if isRemoved(n) {
					continue
				}
				if expectedMin > n.key {
					expectedMin = n.key
				}
			}
		}
		c.Assert(min, check.Equals, expectedMin, check.Commentf("seed:%d", seed))
		preKey = nodes[i].key
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
