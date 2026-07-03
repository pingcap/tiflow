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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlotsTrivial(t *testing.T) {
	t.Parallel()

	const count = 1000
	slots := NewSlots(8)
	nodes := make([]*Node, 0, 1000)

	for i := 0; i < count; i++ {
		node := newNodeForTest(1, 2, 3, 4, 5)
		slots.Add(node)
		nodes = append(nodes, node)
	}

	for i := 0; i < count; i++ {
		slots.Remove(nodes[i])
	}

	require.Equal(t, 0, len(slots.slots[1].nodes))
	require.Equal(t, 0, len(slots.slots[2].nodes))
	require.Equal(t, 0, len(slots.slots[3].nodes))
	require.Equal(t, 0, len(slots.slots[4].nodes))
	require.Equal(t, 0, len(slots.slots[5].nodes))
}

func TestSlotsConcurrentOps(t *testing.T) {
	t.Parallel()

	const N = 256
	slots := NewSlots(8)
	freeNodeChan := make(chan *Node, N)
	inuseNodeChan := make(chan *Node, N)
	for i := 0; i < N; i++ {
		freeNodeChan <- newNodeForTest(1, 9, 17, 25, 33)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// test concurrent add and remove won't panic
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case node := <-freeNodeChan:
				// keys belong to the same slot after hash, since slot num is 8
				slots.Add(node)
				inuseNodeChan <- node
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case node := <-inuseNodeChan:
				// keys belong to the same slot after hash, since slot num is 8
				slots.Remove(node)
				freeNodeChan <- newNodeForTest(1, 9, 17, 25, 33)
			}
		}
	}()

	wg.Wait()
}

func TestSortAndDedupHash(t *testing.T) {
	// If a transaction contains multiple rows, these rows may generate the same hash
	// in some rare cases. We should dedup these hashes to avoid unnecessary self cyclic
	// dependency in the causality dependency graph.
	t.Parallel()
	testCases := []struct {
		hashes   []uint64
		expected []uint64
	}{{
		// No duplicate hashes
		hashes:   []uint64{1, 2, 3, 4, 5},
		expected: []uint64{1, 2, 3, 4, 5},
	}, {
		// Duplicate hashes
		hashes:   []uint64{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
		expected: []uint64{1, 2, 3, 4, 5},
	}, {
		// Has hash value larger than slots count, should sort by `hash % numSlots` first.
		hashes:   []uint64{4, 9, 9, 3},
		expected: []uint64{9, 3, 4},
	}}

	for _, tc := range testCases {
		require.Equal(t, tc.expected, sortAndDedupHashes(tc.hashes, 8))
	}
}
