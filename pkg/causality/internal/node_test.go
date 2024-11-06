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
	"testing"

	"github.com/stretchr/testify/require"
)

func newNodeForTest(hashes ...uint64) *Node {
	return &Node{
		id:                  genNextNodeID(),
		sortedDedupKeysHash: sortAndDedupHashes(hashes, 8),
		assignedTo:          unassigned,
		RandCacheID:         func() cacheID { return 100 },
		OnNotified: func(callback func()) {
			// run the callback immediately
			callback()
		},
	}
}

func TestNodeEquals(t *testing.T) {
	t.Parallel()

	nodeA := newNodeForTest()
	nodeB := newNodeForTest()
	require.False(t, nodeA.nodeID() == nodeB.nodeID())
	require.True(t, nodeA.nodeID() == nodeA.nodeID())
}

func TestNodeDependOn(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph: A --> B
	nodeA := newNodeForTest()
	nodeB := newNodeForTest()

	nodeA.dependOn(map[int64]*Node{nodeB.nodeID(): nodeB})
	require.Equal(t, nodeA.dependerCount(), 0)
	require.Equal(t, nodeB.dependerCount(), 1)
}

func TestNodeSingleDependency(t *testing.T) {
	t.Parallel()

	// Node B depends on A
	nodeA := newNodeForTest()
	nodeB := newNodeForTest()
	nodeB.dependOn(map[int64]*Node{nodeA.nodeID(): nodeA})
	require.True(t, nodeA.tryAssignTo(1))
	require.Equal(t, cacheID(1), nodeA.assignedWorkerID())
	require.Equal(t, cacheID(1), nodeB.assignedWorkerID())

	// Node D depends on C
	nodeC := newNodeForTest()
	nodeD := newNodeForTest()
	nodeD.dependOn(map[int64]*Node{nodeA.nodeID(): nodeC})
	require.True(t, nodeC.tryAssignTo(2))
	require.Equal(t, cacheID(2), nodeC.assignedWorkerID())
	nodeC.remove()
	require.Equal(t, cacheID(2), nodeD.assignedWorkerID())
}

func TestNodeMultipleDependencies(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph:
	//   ┌────►A
	// C─┤
	//   └────►B

	nodeA := newNodeForTest()
	nodeB := newNodeForTest()
	nodeC := newNodeForTest()

	nodeC.dependOn(map[int64]*Node{nodeA.nodeID(): nodeA, nodeB.nodeID(): nodeB})

	require.True(t, nodeA.tryAssignTo(1))
	require.True(t, nodeB.tryAssignTo(2))

	require.Equal(t, unassigned, nodeC.assignedWorkerID())

	nodeA.remove()
	nodeB.remove()
	require.Equal(t, int64(100), nodeC.assignedWorkerID())
}

func TestNodeResolveImmediately(t *testing.T) {
	t.Parallel()

	// Node A depends on 0 unresolved dependencies and some resolved dependencies.
	nodeA := newNodeForTest()
	nodeA.dependOn(nil)
	require.Equal(t, cacheID(100), nodeA.assignedWorkerID())

	// Node D depends on B and C, all of them are assigned to 1.
	nodeB := newNodeForTest()
	require.True(t, nodeB.tryAssignTo(1))
	nodeC := newNodeForTest()
	require.True(t, nodeC.tryAssignTo(1))
	nodeD := newNodeForTest()
	nodeD.dependOn(map[int64]*Node{nodeB.nodeID(): nodeB, nodeC.nodeID(): nodeC})
	require.Equal(t, cacheID(1), nodeD.assignedWorkerID())

	// Node E depends on B and C and some other resolved dependencies.
	nodeB.remove()
	nodeC.remove()
	nodeE := newNodeForTest()
	nodeE.dependOn(map[int64]*Node{nodeB.nodeID(): nodeB, nodeC.nodeID(): nodeC})
	require.Equal(t, cacheID(100), nodeE.assignedWorkerID())
}

func TestNodeDependOnSelf(t *testing.T) {
	t.Parallel()

	nodeA := newNodeForTest()
	require.Panics(t, func() {
		nodeA.dependOn(map[int64]*Node{nodeA.nodeID(): nodeA})
	})
}

func TestNodeDoubleAssigning(t *testing.T) {
	t.Parallel()

	nodeA := newNodeForTest()
	nodeA.TrySendToTxnCache = func(id cacheID) bool {
		return id == 2
	}
	require.False(t, nodeA.tryAssignTo(1))
	require.True(t, nodeA.tryAssignTo(2))
	require.True(t, nodeA.tryAssignTo(2))
}
