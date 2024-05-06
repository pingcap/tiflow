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

var _ SlotNode[*Node] = &Node{} // Asserts that *Node implements SlotNode[*Node].

func newNodeForTest() *Node {
	node := NewNode(nil)
	node.OnNotified = func(callback func()) {
		// run the callback immediately
		callback()
	}
	return node
}

func TestNodeFree(t *testing.T) {
	// This case should not be run parallel to
	// others, for fear that the use-after-free
	// will race with newNodeForTest() in other cases.

	nodeA := newNodeForTest()
	nodeA.Free()

	nodeA = newNodeForTest()
	nodeA.Free()

	// Double freeing should panic.
	require.Panics(t, func() {
		nodeA.Free()
	})
}

func TestNodeEquals(t *testing.T) {
	t.Parallel()

	nodeA := newNodeForTest()
	nodeB := newNodeForTest()
	require.False(t, nodeA.NodeID() == nodeB.NodeID())
	require.True(t, nodeA.NodeID() == nodeA.NodeID())
}

func TestNodeDependOn(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph: A --> B
	nodeA := newNodeForTest()
	nodeB := newNodeForTest()

	nodeA.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB}, 999)
	require.Equal(t, nodeA.dependerCount(), 0)
	require.Equal(t, nodeB.dependerCount(), 1)
}

func TestNodeSingleDependency(t *testing.T) {
	t.Parallel()

	// Node B depends on A, without any other resolved dependencies.
	nodeA := newNodeForTest()
	nodeB := newNodeForTest()
	nodeB.RandCacheID = func() cacheID { return 100 }
	nodeB.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA}, 0)
	require.True(t, nodeA.tryAssignTo(1))
	require.Equal(t, cacheID(1), nodeA.assignedWorkerID())
	require.Equal(t, cacheID(1), nodeB.assignedWorkerID())

	// Node D depends on C, with some other resolved dependencies.
	nodeC := newNodeForTest()
	nodeD := newNodeForTest()
	nodeD.RandCacheID = func() cacheID { return 100 }
	nodeD.DependOn(map[int64]*Node{nodeA.NodeID(): nodeC}, 999)
	require.True(t, nodeC.tryAssignTo(2))
	require.Equal(t, cacheID(2), nodeC.assignedWorkerID())
	nodeC.Remove()
	require.Equal(t, cacheID(100), nodeD.assignedWorkerID())
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

	nodeC.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA, nodeB.NodeID(): nodeB}, 999)
	nodeC.RandCacheID = func() cacheID { return 100 }

	require.True(t, nodeA.tryAssignTo(1))
	require.True(t, nodeB.tryAssignTo(2))

	require.Equal(t, unassigned, nodeC.assignedWorkerID())

	nodeA.Remove()
	nodeB.Remove()
	require.Equal(t, int64(100), nodeC.assignedWorkerID())
}

func TestNodeResolveImmediately(t *testing.T) {
	t.Parallel()

	// Node A depends on 0 unresolved dependencies and some resolved dependencies.
	nodeA := newNodeForTest()
	nodeA.RandCacheID = func() cacheID { return cacheID(100) }
	nodeA.DependOn(nil, 999)
	require.Equal(t, cacheID(100), nodeA.assignedWorkerID())

	// Node D depends on B and C, all of them are assigned to 1.
	nodeB := newNodeForTest()
	require.True(t, nodeB.tryAssignTo(1))
	nodeC := newNodeForTest()
	require.True(t, nodeC.tryAssignTo(1))
	nodeD := newNodeForTest()
	nodeD.RandCacheID = func() cacheID { return cacheID(100) }
	nodeD.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB, nodeC.NodeID(): nodeC}, 0)
	require.Equal(t, cacheID(1), nodeD.assignedWorkerID())

	// Node E depends on B and C and some other resolved dependencies.
	nodeB.Remove()
	nodeC.Remove()
	nodeE := newNodeForTest()
	nodeE.RandCacheID = func() cacheID { return cacheID(100) }
	nodeE.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB, nodeC.NodeID(): nodeC}, 999)
	require.Equal(t, cacheID(100), nodeE.assignedWorkerID())
}

func TestNodeDependOnSelf(t *testing.T) {
	t.Parallel()

	nodeA := newNodeForTest()
	require.Panics(t, func() {
		nodeA.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA}, 999)
	})
}

func TestNodeDoubleAssigning(t *testing.T) {
	t.Parallel()

	// nodeA := newNodeForTest()
	// require.True(t, nodeA.tryAssignTo(1))
	// require.False(t, nodeA.tryAssignTo(2))

	require.True(t, -1 == assignedToAny)
}
