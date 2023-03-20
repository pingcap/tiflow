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

func TestNodeFree(t *testing.T) {
	// This case should not be run parallel to
	// others, for fear that the use-after-free
	// will race with NewNode() in other cases.

	nodeA := NewNode(true)
	nodeA.Free()

	nodeA = NewNode(true)
	nodeA.Free()

	// Double freeing should panic.
	require.Panics(t, func() {
		nodeA.Free()
	})
}

func TestNodeEquals(t *testing.T) {
	t.Parallel()

	nodeA := NewNode(true)
	nodeB := NewNode(true)
	require.False(t, nodeA.NodeID() == nodeB.NodeID())
	require.True(t, nodeA.NodeID() == nodeA.NodeID())
}

func TestNodeDependOn(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph: A --> B
	nodeA := NewNode(true)
	nodeB := NewNode(true)

	nodeA.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB}, 999)
	require.Equal(t, nodeA.dependerCount(), 0)
	require.Equal(t, nodeB.dependerCount(), 1)
}

func TestNodeSingleDependency(t *testing.T) {
	t.Parallel()

	// Node B depends on A, without any other resolved dependencies.
	nodeA := NewNode(true)
	nodeB := NewNode(true)
	nodeB.RandWorkerID = func() workerID { return 100 }
	nodeB.OnResolved = func(_ workerID, serialize bool) { require.False(t, serialize) }
	nodeB.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA}, 0)
	require.True(t, nodeA.assignTo(resolveTo(1)))
	require.Equal(t, workerID(1), nodeA.assignedWorkerID())
	require.Equal(t, workerID(1), nodeB.assignedWorkerID())

	// Node D depends on C, with some other resolved dependencies.
	nodeC := NewNode(true)
	nodeD := NewNode(true)
	nodeD.RandWorkerID = func() workerID { return 100 }
	nodeD.OnResolved = func(_ workerID, serialize bool) { require.True(t, serialize) }
	nodeD.DependOn(map[int64]*Node{nodeA.NodeID(): nodeC}, 999)
	require.True(t, nodeC.assignTo(resolveTo(2)))
	require.Equal(t, workerID(2), nodeC.assignedWorkerID())
	nodeC.Remove()
	require.Equal(t, workerID(100), nodeD.assignedWorkerID())
}

func TestNodeMultipleDependencies(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph:
	//   ┌────►A
	// C─┤
	//   └────►B

	nodeA := NewNode(true)
	nodeB := NewNode(true)
	nodeC := NewNode(true)

	nodeC.RandWorkerID = func() workerID { return 100 }
	nodeC.OnResolved = func(_ workerID, serialize bool) { require.True(t, serialize) }
	nodeC.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA, nodeB.NodeID(): nodeB}, 999)

	require.True(t, nodeA.assignTo(resolveTo(1)))
	require.True(t, nodeB.assignTo(resolveTo(2)))

	require.Equal(t, unassigned, nodeC.assignedWorkerID())

	nodeA.Remove()
	nodeB.Remove()
	require.Equal(t, int64(100), nodeC.assignedWorkerID())
}

func TestNodeResolveImmediately(t *testing.T) {
	t.Parallel()

	// Node A depends on 0 unresolved dependencies and some resolved dependencies.
	nodeA := NewNode(true)
	nodeA.RandWorkerID = func() workerID { return workerID(100) }
	nodeA.OnResolved = func(_ workerID, serialize bool) { require.True(t, serialize) }
	nodeA.DependOn(nil, 999)
	require.Equal(t, workerID(100), nodeA.assignedWorkerID())

	// Node D depends on B and C, all of them are assigned to 1.
	nodeB := NewNode(true)
	require.True(t, nodeB.assignTo(resolveTo(1)))
	nodeC := NewNode(true)
	require.True(t, nodeC.assignTo(resolveTo(1)))
	nodeD := NewNode(true)
	nodeD.RandWorkerID = func() workerID { return workerID(100) }
	nodeD.OnResolved = func(_ workerID, serialize bool) { require.False(t, serialize) }
	nodeD.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB, nodeC.NodeID(): nodeC}, 0)
	require.Equal(t, workerID(1), nodeD.assignedWorkerID())

	// Node E depends on B and C and some other resolved dependencies.
	nodeB.Remove()
	nodeC.Remove()
	nodeE := NewNode(true)
	nodeE.RandWorkerID = func() workerID { return workerID(100) }
	nodeC.OnResolved = func(_ workerID, serialize bool) { require.True(t, serialize) }
	nodeE.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB, nodeC.NodeID(): nodeC}, 999)
	require.Equal(t, workerID(100), nodeE.assignedWorkerID())
}

func TestNodeDispatchSerialize(t *testing.T) {
	t.Parallel()

	nodeA := NewNode(false)
	nodeB := NewNode(false)
	nodeA.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB}, 0)
	require.Equal(t, nodeB.dependerCount(), 1)

	require.True(t, nodeB.assignTo(resolveTo(1)))
	require.Equal(t, unassigned, nodeA.assignedWorkerID())
}

func TestNodeDependOnSelf(t *testing.T) {
	t.Parallel()

	nodeA := NewNode(true)
	require.Panics(t, func() {
		nodeA.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA}, 999)
	})
}

func TestNodeDoubleAssigning(t *testing.T) {
	t.Parallel()

	nodeA := NewNode(true)
	require.True(t, nodeA.assignTo(resolveTo(1)))
	require.False(t, nodeA.assignTo(resolveTo(2)))
}
