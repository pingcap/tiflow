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

	nodeA := NewNode()
	nodeA.Free()

	nodeA = NewNode()
	nodeA.Free()

	// Double freeing should panic.
	require.Panics(t, func() {
		nodeA.Free()
	})
}

func TestNodeEquals(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	nodeB := NewNode()
	require.False(t, nodeA.NodeID() == nodeB.NodeID())
	require.True(t, nodeA.NodeID() == nodeA.NodeID())
}

func TestNodeDependOn(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph: A --> B
	nodeA := NewNode()
	nodeB := NewNode()

	nodeA.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB}, 999)
	require.Equal(t, nodeA.dependerCount(), 0)
	require.Equal(t, nodeB.dependerCount(), 1)
}

func TestNodeSingleDependency(t *testing.T) {
	t.Parallel()

	// Node B depends on A, without any other removed dependencies.
	nodeA := NewNode()
	nodeB := NewNode()
	nodeB.RandWorkerID = func() workerID { return 100 }
	nodeB.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA}, 0)
	require.True(t, nodeA.assignTo(1))
	require.Equal(t, workerID(1), nodeA.assignedWorkerID())
	// Node B should be unassigned before Node A is removed.
	require.Equal(t, unassigned, nodeB.assignedWorkerID())
	nodeA.Remove()
	// Node B should be assigned to random worker after Node A is removed.
	require.Equal(t, workerID(100), nodeB.assignedWorkerID())

	// Node D depends on C, with some other resolved dependencies.
	nodeC := NewNode()
	nodeD := NewNode()
	nodeD.RandWorkerID = func() workerID { return 100 }
	nodeD.DependOn(map[int64]*Node{nodeA.NodeID(): nodeC}, 999)
	require.True(t, nodeC.assignTo(2))
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

	nodeA := NewNode()
	nodeB := NewNode()
	nodeC := NewNode()

	nodeC.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA, nodeB.NodeID(): nodeB}, 999)
	nodeC.RandWorkerID = func() workerID { return 100 }

	require.True(t, nodeA.assignTo(1))
	require.True(t, nodeB.assignTo(2))

	require.Equal(t, unassigned, nodeC.assignedWorkerID())

	nodeA.Remove()
	nodeB.Remove()
	require.Equal(t, int64(100), nodeC.assignedWorkerID())
}

func TestNodeResolveImmediately(t *testing.T) {
	t.Parallel()

	// Node A depends on 0 unresolved dependencies and some resolved dependencies.
	nodeA := NewNode()
	nodeA.RandWorkerID = func() workerID { return workerID(100) }
	nodeA.DependOn(nil, 999)
	require.Equal(t, workerID(100), nodeA.assignedWorkerID())

	// Node D depends on B and C, all of them are assigned to 1.
	nodeB := NewNode()
	require.True(t, nodeB.assignTo(1))
	nodeC := NewNode()
	require.True(t, nodeC.assignTo(1))
	nodeD := NewNode()
	nodeD.RandWorkerID = func() workerID { return workerID(100) }
	nodeD.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB, nodeC.NodeID(): nodeC}, 0)
	// NodeD should be unassigned before Node B and C are removed.
	require.Equal(t, unassigned, nodeD.assignedWorkerID())
	nodeB.Remove()
	nodeC.Remove()
	// NodeD should be assigned to random worker after Node B and C are removed.
	require.Equal(t, workerID(100), nodeD.assignedWorkerID())

	// Node E depends on B and C and some other resolved dependencies.
	nodeE := NewNode()
	nodeE.RandWorkerID = func() workerID { return workerID(100) }
	nodeE.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB, nodeC.NodeID(): nodeC}, 999)
	require.Equal(t, workerID(100), nodeE.assignedWorkerID())
}

func TestNodeDependOnSelf(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	require.Panics(t, func() {
		nodeA.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA}, 999)
	})
}

func TestNodeDoubleAssigning(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	require.True(t, nodeA.assignTo(1))
	require.False(t, nodeA.assignTo(2))
}
