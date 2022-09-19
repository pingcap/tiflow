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

	nodeA.DependOn(map[int64]*Node{nodeB.NodeID(): nodeB})
	require.Equal(t, nodeA.dependerCount(), 0)
	require.Equal(t, nodeB.dependerCount(), 1)
}

func TestNodeSingleDependency(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()

	nodeB := NewNode()
	nodeB.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA})

	require.True(t, nodeA.assignTo(1))
	require.Equal(t, workerID(1), nodeA.assignedWorkerID())
	require.Equal(t, workerID(1), nodeB.assignedWorkerID())
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

	nodeC.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA, nodeB.NodeID(): nodeB})
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

	nodeA := NewNode()
	nodeA.RandWorkerID = func() workerID { return workerID(100) }
	nodeA.DependOn(nil)
	require.Equal(t, workerID(100), nodeA.assignedWorkerID())
}

func TestNodeDependOnSelf(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	require.Panics(t, func() {
		nodeA.DependOn(map[int64]*Node{nodeA.NodeID(): nodeA})
	})
}

func TestNodeDoubleAssigning(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	require.True(t, nodeA.assignTo(1))
	require.False(t, nodeA.assignTo(2))
}
