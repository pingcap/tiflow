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
	"go.uber.org/atomic"
)

var _ Eq[*Node] = &Node{} // Asserts that *Node implements Eq[*Node].

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
	require.False(t, nodeA.Equals(nodeB))
	require.False(t, nodeB.Equals(nodeA))
	require.True(t, nodeA.Equals(nodeA))
}

func TestNodeDependOn(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph: A --> B
	nodeA := NewNode()
	nodeB := NewNode()

	nodeA.DependOn(nodeB)
	require.Equal(t, nodeA.dependerCount(), 0)
	require.Equal(t, nodeB.dependerCount(), 1)

	// DependOn should be idempotent.
	nodeA.DependOn(nodeB)
	require.Equal(t, nodeB.dependerCount(), 1)
}

func TestNodeSingleDependency(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()

	var onNoConflictCalled atomic.Bool
	nodeB := NewNode()
	nodeB.DependOn(nodeA)
	nodeB.OnNoConflict(func(id workerID) {
		require.Equal(t, workerID(1), id)
		require.False(t, onNoConflictCalled.Swap(true))
	})

	nodeA.AssignTo(1)
	require.True(t, onNoConflictCalled.Load())
	require.Equal(t, workerID(1), nodeA.assignedWorkerID())
	require.Equal(t, unassigned, nodeB.assignedWorkerID())

	nodeB.AssignTo(workerID(1))
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

	nodeC.DependOn(nodeA)
	nodeC.DependOn(nodeB)

	var onNoConflictCalled atomic.Bool
	nodeC.OnNoConflict(func(id workerID) {
		require.Equal(t, workerID(1), id)
		require.False(t, onNoConflictCalled.Swap(true))
	})

	require.False(t, onNoConflictCalled.Load())
	nodeA.AssignTo(1)
	require.False(t, onNoConflictCalled.Load())
	nodeB.AssignTo(2)
	require.False(t, onNoConflictCalled.Load())
	nodeB.Remove()
	require.True(t, onNoConflictCalled.Load())
}

func TestNodeResolveImmediately(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	var onNoConflictCalled atomic.Bool
	nodeA.OnNoConflict(func(id workerID) {
		require.Equal(t, unassigned, id) // WorkerID is indeterminate.
		require.False(t, onNoConflictCalled.Swap(true))
	})
	require.True(t, onNoConflictCalled.Load())
}

func TestNodeDependOnSelf(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	require.Panics(t, func() {
		nodeA.DependOn(nodeA)
	})
}

func TestNodeDoubleAssigning(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	nodeA.AssignTo(1)
	require.Panics(t, func() {
		nodeA.AssignTo(1)
	})
}

func TestNodeRemovedPrematurely(t *testing.T) {
	t.Parallel()

	// Construct a dependency graph: A --> B
	nodeA := NewNode()
	nodeB := NewNode()

	nodeA.DependOn(nodeB)

	require.Panics(t, func() {
		nodeA.Remove() // Removed prematurely here.
	})
}

func TestNodeDoubleOnNoConflict(t *testing.T) {
	t.Parallel()

	nodeA := NewNode()
	nodeA.OnNoConflict(func(_ workerID) {})

	require.Panics(t, func() {
		nodeA.OnNoConflict(func(_ workerID) {})
	})
}
