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

func TestSlotsTrivial(t *testing.T) {
	t.Parallel()

	const count = 1000
	slots := NewSlots[*Node](8)
	nodes := make([]*Node, 0, 1000)

	for i := 0; i < count; i++ {
		node := NewNode()
		node.RandWorkerID = func() workerID { return 100 }
		slots.Add(node, []uint64{1, 2, 3, 4, 5})
		nodes = append(nodes, node)
	}

	for i := 0; i < count; i++ {
		slots.Free(nodes[i], []uint64{1, 2, 3, 4, 5})
	}

	require.Equal(t, 0, len(slots.slots[1].nodes))
	require.Equal(t, 0, len(slots.slots[2].nodes))
	require.Equal(t, 0, len(slots.slots[3].nodes))
	require.Equal(t, 0, len(slots.slots[4].nodes))
	require.Equal(t, 0, len(slots.slots[5].nodes))
}
