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

type testElem struct {
	id int
}

func (e *testElem) Equals(other *testElem) bool {
	return e.id == other.id
}

func TestSlotsTrivial(t *testing.T) {
	t.Parallel()

	const count = 1000
	slots := NewSlots[*testElem](8)
	for i := 0; i < count; i++ {
		slots.Add(&testElem{id: i}, []int64{1, 2, 3, 4, 5}, func(_ *testElem) {})
	}

	for i := 0; i < count; i++ {
		slots.Remove(&testElem{id: i}, []int64{1, 2, 3, 4, 5})
	}

	require.Equal(t, 0, slots.slots[1].Len())
	require.Equal(t, 0, slots.slots[2].Len())
	require.Equal(t, 0, slots.slots[3].Len())
	require.Equal(t, 0, slots.slots[4].Len())
	require.Equal(t, 0, slots.slots[5].Len())
}
