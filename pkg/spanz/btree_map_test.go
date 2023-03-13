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

package spanz

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/stretchr/testify/require"
)

func TestSpanMap(t *testing.T) {
	t.Parallel()

	m := NewBtreeMap[int]()

	// Insert then get.
	m.ReplaceOrInsert(tablepb.Span{TableID: 1}, 1)
	v, ok := m.Get(tablepb.Span{TableID: 1})
	require.Equal(t, v, 1)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
	require.True(t, m.Has(tablepb.Span{TableID: 1}))

	// Insert then get again.
	m.ReplaceOrInsert(tablepb.Span{TableID: 1, StartKey: []byte{1}}, 2)
	require.Equal(t, 2, m.Len())
	v, ok = m.Get(tablepb.Span{TableID: 1, StartKey: []byte{1}})
	require.Equal(t, v, 2)
	require.True(t, ok)

	// Overwrite then get.
	old, ok := m.ReplaceOrInsert(
		tablepb.Span{TableID: 1, StartKey: []byte{1}, EndKey: []byte{1}}, 3)
	require.Equal(t, old, 2)
	require.True(t, ok)
	require.Equal(t, 2, m.Len())
	require.True(t, m.Has(tablepb.Span{TableID: 1, StartKey: []byte{1}}))
	v, ok = m.Get(tablepb.Span{TableID: 1, StartKey: []byte{1}})
	require.Equal(t, v, 3)
	require.True(t, ok)

	// get value
	v = m.GetV(tablepb.Span{TableID: 1, StartKey: []byte{1}})
	require.Equal(t, v, 3)

	// Delete than get value
	v, ok = m.Delete(tablepb.Span{TableID: 1, StartKey: []byte{1}})
	require.Equal(t, v, 3)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
	require.False(t, m.Has(tablepb.Span{TableID: 1, StartKey: []byte{1}}))
	v = m.GetV(tablepb.Span{TableID: 1, StartKey: []byte{1}})
	require.Equal(t, v, 0)

	// Pointer value
	mp := NewBtreeMap[*int]()
	vp := &v
	mp.ReplaceOrInsert(tablepb.Span{TableID: 1}, vp)
	vp1, ok := mp.Get(tablepb.Span{TableID: 1})
	require.Equal(t, vp, vp1)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
}

func TestMapAscend(t *testing.T) {
	t.Parallel()

	m := NewBtreeMap[int]()
	for i := 0; i < 4; i++ {
		m.ReplaceOrInsert(tablepb.Span{TableID: int64(i)}, i)
	}

	j := 0
	m.Ascend(func(span tablepb.Span, value int) bool {
		require.Equal(t, tablepb.Span{TableID: int64(j)}, span)
		j++
		return true
	})
	require.Equal(t, 4, j)

	j = 0
	m.AscendRange(tablepb.Span{TableID: 1}, tablepb.Span{TableID: 2},
		func(span tablepb.Span, value int) bool {
			require.Equal(t, tablepb.Span{TableID: 1}, span)
			j++
			return true
		})
	require.Equal(t, 1, j)
}

func TestMapFindHole(t *testing.T) {
	t.Parallel()

	cases := []struct {
		spans         []tablepb.Span
		rang          [2]tablepb.Span
		expectedFound []tablepb.Span
		expectedHole  []tablepb.Span
	}{
		{ // 0. all found.
			spans: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t2_0")},
			},
			rang: [2]tablepb.Span{
				{StartKey: []byte("t1_0")},
				{StartKey: []byte("t2_0")},
			},
			expectedFound: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t2_0")},
			},
		},
		{ // 1. on hole in the middle.
			spans: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: [2]tablepb.Span{
				{StartKey: []byte("t1_0")},
				{StartKey: []byte("t2_0")},
			},
			expectedFound: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			expectedHole: []tablepb.Span{
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_3")},
			},
		},
		{ // 2. two holes in the middle.
			spans: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: [2]tablepb.Span{
				{StartKey: []byte("t1_0")},
				{StartKey: []byte("t2_0")},
			},
			expectedFound: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			expectedHole: []tablepb.Span{
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
			},
		},
		{ // 3. all missing.
			spans: []tablepb.Span{},
			rang: [2]tablepb.Span{
				{StartKey: []byte("t1_0")},
				{StartKey: []byte("t2_0")},
			},
			expectedHole: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			},
		},
		{ // 4. start not found
			spans: []tablepb.Span{
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: [2]tablepb.Span{
				{StartKey: []byte("t1_0")},
				{StartKey: []byte("t2_0")},
			},
			expectedFound: []tablepb.Span{
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			expectedHole: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_4")},
			},
		},
		{ // 5. end not found
			spans: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
			},
			rang: [2]tablepb.Span{
				{StartKey: []byte("t1_0")},
				{StartKey: []byte("t2_0")},
			},
			expectedFound: []tablepb.Span{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
			},
			expectedHole: []tablepb.Span{
				{StartKey: []byte("t1_1"), EndKey: []byte("t2_0")},
			},
		},
	}

	for i, cs := range cases {
		_, _ = i, cs
		m := NewBtreeMap[struct{}]()
		for _, span := range cs.spans {
			m.ReplaceOrInsert(span, struct{}{})
		}
		found, holes := m.FindHoles(cs.rang[0], cs.rang[1])
		require.Equalf(t, cs.expectedFound, found, "case %d, %#v", i, cs)
		require.Equalf(t, cs.expectedHole, holes, "case %d, %#v", i, cs)
	}
}
