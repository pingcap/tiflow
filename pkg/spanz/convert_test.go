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

func TestHashableSpan(t *testing.T) {
	t.Parallel()

	// Make sure it can be a map key.
	m := make(map[hashableSpan]int)
	m[hashableSpan{}] = 1
	require.Equal(t, 1, m[hashableSpan{}])

	span := toHashableSpan(TableIDToComparableSpan(1))
	require.EqualValues(t, TableIDToComparableSpan(1), span.toSpan())
}

func TestHashableSpanHeapAlloc(t *testing.T) {
	span := tablepb.Span{TableID: 1}
	for i := 0; i < 10; i++ {
		span.StartKey = append(span.StartKey, byte(i))
		span.EndKey = append(span.EndKey, byte(i))
	}

	n := 0
	results := testing.Benchmark(func(b *testing.B) {
		n = b.N
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hspan := toHashableSpan(span)
			span = hspan.toSpan()
		}
	})
	require.EqualValues(t, 0, results.MemAllocs/uint64(n))
}

func TestUnsafeStringByte(t *testing.T) {
	b := []byte("unsafe bytes")
	s := "unsafe bytes"

	us := unsafeBytesToString(b)
	require.EqualValues(t, s, us)
	require.EqualValues(t, len(b), len(us))

	ub := unsafeStringToBytes(s)
	require.EqualValues(t, b, ub)
	require.EqualValues(t, len(s), len(ub))
	require.EqualValues(t, len(s), cap(ub))
}

func TestHexKey(t *testing.T) {
	span := TableIDToComparableSpan(8616)
	require.Equal(t, "7480000000000021FFA85F720000000000FA", HexKey(span.StartKey))
	require.Equal(t, "7480000000000021FFA85F730000000000FA", HexKey(span.EndKey))
}
