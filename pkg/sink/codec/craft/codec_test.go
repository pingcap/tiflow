// Copyright 2021 PingCAP, Inc.
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

package craft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

func TestSizeTable(t *testing.T) {
	t.Parallel()

	tables := [][]int64{
		{
			1, 3, 5, 7, 9,
		},
		{
			2, 4, 6, 8, 10,
		},
	}
	bits := make([]byte, 16)
	rand.Read(bits)
	bits = encodeSizeTables(bits, tables)

	size, decoded, err := decodeSizeTables(bits, NewSliceAllocator(64))
	require.Nil(t, err)
	require.Equal(t, tables, decoded)
	require.Equal(t, len(bits)-16, size)
}

func TestUvarintReverse(t *testing.T) {
	t.Parallel()

	var i uint64 = 0

	for i < 0x8000000000000000 {
		bits := make([]byte, 16)
		rand.Read(bits)
		bits, bytes1 := encodeUvarintReversed(bits, i)
		bytes2, u64, err := decodeUvarintReversed(bits)
		require.Nil(t, err)
		require.Equal(t, i, u64)
		require.Equal(t, len(bits)-16, bytes1)
		require.Equal(t, bytes2, bytes1)
		if i == 0 {
			i = 1
		} else {
			i <<= 1
		}
	}
}

func newNullableString(a string) *string {
	return &a
}

func TestEncodeChunk(t *testing.T) {
	t.Parallel()

	stringChunk := []string{"a", "b", "c"}
	nullableStringChunk := []*string{newNullableString("a"), newNullableString("b"), newNullableString("c")}
	int64Chunk := []int64{1, 2, 3}
	allocator := NewSliceAllocator(64)

	bits := encodeStringChunk(nil, stringChunk)
	bits, decodedStringChunk, err := decodeStringChunk(bits, 3, allocator)
	require.Nil(t, err)
	require.Equal(t, 0, len(bits))
	require.Equal(t, stringChunk, decodedStringChunk)

	bits = encodeNullableStringChunk(nil, nullableStringChunk)
	bits, decodedNullableStringChunk, err := decodeNullableStringChunk(bits, 3, allocator)
	require.Nil(t, err)
	require.Equal(t, 0, len(bits))
	require.Equal(t, nullableStringChunk, decodedNullableStringChunk)

	bits = encodeVarintChunk(nil, int64Chunk)
	bits, decodedVarintChunk, err := decodeVarintChunk(bits, 3, allocator)
	require.Nil(t, err)
	require.Equal(t, 0, len(bits))
	require.Equal(t, int64Chunk, decodedVarintChunk)
}
