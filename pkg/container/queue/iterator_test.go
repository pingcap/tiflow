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

package queue

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	iterTestSize = 10007
)

func TestChunkQueueIteratorPrevNext(t *testing.T) {
	t.Parallel()
	q := NewChunkQueue[int]()
	for i := 0; i < iterTestSize; i++ {
		q.PushBack(i)
	}

	var it *ChunkQueueIterator[int]
	i := 0
	for it = q.Begin(); it.Valid(); it.Next() {
		v := it.Value()
		require.Equal(t, i, it.Index())
		require.Equal(t, i, v)
		i++
	}
	i--
	for it = q.End().Prev(); it.Valid(); it.Prev() {
		v := it.Value()
		require.Equal(t, i, it.Index())
		require.Equal(t, i, v)
		i--
	}
}

func BenchmarkChunkQueueIteratorNext(b *testing.B) {
	b.Run("BenchMark-Iterate-ChunkQueue", func(b *testing.B) {
		q := NewChunkQueue[int]()
		n := b.N
		for i := 0; i < n; i++ {
			q.Enqueue(i)
		}
		b.ResetTimer()

		i := 0
		for it := q.Begin(); it.Valid(); it.Next() {
			v := it.Value()
			if v != i {
				panic("not equal")
			}
			i++
		}
	})

	b.Run("BenchMark-Iterate-Slice", func(b *testing.B) {
		n := b.N
		q := make([]int, n, n)
		for i := 0; i < n; i++ {
			q[i] = i
		}
		b.ResetTimer()

		for i := 0; i < n; i++ {
			if q[i] != i {
				panic("error")
			}
		}
	})
}

func TestChunkQueueGetIterator(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()

	for i := 0; i < iterTestSize; i++ {
		q.PushBack(i)
	}
	var it *ChunkQueueIterator[int]
	it = q.GetIterator(-1)
	require.Nil(t, it)
	it = q.GetIterator(iterTestSize)
	require.Nil(t, it)

	require.True(t, q.End().Index() < 0)
	require.True(t, q.Begin().Prev().Index() < 0)

	for i := 0; i < iterTestSize; i++ {
		it = q.GetIterator(i)
		p1 := it.Value()
		require.Equal(t, i, it.Index())
		require.Equal(t, i, p1)

		p2, ok := q.At(i)
		require.True(t, ok)
		require.Equal(t, p1, p2)
	}

	cnt := 0
	for !q.Empty() {
		n := rand.Intn(q.Size())
		if n == 0 {
			n = testCaseSize/20 + 1
		}
		it := q.Begin()
		require.NotNil(t, it)
		require.True(t, it.Valid())

		q.DequeueMany(n)
		require.Equal(t, -1, it.Index())
		require.False(t, it.Valid())

		require.Nil(t, q.Begin().chunk.prevCk)
		cnt += n
		v := q.Begin().Value()
		if cnt >= iterTestSize {
			require.True(t, !it.Valid())
		} else {
			require.Equal(t, cnt, v)
		}
	}
}

// todo: add more random tests
