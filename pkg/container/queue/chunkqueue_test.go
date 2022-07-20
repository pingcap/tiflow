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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testCaseSize = 10 << 20
)

func BenchmarkEnqueue(b *testing.B) {
	b.Run("Benchmark-Enqueue-ChunkQueue", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Enqueue(i)
		}
	})

	b.Run("Benchmark-Enqueue-Slice", func(b *testing.B) {
		q := make([]int, 1024, 1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q = append(q, i)
		}
	})
}

func BenchmarkDequeue(b *testing.B) {
	q := NewChunkQueue[int]()
	for v := 0; v < b.N; v++ {
		q.Enqueue(v)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PopFront()
	}
}

func BenchmarkDequeueMany(b *testing.B) {
	b.Run("Benchmark-DequeueMany-ChunkDeque", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](16)

		x := b.N
		for i := 0; i < x; i++ {
			q.Enqueue(i)
		}
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			vals, ok := q.DequeueMany(l)
			require.True(b, ok)
			require.Equal(b, len(vals), l)
		}
	})

	b.Run("BenchMark-DequeueMany-Slice", func(b *testing.B) {
		q := make([]int, 0, 16)

		x := b.N
		for i := 0; i < x; i++ {
			q = append(q, i)
		}
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			_ = q[:l]
			q = append(make([]int, 0, len(q[l:])), q[l:]...)
		}
	})
}

func TestChunkQueueSimpleWorkflow(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()

	q.PushBack(10)
	require.Equal(t, q.Size(), 1)
	ptr, ok := q.At(0)
	require.Equal(t, *ptr, 10)
	require.True(t, ok)
	v, ok := q.PopFront()
	require.Equal(t, v, 10)
	require.True(t, ok)

	adds := make([]int, 0, testCaseSize)
	q.EnqueueMany(adds...)
	require.True(t, q.Empty())
	for i := 0; i < testCaseSize; i++ {
		adds = append(adds, i)
	}
	q.EnqueueMany(adds...)
	require.Equal(t, testCaseSize, q.Size())
	vals, ok := q.DequeueMany(testCaseSize * 3 / 4)
	require.True(t, ok)
	for i, v := range vals {
		require.Equal(t, adds[i], v)
	}
	require.Equal(t, testCaseSize-testCaseSize*3/4, q.Size())
	q.Clear()
	require.True(t, q.Empty())

	for i := 0; i < testCaseSize; i++ {
		q.PushBack(i)
		require.Equal(t, i+1, q.Size())
	}

	require.Equal(t, testCaseSize, q.Size())
	require.False(t, q.Empty())
	for i := 0; i < 1000; i++ {
		x := rand.Intn(testCaseSize)
		ptr, ok = q.At(x)
		require.Equal(t, x, *ptr)
		require.True(t, ok)
	}

	for i := 0; i < testCaseSize; i++ {
		v, ok = q.PopFront()
		require.True(t, ok)
		require.Equal(t, v, i)
	}

	require.True(t, q.Empty())
	require.Equal(t, 0, q.Size())
	_, ok = q.PopFront()
	require.False(t, ok)
}

func TestChunkQueueExpand(t *testing.T) {
	type Person struct {
		no   int
		name string
	}

	q := NewChunkQueue[*Person]()

	for i := 0; i < testCaseSize; i++ {
		str := fmt.Sprintf("test-name-%d", i)
		q.Enqueue(&Person{
			no:   i,
			name: str,
		})
		require.Equal(t, 1, q.Size())

		freeSpace := q.Cap() - q.Size()
		if q.Empty() {
			require.True(t, freeSpace > 0 && freeSpace <= q.chunkLength)
		} else {
			require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		}

		p, ok := q.PopFront()
		require.True(t, ok)
		require.Equal(t, p.no, i)
		require.Equal(t, p.name, fmt.Sprintf("test-name-%d", i))
		require.True(t, q.Empty())
	}
}

func TestChunkQueueDequeueMany(t *testing.T) {
	q := NewChunkQueue[int]()
	x := testCaseSize
	for v := 0; v < x; v++ {
		q.PushBack(v)
	}
	f := 0
	for !q.Empty() {
		l := rand.Intn(q.Size()/5 + 1)
		if l == 0 {
			l = 1
		}
		vals, ok := q.PopFrontMany(l)
		require.True(t, ok)
		for i := 0; i < l; i++ {
			require.Equal(t, f+i, vals[i])
		}
		f += len(vals)
		require.True(t, len(vals) > 0 && len(vals) <= l)

		freeSpace := q.Cap() - q.Size()
		if q.Empty() {
			require.True(t, freeSpace > 0 && freeSpace <= q.chunkLength)
		} else {
			require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		}
	}
	require.Equal(t, f, testCaseSize)
	require.True(t, q.Empty())
}
