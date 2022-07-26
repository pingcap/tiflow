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

	"github.com/edwingeng/deque"
	"github.com/stretchr/testify/require"
)

const (
	testCaseSize = 10007
)

func TestChunkQueueSimpleWorkflow(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()

	q.Enqueue(10)
	require.Equal(t, q.Size(), 1)
	v, ok := q.At(0)
	require.Equal(t, v, 10)
	require.True(t, ok)
	v, ok = q.Dequeue()
	require.Equal(t, v, 10)
	require.True(t, ok)

	adds := make([]int, 0, testCaseSize)
	// q.EnqueueMany(adds...)
	require.True(t, q.Empty())
	for i := 0; i < testCaseSize; i++ {
		adds = append(adds, i)
	}
	q.EnqueueMany(adds...)
	require.Equal(t, testCaseSize, q.Size(), q.Len())
	vals, ok := q.DequeueMany(testCaseSize * 3 / 4)
	require.True(t, ok)
	for i, v := range vals {
		require.Equal(t, adds[i], v)
	}
	require.Equal(t, testCaseSize-testCaseSize*3/4, q.Size())
	q.Clear()
	require.True(t, q.Empty())

	for i := 0; i < testCaseSize; i++ {
		q.Enqueue(i)
		require.Equal(t, i+1, q.Size())
	}

	require.Equal(t, testCaseSize, q.Size())
	require.False(t, q.Empty())
	for x := 0; x < 1000; x++ {
		i := rand.Intn(testCaseSize)
		v, ok = q.At(i)
		require.True(t, ok)
		it := q.GetIterator(i)
		itv := it.Value()
		require.Equal(t, it.Index(), itv, v)

		require.True(t, it.Replace(i+1))
		require.Equal(t, it.Value(), v+1)
		q.Replace(i, i)
		require.Equal(t, it.Value(), v)
	}

	tail, ok := q.Tail()
	require.Equal(t, tail, testCaseSize-1)
	require.True(t, ok)

	for i := 0; i < testCaseSize; i++ {
		h, ok := q.Head()
		require.Equal(t, i, h)
		require.True(t, ok)

		v, ok = q.Dequeue()
		require.True(t, ok)
		require.Equal(t, v, i)
	}

	require.True(t, q.Empty())
	require.Equal(t, 0, q.Size())
	_, ok = q.Dequeue()
	require.False(t, ok)
	_, ok = q.Head()
	require.False(t, ok)
	_, ok = q.Tail()
	require.False(t, ok)
}

func TestChunkQueueExpand(t *testing.T) {
	t.Parallel()

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

		p, ok := q.Dequeue()
		require.True(t, ok)
		require.Equal(t, p.no, i)
		require.Equal(t, p.name, fmt.Sprintf("test-name-%d", i))
		require.True(t, q.Empty())
	}
}

func TestChunkQueueDequeueMany(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	x := testCaseSize
	for v := 0; v < x; v++ {
		q.Enqueue(v)
	}
	f := 0
	for !q.Empty() {
		l := rand.Intn(q.Size()/5 + 1)
		if l == 0 {
			l = 1
		}
		vals, ok := q.DequeueMany(l)
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

func TestChunkQueueRange(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	for i := 0; i < testCaseSize; i++ {
		q.Enqueue(i)
	}

	var target int
	q.Range(func(v int) bool {
		if v >= 1000 {
			target = v
			return false
		}
		return true
	})
	require.Equal(t, 1000, target)

	q.RangeWithIndex(func(i int, v int) bool {
		require.Equal(t, i, v)
		return true
	})

	q.RangeAndPop(func(v int) bool {
		return v < 1000
	})

	require.Equal(t, testCaseSize-1000, q.Size())

	process := 0
	q.RangeAndPop(func(v int) bool {
		process = v
		return true
	})
	require.Equal(t, testCaseSize-1, process)
	require.True(t, q.Empty())

	require.NotPanics(t, func() {
		q.RangeAndPop(func(v int) bool {
			return true
		})
	})
}

func TestChunkQueueRangeAndPop(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	for i := 0; i < testCaseSize; i++ {
		q.Enqueue(i)
	}

	var target int
	q.Range(func(v int) bool {
		if v >= 1000 {
			target = v
			return false
		}
		return true
	})
	require.Equal(t, 1000, target)

	q.RangeWithIndex(func(i int, v int) bool {
		require.Equal(t, i, v)
		q.Dequeue()
		return true
	})
	require.True(t, q.Empty())
}

func BenchmarkEnqueue(b *testing.B) {
	b.Run("Benchmark-Enqueue-ChunkQueue", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Enqueue(i)
		}
	})

	b.Run("Benchmark-Enqueue-Slice", func(b *testing.B) {
		q := make([]int, 0, 1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q = append(q, i)
		}
	})

	b.Run("Benchmark-Enqueue-EdwingDeque", func(b *testing.B) {
		q := deque.NewDeque()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.PushBack(i)
		}
	})
}

func TestChunkQueueEnqueueMany(t *testing.T) {
	q := NewChunkQueueLeastCapacity[int](16)
	n := testCaseSize
	data := make([]int, 0, n)
	cnt := 0
	for i := 1; i < 10; i++ {
		q.EnqueueMany(data[:n]...)
		cnt += n
		freeSpace := q.Cap() - q.Size()
		fmt.Println(n, freeSpace)
		require.Equal(t, cnt, q.Size())
		require.True(t, freeSpace >= 0 && freeSpace <= q.chunkLength)
	}
}

func BenchmarkEnqueueMany(b *testing.B) {
	prepareSlice := func(n int) []int {
		data := make([]int, 0, n)
		for i := 0; i < n; i++ {
			data = append(data, i)
		}
		return data
	}

	b.Run("Benchmark-EnqueueMany-ChunkDeque", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](16)
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			q.EnqueueMany(data[:n]...)
		}
	})

	b.Run("Benchmark-EnqueueMany-ChunkDeque-OneByOne", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](16)
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			q.EnqueueManyOneByOne(data[:n]...)
		}
	})

	b.Run("BenchMark-EnqueueMany-Slice", func(b *testing.B) {
		q := make([]int, 0, 16)
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			q = append(q, data[:n]...)
		}
	})
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
			if !ok || len(vals) != l {
				panic("error")
			}
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

	b.Run("Benchmark-DequeueMany-EdwingDeque", func(b *testing.B) {
		q := deque.NewDeque()

		x := b.N
		for i := 0; i < x; i++ {
			q.Enqueue(i)
		}
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			if l > 0 {
				vals := q.PopManyFront(l)
				if len(vals) != l {
					fmt.Println(l, len(vals), vals[0])
					panic("error")
				}
			}
		}
	})
}
