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

func TestChunkQueueCommon(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()

	q.Enqueue(10)
	require.Equal(t, q.Len(), 1)
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
	require.Equal(t, testCaseSize, q.Len(), q.Len())
	vals, ok := q.DequeueMany(testCaseSize * 3 / 4)
	require.True(t, ok)
	for i, v := range vals {
		require.Equal(t, adds[i], v)
	}
	require.Equal(t, testCaseSize-testCaseSize*3/4, q.Len())
	q.Clear()
	require.True(t, q.Empty())

	for i := 0; i < testCaseSize; i++ {
		q.Enqueue(i)
		require.Equal(t, i+1, q.Len())
	}

	require.Equal(t, testCaseSize, q.Len())
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
	require.Equal(t, 0, q.Len())
	_, ok = q.Dequeue()
	require.False(t, ok)
	_, ok = q.Head()
	require.False(t, ok)
	_, ok = q.Tail()
	require.False(t, ok)
}

func TestExpand(t *testing.T) {
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
		require.Equal(t, 1, q.Len())

		freeSpace := q.Cap() - q.Len()
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

func TestDequeueMany(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	x := testCaseSize
	for v := 0; v < x; v++ {
		q.Enqueue(v)
	}
	f := 0
	for !q.Empty() {
		l := rand.Intn(q.Len()/5 + 1)
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

		freeSpace := q.Cap() - q.Len()
		if q.Empty() {
			require.True(t, freeSpace > 0 && freeSpace <= q.chunkLength)
		} else {
			require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		}
	}
	require.Equal(t, f, testCaseSize)
	require.True(t, q.Empty())
}

func TestRange(t *testing.T) {
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

	require.Equal(t, testCaseSize-1000, q.Len())

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

func TestRangeAndPop(t *testing.T) {
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

	b.Run("Benchmark-Enqueue-EdwingengDeque", func(b *testing.B) {
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
		freeSpace := q.Cap() - q.Len()
		require.Equal(t, cnt, q.Len())
		require.True(t, freeSpace >= 0 && freeSpace <= q.chunkLength)
	}
}

func prepareSlice(n int) []int {
	data := make([]int, 0, n)
	for i := 0; i < n; i++ {
		data = append(data, i)
	}
	return data
}

func prepareChunkQueue(n int) *ChunkQueue[int] {
	q := NewChunkQueue[int]()
	for i := 0; i < n; i++ {
		q.Enqueue(i)
	}
	return q
}

func BenchmarkEnqueueMany(b *testing.B) {
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
			for _, v := range data {
				q.Enqueue(v)
			}
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

	b.Run("BenchMark-EnqueueMany-EdwingengDeque", func(b *testing.B) {
		q := deque.NewDeque()
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			for _, v := range data {
				q.Enqueue(v)
			}
		}
	})
}

func BenchmarkDequeueMany(b *testing.B) {
	b.Run("Benchmark-DequeueMany-ChunkDeque", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
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
		x := b.N
		q := prepareSlice(x)
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			vals := q[:l]
			if len(vals) != l {
				panic("error")
			}
			q = append(make([]int, 0, len(q[l:])), q[l:]...)
		}
	})

	b.Run("Benchmark-DequeueMany-EdwingengDeque", func(b *testing.B) {
		x := b.N
		q := deque.NewDeque()
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

func BenchmarkRangeAndPop(b *testing.B) {
	b.Run("Benchmark-DequeueMany-RangeAndPop", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		b.ResetTimer()

		q.RangeAndPop(func(val int) bool {
			return val >= 0
		})

		require.True(b, q.Empty())
	})

	b.Run("Benchmark-DequeueMany-IterateAndDequeue", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		b.ResetTimer()

		for it := q.Begin(); it.Valid(); {
			if it.Value() >= 0 {
				it.Next()
				q.Dequeue()
			} else {
				break
			}
		}
		require.True(b, q.Empty())
	})

	b.Run("Benchmark-DequeueMany-CheckAndDequeue", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		b.ResetTimer()

		q.RangeAndPop(func(val int) bool {
			return val < 0
		})
		for i := 0; i < x; i++ {
			v, _ := q.Head()
			if v < 0 {
				break
			}
			q.Dequeue()
		}
		require.True(b, q.Empty())
	})
}
