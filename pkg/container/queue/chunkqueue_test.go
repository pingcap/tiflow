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
	"github.com/labstack/gommon/random"
	"github.com/stretchr/testify/require"
)

const (
	testCaseSize = 10007
)

func TestChunkQueueCommon(t *testing.T) {
	t.Parallel()

	type ZeroSizeType struct{}
	require.Nil(t, NewChunkQueue[ZeroSizeType]())

	q := NewChunkQueue[int]()

	// simple enqueue dequeue
	x := rand.Int()
	q.Enqueue(x)
	require.Equal(t, q.Len(), 1)
	v, ok := q.At(0)
	require.Equal(t, v, x)
	require.True(t, ok)
	v, ok = q.Dequeue()
	require.Equal(t, v, x)
	require.True(t, ok)

	// EnqueueMany & DequeueMany
	elements := make([]int, 0, testCaseSize)
	require.True(t, q.Empty())
	for i := 0; i < testCaseSize; i++ {
		elements = append(elements, i)
	}
	q.EnqueueMany(elements...)
	require.Equal(t, testCaseSize, q.Len(), q.Len())
	vals, ok := q.DequeueMany(testCaseSize * 3 / 4)
	require.True(t, ok)
	for i, v := range vals {
		require.Equal(t, elements[i], v)
	}
	require.Equal(t, testCaseSize-testCaseSize*3/4, q.Len())
	// Clear
	q.Clear()
	require.True(t, q.Empty())

	// Element Access
	q.EnqueueMany(elements...)
	require.Equal(t, testCaseSize, q.Len())
	require.False(t, q.Empty())
	for j := 0; j < testCaseSize; j++ {
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
	_, ok = q.At(-1)
	require.False(t, ok)
	ok = q.Replace(testCaseSize, 0)
	require.False(t, ok)

	tail, ok := q.Tail()
	require.Equal(t, tail, testCaseSize-1)
	require.True(t, ok)

	// Dequeue one by one
	for i := 0; i < testCaseSize; i++ {
		h, ok := q.Head()
		require.Equal(t, i, h)
		require.True(t, ok)
		v, ok = q.Dequeue()
		require.True(t, ok)
		require.Equal(t, v, i)
	}

	// EmptyOperation
	require.True(t, q.Empty())
	require.Equal(t, 0, q.Len())
	_, ok = q.Dequeue()
	require.False(t, ok)
	_, ok = q.Head()
	require.False(t, ok)
	_, ok = q.Tail()
	require.False(t, ok)
}

func TestChunkQueueRandom(t *testing.T) {
	t.Parallel()

	getInt := func() int {
		return rand.Int()
	}
	doRandomTest[int](t, getInt)
	getFloat := func() float64 {
		return rand.Float64() + rand.Float64()
	}

	doRandomTest[float64](t, getFloat)

	getBool := func() bool {
		return rand.Int()%2 == 1
	}
	doRandomTest[bool](t, getBool)

	getString := func() string {
		return random.String(16)
	}
	doRandomTest[string](t, getString)

	type MyType struct {
		x int
		y string
	}
	getMyType := func() MyType {
		return MyType{1, random.String(64)}
	}

	doRandomTest[MyType](t, getMyType)

	getMyTypePtr := func() *MyType {
		return &MyType{1, random.String(64)}
	}

	doRandomTest[*MyType](t, getMyTypePtr)
}

func doRandomTest[T comparable](t *testing.T, getVal func() T) {
	const (
		opEnqueueOne = iota
		opEnqueueMany
		opDequeueOne
		opDequeueMany
		opDequeueAll
	)

	q := NewChunkQueue[T]()
	slice := make([]T, 0, 100)
	var val T
	for i := 0; i < 100; i++ {
		op := rand.Intn(4)
		if i == 99 {
			op = opDequeueAll
		}
		switch op {
		case opEnqueueOne:
			val = getVal()
			q.Enqueue(val)
			slice = append(slice, val)
		case opEnqueueMany:
			n := rand.Intn(1024) + 1
			vals := make([]T, n, n)
			for j := 0; j < n; j++ {
				vals = append(vals, getVal())
			}
			q.EnqueueMany(vals...)
			slice = append(slice, vals...)
		case opDequeueOne:
			if q.Empty() {
				require.Equal(t, 0, q.Len(), len(slice))
				_, ok := q.Dequeue()
				require.False(t, ok)
			} else {
				v, ok := q.Dequeue()
				require.True(t, ok)
				require.Equal(t, slice[0], v)
				slice = slice[1:]
			}
		case opDequeueMany:
			if q.Empty() {
				require.True(t, len(slice) == 0)
			} else {
				n := rand.Intn(q.Len()) + 1
				pops, ok := q.DequeueMany(n)
				require.True(t, ok)
				require.Equal(t, n, len(pops))
				popSlice := slice[0:n]
				slice = append(make([]T, 0, len(slice[n:])), slice[n:]...)

				for i := 0; i < len(pops); i++ {
					require.Equal(t, popSlice[i], pops[i])
				}
			}
		case opDequeueAll:
			pops, ok := q.DequeueAll()
			require.True(t, ok)
			require.Equal(t, len(pops), len(slice))
			for i := 0; i < len(pops); i++ {
				require.Equal(t, slice[i], pops[i])
			}
			slice = slice[:0]
			q.Clear()
		}

		require.Equal(t, q.Len(), len(slice))
		require.Nil(t, q.firstChunk().prev)
		require.Nil(t, q.lastChunk().next)
		freeSpace := q.Cap() - q.Len()
		if q.Empty() {
			require.True(t, freeSpace > 0 && freeSpace <= q.chunkLength)
		} else {
			require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		}
	}
}

func TestExpand(t *testing.T) {
	t.Parallel()
	q := NewChunkQueue[int]()

	for i := 0; i < testCaseSize; i++ {
		q.Enqueue(1)
		require.Equal(t, 1, q.Len())
		freeSpace := q.Cap() - q.Len()
		require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		p, ok := q.Dequeue()
		require.True(t, ok)
		require.Equal(t, 1, p)
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

	var x int
	q.Range(func(v int) bool {
		if v >= 1000 {
			return false
		}
		x++
		return true
	})
	require.Equal(t, 1000, x)

	q.RangeWithIndex(func(i int, v int) bool {
		require.Equal(t, i, v)
		if i >= 1000 {
			return false
		}
		x++
		return true
	})
	require.Equal(t, 2000, x)

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
