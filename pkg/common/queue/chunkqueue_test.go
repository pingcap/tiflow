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
	testCaseSize = 1 << 20
)

func BenchmarkEnqueue(b *testing.B) {
	q := NewChunkQueue[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for v := 0; v < testCaseSize; v++ {
			_ = q.PushBack(v)
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := NewChunkQueue[int]()
	for v := 0; v < b.N; v++ {
		_ = q.PushBack(v)
	}
	b.ResetTimer()
	for v := 0; v < b.N; v++ {
		_, _ = q.PopFront()
	}
}

func BenchmarkDequeueMany(b *testing.B) {
	q := NewChunkQueueLeastCapacity[int](16)
	b.ResetTimer()
	x := b.N
	for v := 0; v < x; v++ {
		_ = q.PushBack(v)
	}
	ls := []int{x / 4, x / 4, x / 4, x - x/4*3}

	for _, l := range ls {
		_, err := q.PopFrontMany(l)
		require.Nil(b, err)
	}
}

func BenchmarkCompareToSlice(b *testing.B) {
	q := make([]int, 0, 16)
	b.ResetTimer()

	x := b.N
	for v := 0; v < x; v++ {
		q = append(q, v)
	}
	ls := []int{x / 4, x / 4, x / 4, x - x/4*3}

	for _, l := range ls {
		_ = q[:l]
		q = append(make([]int, 0, len(q[l:])), q[l:]...)
	}
}

func TestChunkQueue(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()

	err := q.PushBack(10)
	require.NoError(t, err)
	require.Equal(t, q.Size(), 1)
	ptr, ok := q.At(0)
	require.Equal(t, *ptr, 10)
	require.True(t, ok)
	v, err := q.PopFront()
	require.Equal(t, v, 10)
	require.NoError(t, err)

	for i := 0; i < testCaseSize; i++ {
		err = q.PushBack(i)
		require.NoError(t, err)
		require.Equal(t, i+1, q.Size())
	}

	require.Equal(t, testCaseSize, q.Size())
	vals, err := q.PopFrontMany(100)
	require.NoError(t, err)
	for i, v := range vals {
		require.Equal(t, i, v)
	}
	require.Equal(t, testCaseSize-100, q.Size())

	for i := 0; i < 1000; i++ {
		x := rand.Intn(testCaseSize - 100)
		ptr, ok = q.At(x)
		require.Equal(t, x+100, *ptr)
		require.True(t, ok)
	}

	for i := 100; i < testCaseSize; i++ {
		v, err = q.PopFront()
		require.NoError(t, err)
		require.Equal(t, v, i)
	}

	require.Equal(t, 0, q.Size())
	_, err = q.PopFront()
	require.Error(t, err)

	v2 := []int{1, 2, 3, 4, 5}
	err = q.PushBackMany(v2...)
	require.NoError(t, err)

	_, err = q.PopFrontMany(len(v2) + 1)
	require.Error(t, err)

	vals, err = q.PopFrontMany(len(v2))
	require.NoError(t, err)
	for i, v := range vals {
		require.Equal(t, v2[i], v)
	}
}

func TestChunkQueueDequeueMany(t *testing.T) {
	q2 := NewChunkQueue[int]()
	x := testCaseSize + 100
	for v := 0; v < x; v++ {
		_ = q2.PushBack(v + 1)
	}
	popLen := []int{x / 4, x / 4, x / 4, x - x/4*3}

	f := 1
	for _, l := range popLen {
		vals, err := q2.PopFrontMany(l)
		require.NoError(t, err)
		for i := 0; i < l; i++ {
			require.Equal(t, f+i, vals[i])
		}
		f += l
	}
}

// todo
func TestChunkQueueIterator(t *testing.T) {}
