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
	"sync"
	"unsafe"

	"github.com/pingcap/log"
)

const (
	// the size of each chunk is 1024 bytes (1Kb) by default
	defaultSizePerChunk = 1024
	// the minimum length of each chunk is 16
	minimumChunkLen      = 16
	initialPitchArrayLen = 32
)

// ChunkQueue is a generic, efficient and GC-friendly queue
type ChunkQueue[T any] struct {
	// [head, tail) is the section of chunks in use
	head int
	tail int

	// size is number of elements in queue
	size int

	// chunks is an array storing ptr
	chunks []*chunk[T]
	// chunkSize is the max number of elements stored in every chunk
	chunkSize    int
	chunkPool    sync.Pool
	defaultValue T
}

func (q *ChunkQueue[T]) firstChunk() *chunk[T] {
	return q.chunks[q.head]
}

func (q *ChunkQueue[T]) lastChunk() *chunk[T] {
	return q.chunks[q.tail-1]
}

// NewChunkQueue creates a new ChunkQueue
func NewChunkQueue[T any]() *ChunkQueue[T] {
	return NewChunkQueueLeastCapacity[T](1)
}

// NewChunkQueueLeastCapacity creates a ChunkQueue with an argument minCapacity.
// It requests that the queue capacity be at least minCapacity. And it's similar
// to the cap argument when making a slice using make([]T, len, cap)
func NewChunkQueueLeastCapacity[T any](minCapacity int) *ChunkQueue[T] {
	if unsafe.Sizeof(*new(T)) == 0 {
		log.Error("Cannot create a queue of type")
		return nil
	}

	chunkSize := defaultSizePerChunk / int(unsafe.Sizeof(*new(T)))
	if chunkSize < minimumChunkLen {
		chunkSize = minimumChunkLen
	}

	q := &ChunkQueue[T]{
		head:      0,
		tail:      0,
		size:      0,
		chunkSize: chunkSize,
	}
	q.chunkPool = sync.Pool{
		New: func() any {
			return newChunk[T](q.chunkSize, q)
		},
	}

	q.chunks = make([]*chunk[T], initialPitchArrayLen, initialPitchArrayLen)
	q.expandSpace(minCapacity)
	return q
}

// Size() returns the number of elements in queue
func (q *ChunkQueue[T]) Size() int {
	return q.size
}

// Cap() returns the capacity of the queue. The queue can hold more elements
// than that number by automatic expansion
func (q *ChunkQueue[T]) Cap() int {
	return q.chunkSize*(q.tail-q.head) - q.chunks[q.head].l
}

// Empty() indicates whether the queue is empty
func (q *ChunkQueue[T]) Empty() bool {
	return q.size == 0
}

// Getters

// At() returns the pointer to an element
func (q *ChunkQueue[T]) At(idx int) (*T, bool) {
	if idx < 0 || idx >= q.size {
		return new(T), false
	}
	i := q.chunks[q.head].l + idx
	return &q.chunks[q.head+i/q.chunkSize].data[i%q.chunkSize], true
}

// Head() returns the pointer to the first element in queue and nil if empty
func (q *ChunkQueue[T]) Head() (*T, bool) {
	if q.Empty() {
		return nil, false
	}
	c := q.firstChunk()
	return &c.data[c.l], true
}

// Tail() returns the pointer to the last element in queue and nil if empty
func (q *ChunkQueue[T]) Tail() (*T, bool) {
	if q.Empty() {
		return nil, false
	}
	c := q.lastChunk()
	return &c.data[c.r-1], true
}

// expandSpace extends the space by adding chunk(s) to the queue
func (q *ChunkQueue[T]) expandSpace(n int) {
	if n <= 0 {
		n = 1
	}
	chunksNum := (n + q.chunkSize - 1) / q.chunkSize

	if len(q.chunks)-q.tail-chunksNum <= 1 {
		q.expandPitch(chunksNum)
	}

	for i := 0; i < chunksNum; i++ {
		c := q.chunkPool.Get().(*chunk[T])
		c.queue = q
		q.chunks[q.tail] = c
		if q.tail > q.head {
			c.prevCk = q.chunks[q.tail-1]
			q.chunks[q.tail-1].nextCk = c
		}
		q.tail++
	}
}

// expandPitch extends the []chunks array in which there are pointers to chunks
func (q *ChunkQueue[T]) expandPitch(x int) {
	n := len(q.chunks)
	used := q.tail - q.head
	for {
		if n < 1024 {
			n *= 2
		} else {
			n += n / 4
		}
		if n-used > x {
			break
		}
	}
	newChunks := make([]*chunk[T], n, n)
	copy(newChunks[:used], q.chunks[q.head:q.tail])
	q.tail -= q.head
	q.head = 0
	q.chunks = newChunks
}

// PushBack() pushes an element to tail
func (q *ChunkQueue[T]) Enqueue(v T) {
	c := q.lastChunk()
	if c.r == q.chunkSize {
		q.expandSpace(1)
		c = q.lastChunk()
	}

	c.data[c.r] = v
	c.r++
	q.size++
}

// PopFront() pops an element from head
func (q *ChunkQueue[T]) Dequeue() (T, bool) {
	if q.Empty() {
		return *new(T), false
	}

	c := q.firstChunk()
	v := c.data[c.l]
	c.data[c.l] = q.defaultValue
	c.l++
	q.size--

	if c.l == q.chunkSize {
		q.popChunk()
	}
	return v, true
}

func (q *ChunkQueue[T]) popChunk() {
	c := q.firstChunk()
	if c.nextCk == nil {
		q.expandSpace(1)
	}
	q.chunks[q.head] = nil
	q.head++
	q.chunks[q.head].prevCk = nil

	c.reset()
	q.chunkPool.Put(c)
}

// Enqueue() enques a single element to the tail
func (q *ChunkQueue[T]) PushBack(v T) {
	q.Enqueue(v)
}

// Dequeue() deques a single element from the head
func (q *ChunkQueue[T]) PopFront() (T, bool) {
	return q.Dequeue()
}

// PushBackMany() pushes multiple elements to the tail at a time
func (q *ChunkQueue[T]) PushBackMany(vals ...T) {
	q.EnqueueMany(vals...)
}

// EnqueueMany() enqueues multiple elements at a time
func (q *ChunkQueue[T]) EnqueueMany(vals ...T) {
	if q.Cap()-q.Size() < len(vals) {
		q.expandSpace(len(vals))
	}

	for _, val := range vals {
		q.Enqueue(val)
	}
}

// PopFrontMany() deques n elements from the head.
func (q *ChunkQueue[T]) PopFrontMany(n int) ([]T, bool) {
	return q.DequeueMany(n)
}

// DequeueMany() deques n elements from the head.
func (q *ChunkQueue[T]) DequeueMany(n int) ([]T, bool) {
	if n < 0 {
		return nil, false
	}

	ok := n <= q.size
	if q.size < n {
		n = q.size
	}

	res := make([]T, n, n)
	cnt := 0
	//emptyChunk := make([]T, q.chunkSize, q.chunkSize)
	for i := q.head; i < q.tail && cnt < n; i++ {
		c := q.chunks[i]
		popLen := c.len()
		if n-cnt < popLen {
			popLen = n - cnt
		}
		//copy(res[cnt:cnt+popLen], c.data[c.l:c.l+popLen])
		//copy(c.data[c.l:c.l+popLen], emptyChunk[:popLen])
		for j := 0; j < popLen; j++ {
			res[cnt+j] = c.data[c.l+j]
			c.data[c.l+j] = q.defaultValue
		}
		c.l += popLen
		cnt += popLen
		q.size -= popLen

		if c.l == q.chunkSize {
			q.popChunk()
		}
	}
	return res, ok
}

func (q *ChunkQueue[T]) Clear() {
	if q.Empty() {
		return
	}

	emptyChunk := make([]T, q.chunkSize, q.chunkSize)
	for i := q.head; i < q.tail; i++ {
		q.size -= q.chunks[i].len()
		copy(q.chunks[i].data[:], emptyChunk[:])
		q.popChunk()
	}
}
