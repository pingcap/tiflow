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
	// the size of each chunk is 1024 bytes (1kB) by default
	defaultSizePerChunk = 1024
	// the minimum length of each chunk is 16
	minimumChunkLen      = 16
	defaultPitchArrayLen = 16
)

// ChunkQueue is a generic, efficient, iterable and GC-friendly queue.
// Attention, it's not thread-safe.
type ChunkQueue[T any] struct {
	// [head, tail) is the section of chunks in use
	head int
	tail int

	// size is number of elements in queue
	size int

	// chunks is an array storing ptr
	chunks []*chunk[T]
	// chunkLength is the max number of elements stored in every chunk
	chunkLength  int
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
	elementSize := unsafe.Sizeof(*new(T))
	if elementSize == 0 {
		log.Error("Cannot create a queue of type")
		return nil
	}

	chunkLength := int(defaultSizePerChunk / elementSize)
	if chunkLength < minimumChunkLen {
		chunkLength = minimumChunkLen
	}

	q := &ChunkQueue[T]{
		head:        0,
		tail:        0,
		size:        0,
		chunkLength: chunkLength,
	}
	q.chunkPool = sync.Pool{
		New: func() any {
			return newChunk[T](q.chunkLength, q)
		},
	}

	q.chunks = make([]*chunk[T], defaultPitchArrayLen, defaultPitchArrayLen)
	q.extend(minCapacity)
	return q
}

// Size returns the number of elements in queue
func (q *ChunkQueue[T]) Size() int {
	return q.size
}

// Cap returns the capacity of the queue. The queue can hold more elements
// than that number by automatic expansion
func (q *ChunkQueue[T]) Cap() int {
	return q.chunkLength*(q.tail-q.head) - q.chunks[q.head].l
}

// Empty indicates whether the queue is empty
func (q *ChunkQueue[T]) Empty() bool {
	return q.size == 0
}

// At returns the value of a given index. At() does NOT support modifying the value
func (q *ChunkQueue[T]) At(idx int) (T, bool) {
	if idx < 0 || idx >= q.size {
		return q.defaultValue, false
	}
	i := q.chunks[q.head].l + idx
	return q.chunks[q.head+i/q.chunkLength].data[i%q.chunkLength], true
}

// Replace assigns a new value to a given index
func (q *ChunkQueue[T]) Replace(idx int, val T) bool {
	if idx < 0 || idx >= q.size {
		return false
	}
	i := q.chunks[q.head].l + idx
	q.chunks[q.head+i/q.chunkLength].data[i%q.chunkLength] = val
	return true
}

// Head returns the value of the first element in queue
func (q *ChunkQueue[T]) Head() (T, bool) {
	if q.Empty() {
		return q.defaultValue, false
	}
	c := q.firstChunk()
	return c.data[c.l], true
}

// Tail returns the value of the last element in queue
func (q *ChunkQueue[T]) Tail() (T, bool) {
	if q.Empty() {
		return q.defaultValue, false
	}
	c := q.lastChunk()
	return c.data[c.r-1], true
}

// extend extends the space by adding chunk(s) to the queue
func (q *ChunkQueue[T]) extend(n int) {
	if n <= 0 {
		n = 1
	}
	chunksNum := (n + q.chunkLength - 1) / q.chunkLength

	// should reallocate the chunks pointers array if the tail cannot hold
	if q.tail+chunksNum+1 >= len(q.chunks) {
		q.reallocateChunksArray(chunksNum)
	}

	for i := 0; i < chunksNum; i++ {
		c := q.chunkPool.Get().(*chunk[T])
		c.queue = q
		q.chunks[q.tail] = c
		if q.tail > q.head {
			c.prev = q.chunks[q.tail-1]
			q.chunks[q.tail-1].next = c
		}
		q.tail++
	}
}

// reallocateChunksArray extends/shrinks the []chunks array,
// and move the pointers to head
func (q *ChunkQueue[T]) reallocateChunksArray(need int) {
	var n int
	if need < 0 {
		n = defaultPitchArrayLen
	} else {
		n = len(q.chunks)
	}
	used := q.tail - q.head
	// Twice the array if more than a half will be in use
	for used+need > n/2 {
		n *= 2
	}
	if n != len(q.chunks) {
		newChunks := make([]*chunk[T], n, n)
		copy(newChunks[:used], q.chunks[q.head:q.tail])
		q.chunks = newChunks
	} else if q.head > 0 {
		copy(q.chunks[:used], q.chunks[q.head:q.tail])
		for i := used; i < q.tail; i++ {
			q.chunks[i] = nil
		}
	}
	q.tail -= q.head
	q.head = 0
}

// Enqueue enqueues an element to tail
func (q *ChunkQueue[T]) Enqueue(v T) {
	c := q.lastChunk()
	if c.r == q.chunkLength {
		q.extend(1)
		c = q.lastChunk()
	}

	c.data[c.r] = v
	c.r++
	q.size++
}

// Dequeue dequeues an element from head
func (q *ChunkQueue[T]) Dequeue() (T, bool) {
	if q.Empty() {
		return q.defaultValue, false
	}

	c := q.firstChunk()
	v := c.data[c.l]
	c.data[c.l] = q.defaultValue
	c.l++
	q.size--

	if c.l == q.chunkLength {
		q.popChunk()
	}
	return v, true
}

func (q *ChunkQueue[T]) popChunk() {
	c := q.firstChunk()
	if c.next == nil {
		q.extend(1)
	}
	q.chunks[q.head] = nil
	q.head++
	q.chunks[q.head].prev = nil

	c.reset()
	q.chunkPool.Put(c)
}

// EnqueueMany enqueues multiple elements at a time
func (q *ChunkQueue[T]) EnqueueMany(vals ...T) {
	n := len(vals)
	c := q.lastChunk()
	if q.Cap()-q.Size() < n {
		q.extend(n - (q.chunkLength - c.r))
	}

	for _, val := range vals {
		q.Enqueue(val)
	}
}

// DequeueAll dequeues all elements in the queue
func (q *ChunkQueue[T]) DequeueAll() ([]T, bool) {
	return q.DequeueMany(q.Size())
}

// DequeueMany dequeues n elements at a time
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
	for i := q.head; i < q.tail && cnt < n; i++ {
		c := q.chunks[i]
		popLen := c.len()
		if n-cnt < popLen {
			popLen = n - cnt
		}
		for j := 0; j < popLen; j++ {
			res[cnt+j] = c.data[c.l+j]
			c.data[c.l+j] = q.defaultValue
		}
		c.l += popLen
		cnt += popLen
		q.size -= popLen

		if c.l == q.chunkLength {
			q.popChunk()
		}
	}
	return res, ok
}

// Clear clears the queue to empty and shrinks the chunks array
func (q *ChunkQueue[T]) Clear() {
	if !q.Empty() {
		emptyChunk := make([]T, q.chunkLength, q.chunkLength)
		for i := q.head; i < q.tail; i++ {
			q.size -= q.chunks[i].len()
			copy(q.chunks[i].data[:], emptyChunk[:])
			q.popChunk()
		}
	}
	// Shink the chunks array
	q.reallocateChunksArray(-1)
}

// Shrink shrinks the space of the chunks array
func (q *ChunkQueue[T]) Shrink() {
	q.reallocateChunksArray(-1)
}

// Range iterates the queue from head to the first element that does NOT satisfy f()
func (q *ChunkQueue[T]) Range(f func(e T) bool) {
	if q.Empty() {
		return
	}

	var c *chunk[T]
	for i := q.head; i < q.tail; i++ {
		c = q.chunks[i]
		for j := c.l; j < c.r; j++ {
			if !f(c.data[j]) {
				return
			}
		}
	}
}

// RangeWithIndex iterates the queue with index from head to the first element
// that does NOT satisfy f
func (q *ChunkQueue[T]) RangeWithIndex(f func(idx int, e T) bool) {
	if q.Empty() {
		return
	}

	var c *chunk[T]
	idx := 0
	for i := q.head; i < q.tail; i++ {
		c = q.chunks[i]
		for j := c.l; j < c.r; j++ {
			if !f(idx, c.data[j]) {
				return
			}
			idx++
		}
	}
}
