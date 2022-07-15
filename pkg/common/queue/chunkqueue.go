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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const (
	defaultSizePerChunk = 1024
)

// ChunkQueue is a generic, efficient and GC-friendly queue
type ChunkQueue[T any] struct {
	// [head, tail] is the section of chunks in use
	head int
	tail int
	size int

	chunks       []*chunk[T]
	chunkSize    int
	chunkPool    sync.Pool
	defaultValue T
}

func (q *ChunkQueue[T]) headChunk() *chunk[T] {
	return q.chunks[q.head]
}

func (q *ChunkQueue[T]) tailChunk() *chunk[T] {
	return q.chunks[q.tail]
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
		log.Panic("Cannot create queue of type that size == 0")
	}

	chunkSize := defaultSizePerChunk / int(unsafe.Sizeof(*new(T)))
	if chunkSize < 16 {
		chunkSize = 16
	}

	q := &ChunkQueue[T]{
		head:      0,
		tail:      -1, // temporary be negative
		size:      0,
		chunkSize: chunkSize,
	}
	q.chunkPool = sync.Pool{
		New: func() any {
			return newChunk[T](q.chunkSize, q)
		},
	}

	q.chunks = make([]*chunk[T], 32, 32)
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
	return q.chunkSize*(q.tail-q.head) - q.chunks[q.head].l - 1
}

// Empty returns if the queue is empty now
func (q *ChunkQueue[T]) Empty() bool {
	return q.size == 0
}

// Getters

// At() returns the pointer to an element
func (q *ChunkQueue[T]) At(idx int) (*T, bool) {
	if q == nil || idx < 0 || idx > q.size {
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
	headChunk := q.headChunk()
	return &headChunk.data[headChunk.l], true
}

// Tail() returns the pointer to the first element in queue and nil if empty
func (q *ChunkQueue[T]) Tail() (*T, bool) {
	if q.Empty() {
		return nil, false
	}
	tailChunk := q.headChunk()
	return &tailChunk.data[tailChunk.r-1], true
}

// Begin() gives the first iterator of the queue
func (q *ChunkQueue[T]) Begin() *ChunkQueueIterator[T] {
	return &ChunkQueueIterator[T]{
		chunk:      q.headChunk(),
		idxInChunk: q.headChunk().l,
	}
}

// End() creates an special iterator of the queue representing the end
func (q *ChunkQueue[T]) End() *ChunkQueueIterator[T] {
	return &ChunkQueueIterator[T]{
		chunk:      q.tailChunk(),
		idxInChunk: -1,
	}
}

// Operations
func (q *ChunkQueue[T]) expandSpace(n int) {
	if n <= 0 {
		n = 1
	}
	chunksNum := (n + q.chunkSize - 1) / q.chunkSize

	if len(q.chunks)-q.tail-chunksNum <= 4 {
		q.expandPitch(chunksNum)
	}

	for i := 0; i < chunksNum; i++ {
		idx := i + q.tail + 1
		c := q.chunkPool.Get().(*chunk[T])

		// c := new(chunk[T])
		// c.data = make([]T, q.chunkSize, q.chunkSize)

		if idx > 0 {
			c.prevCk = q.chunks[idx-1]
			q.chunks[idx-1].nextCk = c
		}
		q.chunks[idx] = c
	}
	if q.tail < 0 {
		q.tail = 0
	}
}

func (q *ChunkQueue[T]) expandPitch(x int) {
	n := len(q.chunks)
	used := q.tail - q.head + 1
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
	copy(newChunks, q.chunks[q.head:q.tail+1])
	q.tail -= q.head
	q.head = 0
	q.chunks = newChunks
}

// PushBack() pushes an element to tail
func (q *ChunkQueue[T]) PushBack(v T) error {
	tailChunk := q.tailChunk()
	if tailChunk.r < q.chunkSize {
		tailChunk.data[tailChunk.r] = v
		tailChunk.r++
	} else {
		if tailChunk.nextCk == nil {
			q.expandSpace(1)
		}
		tailChunk = tailChunk.nextCk
		tailChunk.data[tailChunk.r] = v
		tailChunk.r++
		q.tail++
	}
	q.size++
	return nil
}

// PopFront() pops an element from head
func (q *ChunkQueue[T]) PopFront() (T, error) {
	if q.Empty() {
		return *new(T), errors.New("empty queue")
	}

	headChunk := q.headChunk()
	v := headChunk.data[headChunk.l]
	headChunk.data[headChunk.l] = q.defaultValue
	headChunk.l++
	q.size--

	if headChunk.l == q.chunkSize {
		q.popChunk()
	}

	return v, nil
}

func (q *ChunkQueue[T]) popChunk() {
	headChunk := q.headChunk()
	if headChunk.nextCk == nil {
		q.expandSpace(1)
	}

	q.chunks[q.head] = nil
	q.head++

	q.chunks[q.head].prevCk = nil
	headChunk.reset()
	q.chunkPool.Put(headChunk)
	if q.Empty() {
		q.tail = q.head
	}
}

// Enqueue() enques a single element to the tail
func (q *ChunkQueue[T]) Enqueue(v T) error {
	return q.PushBack(v)
}

// Dequeue() deques a single element from the head
func (q *ChunkQueue[T]) Dequeue() (T, error) {
	return q.PopFront()
}

// EnqueueMany() pushes multiple elements to the tail at a time
func (q *ChunkQueue[T]) EnqueueMany(vals ...T) error {
	return q.PushBackMany(vals...)
}

// PushBackMany() pushes multiple elements to the tail at a time
func (q *ChunkQueue[T]) PushBackMany(vals ...T) error {
	if q.Cap()-q.Size() < len(vals) {
		q.expandSpace(len(vals) + 1)
	}

	for _, val := range vals {
		if err := q.PushBack(val); err != nil {
			return err
		}
	}
	return nil
}

// DequeueMany() deques n elements from head.
func (q *ChunkQueue[T]) DequeueMany(n int) ([]T, error) {
	return q.PopFrontMany(n)
}

// PopFrontMany() deques n elements from head.
func (q *ChunkQueue[T]) PopFrontMany(n int) ([]T, error) {
	if n < 0 {
		return nil, errors.New("could not pop elements of a negative number")
	}
	if q.size < n {
		return nil, errors.New("not enough elements to pop")
	}

	res := make([]T, n, n)
	cnt := 0
	for i := q.head; i <= q.tail && cnt < n; i++ {
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

		if c.l == q.chunkSize {
			q.popChunk()
		}
	}
	return res, nil
}
