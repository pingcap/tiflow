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

package containers

import "sync"

// SliceQueue is a FIFO queue implemented
// by a Go slice.
type SliceQueue[T any] struct {
	mu    sync.Mutex
	elems []T

	// C is a signal for non-empty queue.
	// A consumer can select for C and then Pop
	// as many elements as possible in a for-select
	// loop.
	// Refer to an example in TestSliceQueueConcurrentWriteAndRead.
	C chan struct{}

	pool *sync.Pool
}

// NewSliceQueue creates a new SliceQueue.
func NewSliceQueue[T any]() *SliceQueue[T] {
	return &SliceQueue[T]{
		C:    make(chan struct{}, 1),
		pool: &sync.Pool{},
	}
}

// Push pushes element to the end of the queue
func (q *SliceQueue[T]) Push(elem T) {
	q.mu.Lock()

	signal := false
	if len(q.elems) == 0 {
		signal = true
		if q.elems == nil {
			q.elems = q.allocateSlice()
			q.elems = q.elems[:0]
		}
	}

	q.elems = append(q.elems, elem)
	q.mu.Unlock()

	if signal {
		select {
		case q.C <- struct{}{}:
		default:
		}
	}
}

// Pop removes the first element from queue and returns it, if it exists
func (q *SliceQueue[T]) Pop() (T, bool) {
	q.mu.Lock()

	var zero T
	if len(q.elems) == 0 {
		q.mu.Unlock()
		return zero, false
	}

	ret := q.elems[0]
	q.elems[0] = zero
	q.elems = q.elems[1:]

	if len(q.elems) == 0 {
		q.freeSlice(q.elems)
		q.elems = nil
	} else {
		// non empty queue
		select {
		case q.C <- struct{}{}:
		default:
		}
	}

	q.mu.Unlock()
	return ret, true
}

// Peek returns the first element of the queue if exits.
func (q *SliceQueue[T]) Peek() (retVal T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elems) == 0 {
		ok = false
		return
	}

	return q.elems[0], true
}

// Size returns the element count in the queue
func (q *SliceQueue[T]) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.elems)
}

func (q *SliceQueue[T]) allocateSlice() []T {
	ptr := q.pool.Get()
	if ptr == nil {
		return make([]T, 0, 16)
	}

	return *(ptr.(*[]T))
}

func (q *SliceQueue[T]) freeSlice(s []T) {
	if len(s) != 0 {
		panic("only empty slice allowed")
	}
	q.pool.Put(&s)
}
