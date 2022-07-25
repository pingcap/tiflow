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

// ChunkQueueIterator is the iterator type of ChunkQueue
type ChunkQueueIterator[T any] struct {
	idxInChunk int
	chunk      *chunk[T]
}

// Begin returns the first iterator of the queue
func (q *ChunkQueue[T]) Begin() *ChunkQueueIterator[T] {
	return &ChunkQueueIterator[T]{
		chunk:      q.firstChunk(),
		idxInChunk: q.firstChunk().l,
	}
}

// End creates a special iterator of the queue representing the end
func (q *ChunkQueue[T]) End() *ChunkQueueIterator[T] {
	return &ChunkQueueIterator[T]{
		chunk:      q.lastChunk(),
		idxInChunk: q.chunkLength,
	}
}

// GetIterator returns the iterator of a given index, and nil if out of range
func (q *ChunkQueue[T]) GetIterator(idx int) *ChunkQueueIterator[T] {
	if idx < 0 || idx >= q.size {
		return nil
	}
	idx += q.chunks[q.head].l
	return &ChunkQueueIterator[T]{
		chunk:      q.chunks[q.head+idx/q.chunkLength],
		idxInChunk: idx % q.chunkLength,
	}
}

// Valid indicates if the element of the iterator is in queue or not
func (it *ChunkQueueIterator[T]) Valid() bool {
	return it.chunk != nil && it.idxInChunk >= it.chunk.l && it.idxInChunk < it.chunk.r
}

// Value returns the element value of a valid iterator which is in queue.
// It's meaningless and may panic otherwise
func (it *ChunkQueueIterator[T]) Value() T {
	return it.chunk.data[it.idxInChunk]
}

// Index returns the index of a valid iterator, and -1 otherwise
func (it *ChunkQueueIterator[T]) Index() int {
	if !it.Valid() {
		return -1
	}
	q := it.chunk.queue
	idx := 0
	for i := q.head; i < q.tail; i++ {
		if q.chunks[i] != it.chunk {
			idx += q.chunks[i].len()
		} else {
			idx += it.idxInChunk
			break
		}
	}
	return idx
}

// Next updates the current iterator to its next iterator and returns it
func (it *ChunkQueueIterator[T]) Next() *ChunkQueueIterator[T] {
	it.idxInChunk++
	if it.idxInChunk < it.chunk.r {
		return it
	}

	c, q := it.chunk, it.chunk.queue
	if it.idxInChunk == q.chunkLength && c.next != nil && !c.empty() {
		it.idxInChunk, it.chunk = 0, c.next
	} else {
		it.idxInChunk = q.chunkLength
	}
	return it
}

// Prev updates the current to its previous one and returns it
func (it *ChunkQueueIterator[T]) Prev() *ChunkQueueIterator[T] {
	if it.chunk == nil {
		return it
	}

	c := it.chunk
	if it.idxInChunk < c.l || it.idxInChunk >= c.r {
		// if the iterator is an end iterator and the queue is not empty,
		// then the iterator shall point to the last element.
		if it.idxInChunk == len(c.data) && !c.queue.Empty() {
			lastChunk := c.queue.lastChunk()
			it.chunk, it.idxInChunk = lastChunk, lastChunk.r-1
		}
		return it
	}

	it.idxInChunk--
	if it.idxInChunk >= it.chunk.l {
		return it
	}

	it.chunk = c.prev
	if it.chunk == nil {
		it.idxInChunk = -1
	} else {
		it.idxInChunk = it.chunk.r - 1
	}
	return it
}
