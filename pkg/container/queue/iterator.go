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

// ChunkQueueIterator is the iterator of ChunkQueue
type ChunkQueueIterator[T any] struct {
	idxInChunk int
	chunk      *chunk[T]
}

// Begin() gives the first iterator of the queue
func (q *ChunkQueue[T]) Begin() *ChunkQueueIterator[T] {
	if q.Empty() {
		return q.End()
	}
	return &ChunkQueueIterator[T]{
		chunk:      q.firstChunk(),
		idxInChunk: q.firstChunk().l,
	}
}

// End() creates an special iterator of the queue representing the end
func (q *ChunkQueue[T]) End() *ChunkQueueIterator[T] {
	return &ChunkQueueIterator[T]{
		chunk:      q.lastChunk(),
		idxInChunk: q.chunkLength,
	}
}

// GetIterator() returns a iterator given the index, and nil if out of range
func (q *ChunkQueue[T]) GetIterator(idx int) *ChunkQueueIterator[T] {
	if q.Empty() || idx < 0 || idx >= q.size {
		return nil
	}
	idx += q.chunks[q.head].l
	return &ChunkQueueIterator[T]{
		chunk:      q.chunks[q.head+idx/q.chunkLength],
		idxInChunk: idx % q.chunkLength,
	}
}

//// isEnd() checks if the iterator is an end iterator.
//// Iterators of element that has been dequeued are end iterators.
//func (it *ChunkQueueIterator[T]) isEnd() bool {
//	return it.chunk != nil && it.idxInChunk == len(it.chunk.data)
//}

// Valid() indicates if iterator points to a valid element
func (it *ChunkQueueIterator[T]) Valid() bool {
	return it.chunk != nil && it.chunk.queue != nil &&
		it.idxInChunk >= it.chunk.l && it.idxInChunk < it.chunk.r
}

// Value returns the element value of the iterator
func (it *ChunkQueueIterator[T]) Value() T {
	if !it.Valid() {
		var defaultValue T
		return defaultValue
	}
	return it.chunk.data[it.idxInChunk]
}

// Index() returns the index of a given iterator, -1 for end or expired iterator
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

// Next() updates the current iterator to its next iterator and returns it
func (it *ChunkQueueIterator[T]) Next() *ChunkQueueIterator[T] {
	if !it.Valid() {
		return it
	}

	it.idxInChunk++
	if it.idxInChunk < it.chunk.r {
		return it
	}

	nextCk := it.chunk.nextCk
	if chunkLength := len(it.chunk.data); it.chunk.r < chunkLength ||
		nextCk == nil || nextCk.empty() {
		it.idxInChunk = len(it.chunk.data)
	} else {
		it.chunk, it.idxInChunk = nextCk, nextCk.l
	}
	return it
}

// Prev updates the current to its previous one and returns it
func (it *ChunkQueueIterator[T]) Prev() *ChunkQueueIterator[T] {
	if !it.Valid() {
		// if the iterator is an end iterator and the queue is not empty,
		// then the iterator shall point to the last element.
		if c := it.chunk; c != nil && c.queue != nil &&
			it.idxInChunk == len(c.data) {
			if !c.queue.Empty() {
				lastChunk := c.queue.lastChunk()
				it.chunk, it.idxInChunk = lastChunk, lastChunk.r-1
				return it
			}
		}
		return it
	}

	it.idxInChunk--
	if it.idxInChunk >= it.chunk.l {
		return it
	}

	it.chunk = it.chunk.prevCk
	if it.chunk == nil {
		it.idxInChunk = -1
	} else {
		it.idxInChunk = it.chunk.r - 1
	}
	return it
}
