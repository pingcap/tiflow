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
	// queue      *ChunkQueue[T]
	chunk *chunk[T]
}

// IsEnd() checks if the iterator is the end()
func (it *ChunkQueueIterator[T]) IsEnd() bool {
	if it.chunk == nil || it.chunk.queue == nil {
		panic("invalid interator")
	}
	return it.idxInChunk < 0
}

// Value() returns the pointer to the element of the iterator
func (it *ChunkQueueIterator[T]) Value() (*T, bool) {
	if it.IsEnd() {
		return nil, false
	}
	return &it.chunk.data[it.idxInChunk], true
}

// Next() returns the next iterator of the current iterator
func (it *ChunkQueueIterator[T]) Next() *ChunkQueueIterator[T] {
	if it.IsEnd() {
		return it
	}

	it.idxInChunk++
	if it.idxInChunk < it.chunk.r {
		return it
	}

	nextCk := it.chunk.nextCk
	q := it.chunk.queue
	if it.chunk.r < q.chunkSize || nextCk == nil || nextCk.empty() {
		return q.End()
	}

	it.chunk = nextCk
	it.idxInChunk = nextCk.l
	return it
}

// Prev() returns the previous iterator of the current iterator
func (it *ChunkQueueIterator[T]) Prev() *ChunkQueueIterator[T] {
	if it.IsEnd() {
		q := it.chunk.queue
		if q.Empty() {
			return it
		}
		it.chunk = q.chunks[q.tail]
		if it.chunk.empty() {
			panic("None empty queue with last empty chunk")
		}
		it.idxInChunk = it.chunk.r - 1
		return it
	}

	it.idxInChunk--
	if it.idxInChunk >= it.chunk.l {
		return it
	}

	prevCk := it.chunk.prevCk
	q := it.chunk.queue
	if it.chunk.l > 0 || prevCk == nil || prevCk.empty() {
		return q.End()
	}
	it.chunk = prevCk
	it.idxInChunk = prevCk.r - 1

	return it
}
