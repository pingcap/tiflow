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

type chunk[T any] struct {
	// [l, r) is a left open right closed interval,
	// that indicates invalid elements within the chunk
	l    int
	r    int
	data []T

	prevCk *chunk[T]
	nextCk *chunk[T]

	// a pointer points to the queue
	queue *ChunkQueue[T]
}

func (c *chunk[T]) empty() bool {
	return c.l >= c.r
}

func (c *chunk[T]) len() int {
	return c.r - c.l
}

func (c *chunk[T]) front() (T, bool) {
	if !c.empty() {
		return c.data[c.l], true
	}
	return *new(T), false
}

func (c *chunk[T]) back() (T, bool) {
	if !c.empty() {
		return c.data[c.r-1], true
	}
	return *new(T), false
}

func (c *chunk[T]) reset() {
	c.l = 0
	c.r = 0
	c.prevCk = nil
	c.nextCk = nil
}

func newChunk[T any](sz int, q *ChunkQueue[T]) *chunk[T] {
	return &chunk[T]{
		data:  make([]T, sz, sz),
		queue: q,
	}
}
