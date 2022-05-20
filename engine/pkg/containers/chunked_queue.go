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

import (
	"sync"

	"github.com/edwingeng/deque"
)

// Deque implements Queue with edwingeng/deque
//nolint:structcheck
type Deque[T any] struct {
	// mu protects deque, because it is not thread-safe.
	mu    sync.RWMutex
	deque deque.Deque
}

// NewDeque creates a new Deque instance
func NewDeque[T any]() *Deque[T] {
	return &Deque[T]{
		deque: deque.NewDeque(),
	}
}

func (d *Deque[T]) Push(elem T) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.deque.PushBack(elem)
}

func (d *Deque[T]) Pop() (T, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.deque.Empty() {
		var noVal T
		return noVal, false
	}

	return d.deque.PopFront().(T), true
}

func (d *Deque[T]) Peek() (T, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.deque.Empty() {
		var noVal T
		return noVal, false
	}

	return d.deque.Front().(T), true
}

func (d *Deque[T]) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.deque.Len()
}
