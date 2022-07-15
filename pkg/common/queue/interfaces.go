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

import "github.com/pingcap/tiflow/pkg/common/iterator"

// ChunkQueueInterface defines the interface of ChunkQueue
type ChunkQueueInterface[T any] interface {
	iterator.Iterator[T]

	// Meta
	Size() int
	Cap() int
	Empty() bool

	// Getters
	At(idx int) (*T, bool)
	Front() iterator.Iterator[T]
	Back() iterator.Iterator[T]

	// Operations
	Enqueue(v T) error
	Dequeue() (T, error)
	EnqueueMany(vals ...T) error
	DequeueMany(n int) ([]T, error)

	PushBack(v T) error
	PopFront() (T, error)
	PushBackMany(vals ...T) error
	PopFrontMany(n int) ([]T, error)
}
