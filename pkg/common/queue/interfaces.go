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

type ChunkQueueInterface[T any] interface {
	iterator.Iterator[T]

	// Meta
	Size() int
	Cap() int
	Empty() bool

	// Getters
	At(idx int) *T
	Front() iterator.Iterator[T]
	Back() iterator.Iterator[T]

	// Operations
	Enqueue(v T)
	Dequeue() T

	EnqueueMany(val ...T)
	DequeueMany(n int) []T
}
