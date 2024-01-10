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

package sinkmanager

import (
	"container/heap"
	"sync"

	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

// progress is the fetch progress of a table.
type progress struct {
	span              tablepb.Span
	nextLowerBoundPos sorter.Position
	version           uint64
}

// Assert progressHeap implements heap.Interface
var _ heap.Interface = (*progressHeap)(nil)

type progressHeap struct {
	heap []*progress
}

func newProgressHeap() *progressHeap {
	return &progressHeap{
		heap: make([]*progress, 0),
	}
}

func (p *progressHeap) Len() int {
	return len(p.heap)
}

func (p *progressHeap) Less(i, j int) bool {
	a := p.heap[i]
	b := p.heap[j]
	return a.nextLowerBoundPos.Compare(b.nextLowerBoundPos) == -1
}

func (p *progressHeap) Swap(i, j int) {
	p.heap[i], p.heap[j] = p.heap[j], p.heap[i]
}

func (p *progressHeap) Push(x any) {
	p.heap = append(p.heap, x.(*progress))
}

func (p *progressHeap) Pop() any {
	n := len(p.heap)
	x := p.heap[n-1]
	p.heap = p.heap[:n-1]
	return x
}

// tableProgresses used to manage the progress of all tables.
// SinkManager will use this to determine when and how to fetch data from the sorter.
type tableProgresses struct {
	mu   sync.Mutex
	heap *progressHeap
}

func newTableProgresses() *tableProgresses {
	ph := newProgressHeap()
	heap.Init(ph)
	return &tableProgresses{
		heap: ph,
	}
}

func (p *tableProgresses) push(progress *progress) {
	p.mu.Lock()
	defer p.mu.Unlock()
	heap.Push(p.heap, progress)
}

func (p *tableProgresses) pop() *progress {
	p.mu.Lock()
	defer p.mu.Unlock()
	return heap.Pop(p.heap).(*progress)
}

func (p *tableProgresses) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heap.Len()
}
