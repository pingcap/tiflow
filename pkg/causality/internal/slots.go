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

package internal

import (
	"container/list"
	"sync"
)

type Eq[T any] interface {
	Equals(other T) bool
}

// Slots implements slot-based conflict detection.
// It holds references to E, which can be used to build
// a DAG of dependency.
type Slots[E Eq[E]] struct {
	mu    sync.Mutex
	slots map[int64]*list.List

	numSlots int64
}

func NewSlots[E Eq[E]](numSlots int64) *Slots[E] {
	return &Slots[E]{
		slots:    make(map[int64]*list.List),
		numSlots: numSlots,
	}
}

func (s *Slots[E]) Add(elem E, keys []int64, onConflict func(dependee E)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		if elemList, ok := s.slots[key%s.numSlots]; ok {
			for e := elemList.Front(); e != nil; e = e.Next() {
				e := e.Value.(E)
				if e.Equals(elem) {
					continue
				}
				onConflict(e)
			}
		} else {
			s.slots[key%s.numSlots] = list.New()
		}
		s.slots[key%s.numSlots].PushBack(elem)
	}
}

func (s *Slots[E]) Remove(elem E, keys []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		elemList, ok := s.slots[key%s.numSlots]
		if !ok {
			panic("elem list is not found")
		}
		for e := elemList.Front(); e != nil; e = e.Next() {
			if elem.Equals(e.Value.(E)) {
				elemList.Remove(e)
				break
			}
		}
	}
}
