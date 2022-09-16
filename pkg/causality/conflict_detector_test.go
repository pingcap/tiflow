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

package causality

import (
	"fmt"
	"testing"
	"time"
)

type event struct {
	keys []conflictKey
}

func (e event) ConflictKeys(numSlots int64) []conflictKey {
	return e.keys
}

type wkr struct {
	quickUnlock bool
	events      []event
	unlocks     []func()
}

func (w *wkr) Add(txn event, unlock func()) {
	w.events = append(w.events, txn)
	if w.quickUnlock {
		unlock()
	} else {
		w.unlocks = append(w.unlocks, unlock)
	}
	return
}

func TestConflictDetectorContinueFetch(t *testing.T) {
	workers := make([]*wkr, 0, 2)
	for i := 0; i < 2; i++ {
		workers = append(workers, &wkr{quickUnlock: false})
	}
	detector := NewConflictDetector[*wkr, event](workers, 8)

	detector.Add(event{keys: []conflictKey{1}})
	time.Sleep(100 * time.Millisecond)
	detector.Add(event{keys: []conflictKey{1}})
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("wkr 1: %d\n", len(workers[0].events))
	fmt.Printf("wkr 2: %d\n", len(workers[1].events))
}

func TestConflictDetectorBatchUnlock(t *testing.T) {
	workers := make([]*wkr, 0, 2)
	for i := 0; i < 2; i++ {
		workers = append(workers, &wkr{quickUnlock: false})
	}
	detector := NewConflictDetector[*wkr, event](workers, 8)

	detector.Add(event{keys: []conflictKey{1, 2}})
	time.Sleep(100 * time.Millisecond)
	detector.Add(event{keys: []conflictKey{3, 4}})
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("wkr 1: %d\n", len(workers[0].events))
	fmt.Printf("wkr 2: %d\n", len(workers[1].events))
	detector.Add(event{keys: []conflictKey{2, 3}})
	time.Sleep(100 * time.Millisecond)
	detector.Add(event{keys: []conflictKey{1, 4}})
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("wkr 1: %d\n", len(workers[0].events))
	fmt.Printf("wkr 2: %d\n", len(workers[1].events))

	if workers[0].events[0].keys[0] == 1 {
		fmt.Printf("clear worker 1\n")
		workers[0].unlocks[0]()
		workers[0].unlocks = nil
		workers[0].events = nil
	} else {
		fmt.Printf("clear worker 2\n")
		workers[1].unlocks[0]()
		workers[1].unlocks = nil
		workers[1].events = nil
	}
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("wkr 1: %d\n", len(workers[0].events))
	fmt.Printf("wkr 2: %d\n", len(workers[1].events))
}
