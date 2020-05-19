// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"container/list"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
)

// test helper
type memOracle struct {
	ts uint64
}

func newMemOracle() *memOracle {
	return &memOracle{
		ts: 0,
	}
}

func (o *memOracle) getTs() uint64 {
	return atomic.AddUint64(&o.ts, 1)
}

// a test helper, generate n pair item
func newItemGenerator(txnNum int32, maxLatency int64, fakeTxnPerNum int32) <-chan sortItem {
	items := make(chan sortItem)
	oracle := newMemOracle()

	const threadNum = 10

	var wg sync.WaitGroup

	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				num := atomic.AddInt32(&txnNum, -1)
				if num < 0 {
					return
				}

				if fakeTxnPerNum > 0 && num%fakeTxnPerNum == 0 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ts := oracle.getTs()
						items <- sortItem{
							start:  ts,
							commit: ts,
							tp:     cdcpb.Event_ROLLBACK,
						}
					}()
				}

				start := oracle.getTs()
				items <- sortItem{
					start: start,
					tp:    cdcpb.Event_PREWRITE,
				}

				time.Sleep(time.Millisecond * time.Duration(rand.Int63()%maxLatency))

				commit := oracle.getTs()
				items <- sortItem{
					start:  start,
					commit: commit,
					tp:     cdcpb.Event_COMMIT,
				}

				time.Sleep(time.Microsecond * time.Duration(rand.Int63()%maxLatency))
			}

		}()
	}

	go func() {
		wg.Wait()
		close(items)
	}()

	return items
}

type sortItem struct {
	start  uint64
	commit uint64
	tp     cdcpb.Event_LogType
}

type sorter struct {
	maxTsItemCB func(item sortItem)
	// save the start ts of txn which we need to wait for the C binlog
	waitStartTs map[uint64]struct{}

	lock   sync.Mutex
	cond   *sync.Cond
	items  *list.List
	wg     sync.WaitGroup
	closed int32
}

func newSorter(fn func(item sortItem)) *sorter {
	sorter := &sorter{
		maxTsItemCB: fn,
		items:       list.New(),
		waitStartTs: make(map[uint64]struct{}),
	}

	sorter.cond = sync.NewCond(&sorter.lock)
	sorter.wg.Add(1)
	go sorter.run()

	return sorter
}

func (s *sorter) allMatched() bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.waitStartTs) == 0
}

func (s *sorter) pushTsItem(item sortItem) {
	if s.isClosed() {
		log.Fatal("sorter is closed but still push item, this should never happen")
	}

	s.lock.Lock()

	if item.tp == cdcpb.Event_PREWRITE {
		s.waitStartTs[item.start] = struct{}{}
	} else {
		delete(s.waitStartTs, item.start)
		s.cond.Signal()
	}

	s.items.PushBack(item)
	if s.items.Len() == 1 {
		s.cond.Signal()
	}
	s.lock.Unlock()
}

func (s *sorter) run() {
	defer s.wg.Done()

	go func() {
		// Avoid if no any more pushTsItem call so block at s.cond.Wait() in run() waiting the matching c-binlog
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()
		for range tick.C {
			s.cond.Signal()
			if s.isClosed() {
				return
			}
		}
	}()

	var maxTsItem sortItem
	for {
		s.cond.L.Lock()
		for s.items.Len() == 0 {
			if s.isClosed() {
				s.cond.L.Unlock()
				return
			}
			s.cond.Wait()
		}

		front := s.items.Front()
		item := front.Value.(sortItem)
		s.items.Remove(front)

		if item.tp == cdcpb.Event_PREWRITE {
			for {
				_, ok := s.waitStartTs[item.start]
				if !ok {
					break
				}

				// quit if sorter is closed and still not get the according C binlog
				// or continue to handle the items
				if s.isClosed() {
					s.cond.L.Unlock()
					return
				}
				s.cond.Wait()
			}
		} else {
			// commit is greater than zero, it's OK when we get the first item and maxTsItem.commit = 0
			if item.commit > maxTsItem.commit {
				maxTsItem = item
				s.maxTsItemCB(maxTsItem)
			}
		}
		s.cond.L.Unlock()
	}
}

func (s *sorter) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *sorter) close() {
	atomic.StoreInt32(&s.closed, 1)
	// let run() await and quit when items.Len() = 0
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()

	s.wg.Wait()
}
