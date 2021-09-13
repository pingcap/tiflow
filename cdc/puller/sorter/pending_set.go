// Copyright 2021 PingCAP, Inc.
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

package sorter

import (
	"container/heap"
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	minCompactionFactor     = 4
	maxCompactionFactor     = 512
	maxCompactedFileSize    = 128 * 1024 * 1024 // 128M
	advisedAdvanceStepFiles = 4
)

type pendingSet struct {
	tree           *btree.BTree
	compacted      *btree.BTree
	current        map[*flushTask]struct{}
	lastResolvedTs uint64

	insertMu sync.Mutex
	inserted []*flushTask

	nextTaskID int64 // used to generate unique IDs for tasks
}

var (
	pendingSetFreeListMu sync.Mutex
	pendingSetFreeList   *btree.FreeList
)

func newPendingSet() *pendingSet {
	pendingSetFreeListMu.Lock()
	defer pendingSetFreeListMu.Unlock()

	if pendingSetFreeList == nil {
		pendingSetFreeList = btree.NewFreeList(100000)
	}

	return &pendingSet{
		tree:      btree.NewWithFreeList(3, pendingSetFreeList),
		compacted: btree.NewWithFreeList(3, pendingSetFreeList),
		current:   map[*flushTask]struct{}{},
	}
}

// Insert inserts a flushTask into the pendingSet.
// Note that duplicate inserts are NOT allowed here.
func (s *pendingSet) Insert(task *flushTask) {
	s.insertMu.Lock()
	defer s.insertMu.Unlock()

	s.inserted = append(s.inserted, task)
}

func (s *pendingSet) flushInserted() {
	s.insertMu.Lock()
	defer s.insertMu.Unlock()

	for _, task := range s.inserted {
		if s.tree.ReplaceOrInsert(task) != nil {
			panic("duplicate element")
		}
	}

	s.inserted = s.inserted[:0]
}

// Retire deletes `task` from the pendingSet.
// If `task` was not previously in the pendingSet,
// the function returns false.
func (s *pendingSet) Retire(task *flushTask) {
	if _, ok := s.current[task]; ok {
		delete(s.current, task)
		return
	}

	panic("unreachable")
}

func (s *pendingSet) peekMin() (*flushTask, *btree.BTree) {
	if s.tree.Len() == 0 && s.compacted.Len() == 0 {
		return nil
	}
	if s.tree.Len() > 0 && s.compacted.Len() == 0 {
		return s.tree.Min().(*flushTask), s.tree
	}
	if s.tree.Len() == 0 && s.compacted.Len() > 0 {
		return s.compacted.Min().(*flushTask), s.compacted
	}
	treeMin := s.tree.Min().(*flushTask)
	compactedMin := s.compacted.Min().(*flushTask)
	if treeMin.tsLowerBound < compactedMin.tsLowerBound {
		return treeMin, s.tree
	}
	return compactedMin, s.compacted
}

func (s *pendingSet) AdvanceResolvedTs(resolvedTs uint64) uint64 {
	// We need to insert the newly received flushTasks to the tree,
	// so that they can be properly sorted.
	s.flushInserted()

	// There are no unprocessed flushTasks, we can push resolvedTs all
	// the way to the last received resolvedTs.
	if s.compacted.Len() == 0 && s.tree.Len() == 0 {
		return resolvedTs
	}
	
	var (
		i             int
		newResolvedTs uint64
	)

	for {
		var nextStepResolvedTs uint64
		for {
			var treeToPop *btree.BTree
			if s.compacted.Len() == 0 && s.tree.Len() > 0 {
				treeToPop = s.tree
			} else if s.tree.Len() == 0 && s.compacted.Len() > 0 {
				treeToPop = s.compacted
			} else if s.tree.Min().(*flushTask).tsLowerBound < s.compacted.Min().(*flushTask).tsLowerBound {
				treeToPop = s.tree
			} else {
				treeToPop = s.compacted
			}

			taskToPop := treeToPop.Min().(*flushTask)
			nextStepResolvedTs = taskToPop.tsLowerBound
			if nextStepResolvedTs > resolvedTs {
				log.Info("wee!")
				return newResolvedTs
			}

			log.Info("addToCurrent",
				zap.Uint64("nextStepResolvedTs", nextStepResolvedTs),
				zap.Uint64("newResolvedTs", newResolvedTs))
			s.current[taskToPop] = struct{}{}
			treeToPop.DeleteMin()
			i++

			if s.compacted.Len() > 0 && s.compacted.Min().(*flushTask).tsLowerBound == nextStepResolvedTs {
				continue
			}
			if s.tree.Len() > 0 && s.tree.Min().(*flushTask).tsLowerBound == nextStepResolvedTs {
				continue
			}
			break
		}
		if nextStepResolvedTs <= newResolvedTs {
			panic("bug?")
		}
		newResolvedTs = nextStepResolvedTs
		log.Info("update newResolvedTs", zap.Uint64("nextStepResolvedTs", nextStepResolvedTs))
		if newResolvedTs >= resolvedTs {
			newResolvedTs = resolvedTs
			break
		}
		if i >= advisedAdvanceStepFiles {
			break
		}
	}

	s.lastResolvedTs = newResolvedTs
	return newResolvedTs
}

func (s *pendingSet) ForAllCurrentFlushTasks(fn func(task *flushTask) (bool, error)) error {
	for task := range s.current {
		ok, err := fn(task)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			return nil
		}
	}
	return nil
}

// closedCh is a closed channel used as the placeholder for
// flushTasks generated by the compactor.
var closedCh = func() chan error {
	ret := make(chan error)
	close(ret)
	return ret
}()

// ForAll iterators through all the tasks in the pendingSet.
func (s *pendingSet) ForAll(fn func(task *flushTask) (bool, error)) (ret error) {
	s.flushInserted()

	var retErr error
	s.tree.Ascend(func(taskI btree.Item) bool {
		task := taskI.(*flushTask)
		ok, err := fn(task)
		if err != nil {
			retErr = errors.Trace(err)
			return false
		}
		return ok
	})
	if retErr != nil {
		return retErr
	}

	s.compacted.Ascend(func(taskI btree.Item) bool {
		task := taskI.(*flushTask)
		ok, err := fn(task)
		if err != nil {
			retErr = errors.Trace(err)
			return false
		}
		return ok
	})
	if retErr != nil {
		return retErr
	}

	for task := range s.current {
		ok, err := fn(task)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			return nil
		}
	}

	return
}

// Clear clears the pendingSet
func (s *pendingSet) Clear() {
	s.flushInserted()

	s.tree.Clear(false)
	s.compacted.Clear(false)
}

func (s *pendingSet) Compact(ctx context.Context) (ret error) {
	s.flushInserted()

	candidates, err := s.findCompactCandidates(ctx, s.lastResolvedTs)
	if err != nil {
		return errors.Trace(err)
	}

	if len(candidates) < minCompactionFactor {
		return nil
	}

	backEnd, err := pool.alloc(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	sortHeap := new(sortHeap)
	newFlushTask := &flushTask{
		taskID:        int(atomic.AddInt64(&s.nextTaskID, 1)),
		heapSorterID:  -1, // the compactor is -1
		tsLowerBound:  uint64(math.MaxUint64),
		maxResolvedTs: 0, // no need in compactor
		finished:      closedCh,
		isEmpty:       false,
		backend:       backEnd,
		itemCache:     nil,
	}
	newFlushTask.dealloc = func() error {
		backEnd := newFlushTask.GetBackEnd()
		if backEnd != nil {
			defer newFlushTask.markDeallocated()
			return pool.dealloc(backEnd)
		}
		return nil
	}

	defer func() {
		if ret != nil {
			_ = newFlushTask.dealloc()
		}
	}()

	for _, task := range candidates {
		if task.tsLowerBound < newFlushTask.tsLowerBound {
			newFlushTask.tsLowerBound = task.tsLowerBound
		}

		var err error
		task.reader, err = task.GetBackEnd().reader()
		if err != nil {
			return errors.Trace(err)
		}

		nextEvent, err := task.reader.readNext()
		if err != nil {
			return errors.Trace(err)
		}
		if nextEvent == nil {
			panic("empty backEnd, bug?")
		}

		heap.Push(sortHeap, &sortItem{
			entry: nextEvent,
			data:  task,
		})
	}

	writer, err := backEnd.writer()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if ret != nil && writer != nil {
			_ = writer.flushAndClose()
		}
	}()
	count := uint64(0)
	for sortHeap.Len() > 0 {
		item := heap.Pop(sortHeap).(*sortItem)
		if err := writer.writeNext(item.entry); err != nil {
			return errors.Trace(err)
		}
		count++

		task := item.data.(*flushTask)
		nextEvent, err := task.reader.readNext()
		if err != nil {
			_ = task.reader.resetAndClose() // prevents fd leak
			task.reader = nil
			return errors.Trace(err)
		}
		if nextEvent == nil {
			// EOF
			if err := task.reader.resetAndClose(); err != nil {
				return errors.Trace(err)
			}
			if s.tree.Delete(task) == nil {
				panic("unreachable")
			}
			_ = task.dealloc()
			continue
		}

		heap.Push(sortHeap, &sortItem{
			entry: nextEvent,
			data:  task,
		})
	}
	newFlushTask.dataSize = int64(writer.dataSize())

	writer1 := writer
	writer = nil
	if err := writer1.flushAndClose(); err != nil {
		return errors.Trace(err)
	}

	s.compacted.ReplaceOrInsert(newFlushTask)

	log.Info("files compacted",
		zap.Int("num-files", len(candidates)),
		zap.Int64("data-size", newFlushTask.dataSize))

	return nil
}

func (s *pendingSet) findCompactCandidates(ctx context.Context, resolvedTs uint64) (ret []*flushTask, err error) {
	totalSize := int64(0)

	log.Info("start findCompactCandidates", zap.Int("btree-size", s.tree.Len()))
	var (
		candidates []*flushTask
		retErr     error
	)
	s.tree.Descend(func(taskI btree.Item) bool {
		task := taskI.(*flushTask)
		log.Info("findCompactCandidates",
			zap.Uint64("tsLowerBound", task.tsLowerBound),
			zap.Uint64("resolvedTs", resolvedTs))
		if task.tsLowerBound <= resolvedTs {
			return false
		}

		select {
		case <-ctx.Done():
			retErr = errors.Trace(ctx.Err())
			return false
		case <-task.finished:
		}

		candidates = append(candidates, task)
		totalSize += task.dataSize
		if len(ret) >= maxCompactionFactor || totalSize >= maxCompactedFileSize {
			return false
		}
		return true
	})
	if retErr != nil {
		return nil, errors.Trace(retErr)
	}

	return candidates, nil
}

// Less implements the btree.Item interface, used to perform comparison in the btree.
func (t *flushTask) Less(than btree.Item) bool {
	other := than.(*flushTask)
	if t.tsLowerBound != other.tsLowerBound {
		return t.tsLowerBound < other.tsLowerBound
	}

	// We cannot allow comparison between two distinct tasks to tie,
	// because the btree implementation assumes that equality implies identity.
	//
	// i.e, if !a.Less(b) && !b.Less(a), btree treats this to mean a == b (i.e. we can only
	// hold one of either a or b in the tree).

	if t.taskID != other.taskID {
		return t.taskID < other.taskID
	}
	if t.heapSorterID != other.heapSorterID {
		return t.heapSorterID < other.heapSorterID
	}

	if t != other {
		panic("unreachable")
	}

	// We are truly comparing the task with itself,
	// so by definition we return false, since one element
	// cannot be less than itself.
	return false
}
