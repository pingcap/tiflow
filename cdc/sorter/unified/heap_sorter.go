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

package unified

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/zap"
)

const (
	flushRateLimitPerSecond = 10
	sortHeapCapacity        = 32
	sortHeapInputChSize     = 1024
)

type flushTask struct {
	taskID        int
	heapSorterID  int
	reader        backEndReader
	tsLowerBound  uint64
	maxResolvedTs uint64
	finished      chan error
	dealloc       func() error
	dataSize      int64
	lastTs        uint64 // for debugging TODO remove
	canceller     *asyncCanceller

	isEmpty bool // read only field

	deallocLock   sync.RWMutex
	isDeallocated bool    // do not access directly
	backend       backEnd // do not access directly
}

func (t *flushTask) markDeallocated() {
	t.deallocLock.Lock()
	defer t.deallocLock.Unlock()

	t.backend = nil
	t.isDeallocated = true
}

func (t *flushTask) GetBackEnd() backEnd {
	t.deallocLock.RLock()
	defer t.deallocLock.RUnlock()

	return t.backend
}

type heapSorter struct {
	id          int
	taskCounter int
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *flushTask
	heap        sortHeap
	canceller   *asyncCanceller

	poolHandle    workerpool.EventHandle
	internalState *heapSorterInternalState
}

func newHeapSorter(id int, out chan *flushTask) *heapSorter {
	return &heapSorter{
		id:        id,
		inputCh:   make(chan *model.PolymorphicEvent, sortHeapInputChSize),
		outputCh:  out,
		heap:      make(sortHeap, 0, sortHeapCapacity),
		canceller: new(asyncCanceller),
	}
}

// flush should only be called in the same goroutine where the heap is being written to.
func (h *heapSorter) flush(ctx context.Context, maxResolvedTs uint64) error {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	var (
		backEnd    backEnd
		lowerBound uint64
	)

	if h.heap.Len() > 0 {
		lowerBound = h.heap[0].entry.CRTs
	} else {
		return nil
	}

	sorterFlushCountHistogram.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID).
		Observe(float64(h.heap.Len()))

	// We check if the heap contains only one entry and that entry is a ResolvedEvent.
	// As an optimization, when the condition is true, we clear the heap and send an empty flush.
	// Sending an empty flush saves CPU and potentially IO.
	// Since when a table is mostly idle or near-idle, most flushes would contain one ResolvedEvent alone,
	// this optimization will greatly improve performance when (1) total number of table is large,
	// and (2) most tables do not have many events.
	if h.heap.Len() == 1 && h.heap[0].entry.IsResolved() {
		h.heap.Pop()
	}

	isEmptyFlush := h.heap.Len() == 0
	var finishCh chan error
	if !isEmptyFlush {
		failpoint.Inject("InjectErrorBackEndAlloc", func() {
			failpoint.Return(cerrors.ErrUnifiedSorterIOError.Wrap(errors.New("injected alloc error")).FastGenWithCause())
		})

		var err error
		backEnd, err = pool.alloc(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		finishCh = make(chan error, 1)
	}

	task := &flushTask{
		taskID:        h.taskCounter,
		heapSorterID:  h.id,
		backend:       backEnd,
		tsLowerBound:  lowerBound,
		maxResolvedTs: maxResolvedTs,
		finished:      finishCh,
		canceller:     h.canceller,
		isEmpty:       isEmptyFlush,
	}
	h.taskCounter++

	var oldHeap sortHeap
	if !isEmptyFlush {
		task.dealloc = func() error {
			backEnd := task.GetBackEnd()
			if backEnd != nil {
				defer task.markDeallocated()
				return pool.dealloc(backEnd)
			}
			return nil
		}
		oldHeap = h.heap
		h.heap = make(sortHeap, 0, sortHeapCapacity)
	} else {
		task.dealloc = func() error {
			task.markDeallocated()
			return nil
		}
	}
	failpoint.Inject("sorterDebug", func() {
		tableID, tableName := contextutil.TableIDFromCtx(ctx)
		log.Debug("Unified Sorter new flushTask",
			zap.Int64("tableID", tableID),
			zap.String("tableName", tableName),
			zap.Int("heapID", task.heapSorterID),
			zap.Uint64("resolvedTs", task.maxResolvedTs))
	})

	if !isEmptyFlush {
		backEndFinal := backEnd
		err := heapSorterIOPool.Go(ctx, func() {
			failpoint.Inject("asyncFlushStartDelay", func() {
				log.Debug("asyncFlushStartDelay")
			})

			h.canceller.EnterAsyncOp()
			defer h.canceller.FinishAsyncOp()

			if h.canceller.IsCanceled() {
				if backEndFinal != nil {
					_ = task.dealloc()
				}
				task.finished <- cerrors.ErrAsyncIOCancelled.GenWithStackByArgs()
				return
			}

			writer, err := backEnd.writer()
			if err != nil {
				if backEndFinal != nil {
					_ = task.dealloc()
				}
				task.finished <- errors.Trace(err)
				return
			}

			defer func() {
				// handle errors (or aborts) gracefully to prevent resource leaking (especially FD's)
				if writer != nil {
					_ = writer.flushAndClose()
				}
				if backEndFinal != nil {
					_ = task.dealloc()
				}
				close(task.finished)
			}()

			failpoint.Inject("InjectErrorBackEndWrite", func() {
				task.finished <- cerrors.ErrUnifiedSorterIOError.Wrap(errors.New("injected write error")).FastGenWithCause()
				failpoint.Return()
			})

			counter := 0
			for oldHeap.Len() > 0 {
				failpoint.Inject("asyncFlushInProcessDelay", func() {
					log.Debug("asyncFlushInProcessDelay")
				})
				// no need to check for cancellation so frequently.
				if counter%10000 == 0 && h.canceller.IsCanceled() {
					task.finished <- cerrors.ErrAsyncIOCancelled.GenWithStackByArgs()
					return
				}
				counter++

				event := heap.Pop(&oldHeap).(*sortItem).entry
				err := writer.writeNext(event)
				if err != nil {
					task.finished <- errors.Trace(err)
					return
				}
			}

			dataSize := writer.dataSize()
			atomic.StoreInt64(&task.dataSize, int64(dataSize))
			eventCount := writer.writtenCount()

			writer1 := writer
			writer = nil
			err = writer1.flushAndClose()
			if err != nil {
				task.finished <- errors.Trace(err)
				return
			}

			backEndFinal = nil

			failpoint.Inject("sorterDebug", func() {
				tableID, tableName := contextutil.TableIDFromCtx(ctx)
				log.Debug("Unified Sorter flushTask finished",
					zap.Int("heapID", task.heapSorterID),
					zap.Int64("tableID", tableID),
					zap.String("tableName", tableName),
					zap.Uint64("resolvedTs", task.maxResolvedTs),
					zap.Uint64("dataSize", dataSize),
					zap.Int("size", eventCount))
			})

			task.finished <- nil // DO NOT access `task` beyond this point in this function
		})
		if err != nil {
			close(task.finished)
			return errors.Trace(err)
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.outputCh <- task:
	}
	return nil
}

var (
	heapSorterPool   workerpool.WorkerPool
	heapSorterIOPool workerpool.AsyncPool
	poolOnce         sync.Once
)

type heapSorterInternalState struct {
	maxResolved           uint64
	heapSizeBytesEstimate int64
	rateCounter           int
	sorterConfig          *config.SorterConfig
	timerMultiplier       int
}

func (h *heapSorter) init(ctx context.Context, onError func(err error)) {
	state := &heapSorterInternalState{
		sorterConfig: config.GetGlobalServerConfig().Sorter,
	}

	poolHandle := heapSorterPool.RegisterEvent(func(ctx context.Context, eventI interface{}) error {
		event := eventI.(*model.PolymorphicEvent)
		heap.Push(&h.heap, &sortItem{entry: event})
		isResolvedEvent := event.RawKV != nil && event.IsResolved()

		if isResolvedEvent {
			if event.RawKV.CRTs < state.maxResolved {
				log.Panic("ResolvedTs regression, bug?", zap.Uint64("resolvedTs", event.RawKV.CRTs),
					zap.Uint64("maxResolvedTs", state.maxResolved))
			}
			state.maxResolved = event.RawKV.CRTs
		}

		if event.RawKV.CRTs < state.maxResolved {
			log.Panic("Bad input to sorter", zap.Uint64("curTs", event.RawKV.CRTs), zap.Uint64("maxResolved", state.maxResolved))
		}

		// 5 * 8 is for the 5 fields in PolymorphicEvent
		state.heapSizeBytesEstimate += event.RawKV.ApproximateDataSize() + 40
		needFlush := state.heapSizeBytesEstimate >= int64(state.sorterConfig.ChunkSizeLimit) ||
			(isResolvedEvent && state.rateCounter < flushRateLimitPerSecond)

		if needFlush {
			state.rateCounter++
			err := h.flush(ctx, state.maxResolved)
			if err != nil {
				return errors.Trace(err)
			}
			state.heapSizeBytesEstimate = 0
		}

		return nil
	}).SetTimer(ctx, 1*time.Second, func(ctx context.Context) error {
		state.rateCounter = 0
		state.timerMultiplier = (state.timerMultiplier + 1) % 5
		if state.timerMultiplier == 0 && state.rateCounter < flushRateLimitPerSecond {
			err := h.flush(ctx, state.maxResolved)
			if err != nil {
				return errors.Trace(err)
			}
			state.heapSizeBytesEstimate = 0
		}
		return nil
	}).OnExit(onError)

	h.poolHandle = poolHandle
	h.internalState = state
}

// asyncCanceller is a shared object used to cancel async IO operations.
// We do not use `context.Context` because (1) selecting on `ctx.Done()` is expensive
// especially if the context is shared by many goroutines, and (2) due to the complexity
// of managing contexts through the workerpools, using a special shared object seems more reasonable
// and readable.
type asyncCanceller struct {
	exitRWLock sync.RWMutex // held when an asynchronous flush is taking place
	hasExited  int32        // this flag should be accessed atomically
}

func (c *asyncCanceller) EnterAsyncOp() {
	c.exitRWLock.RLock()
}

func (c *asyncCanceller) FinishAsyncOp() {
	c.exitRWLock.RUnlock()
}

func (c *asyncCanceller) IsCanceled() bool {
	return atomic.LoadInt32(&c.hasExited) == 1
}

func (c *asyncCanceller) Cancel() {
	// Sets the flag
	atomic.StoreInt32(&c.hasExited, 1)

	// By taking the lock, we are making sure that all IO operations that started before setting the flag have finished,
	// so that by the returning of this function, no more IO operations will finish successfully.
	// Since IO operations that are NOT successful will clean up themselves, the goroutine in which this
	// function was called is responsible for releasing files written by only those IO operations that complete BEFORE
	// this function returns.
	// In short, we are creating a linearization point here.
	c.exitRWLock.Lock()
	defer c.exitRWLock.Unlock()
}

func lazyInitWorkerPool() {
	poolOnce.Do(func() {
		sorterConfig := config.GetGlobalServerConfig().Sorter
		heapSorterPool = workerpool.NewDefaultWorkerPool(sorterConfig.NumWorkerPoolGoroutine)
		heapSorterIOPool = workerpool.NewDefaultAsyncPool(sorterConfig.NumWorkerPoolGoroutine * 2)
	})
}
