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

package sorter

import (
	"container/heap"
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/workerpool"
	"go.uber.org/zap"
)

const (
	flushRateLimitPerSecond = 10
)

type flushTask struct {
	taskID        int
	heapSorterID  int
	backend       backEnd
	reader        backEndReader
	tsLowerBound  uint64
	maxResolvedTs uint64
	finished      chan error
	dealloc       func() error
	dataSize      int64
	lastTs        uint64 // for debugging TODO remove
}

type heapSorter struct {
	id          int
	taskCounter int
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *flushTask
	heap        sortHeap

	runtimeState *heapSorterRuntimeState
}

func newHeapSorter(id int, out chan *flushTask) *heapSorter {
	return &heapSorter{
		id:       id,
		inputCh:  make(chan *model.PolymorphicEvent, 1024*1024),
		outputCh: out,
		heap:     make(sortHeap, 0, 65536),
	}
}

// flush should only be called within the main loop in run().
func (h *heapSorter) flush(ctx context.Context, maxResolvedTs uint64) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	_, tableName := util.TableIDFromCtx(ctx)
	sorterFlushCountHistogram.WithLabelValues(captureAddr, changefeedID, tableName).Observe(float64(h.heap.Len()))

	if h.heap.Len() == 1 && h.heap[0].entry.RawKV.OpType == model.OpTypeResolved {
		h.heap.Pop()
	}

	isEmptyFlush := h.heap.Len() == 0
	var (
		backEnd    backEnd
		lowerBound uint64
	)

	var finishCh chan error
	if !isEmptyFlush {
		var err error
		backEnd, err = pool.alloc(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		lowerBound = h.heap[0].entry.CRTs
		finishCh = make(chan error, 1)
	}

	task := &flushTask{
		taskID:        h.taskCounter,
		heapSorterID:  h.id,
		backend:       backEnd,
		tsLowerBound:  lowerBound,
		maxResolvedTs: maxResolvedTs,
		finished:      finishCh,
	}
	h.taskCounter++

	var oldHeap sortHeap
	if !isEmptyFlush {
		task.dealloc = func() error {
			if task.backend != nil {
				task.backend = nil
				return pool.dealloc(backEnd)
			}
			return nil
		}
		oldHeap = h.heap
		h.heap = make(sortHeap, 0, 65536)
	} else {
		task.dealloc = func() error {
			return nil
		}
	}
	failpoint.Inject("sorterDebug", func() {
		log.Debug("Unified Sorter new flushTask",
			zap.String("table", tableNameFromCtx(ctx)),
			zap.Int("heap-id", task.heapSorterID),
			zap.Uint64("resolvedTs", task.maxResolvedTs))
	})

	if !isEmptyFlush {
		backEndFinal := backEnd
		err := heapSorterIOPool.Go(ctx, func() {
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

			for oldHeap.Len() > 0 {
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
				log.Debug("Unified Sorter flushTask finished",
					zap.Int("heap-id", task.heapSorterID),
					zap.String("table", tableNameFromCtx(ctx)),
					zap.Uint64("resolvedTs", task.maxResolvedTs),
					zap.Uint64("data-size", dataSize),
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
	heapSorterPool   = workerpool.NewDefaultWorkerPool()
	heapSorterIOPool = workerpool.NewDefaultAsyncPool()
)

type heapSorterRuntimeState struct {
	maxResolved           uint64
	heapSizeBytesEstimate int64
	rateCounter           int
	sorterConfig          *config.SorterConfig
	poolHandle            workerpool.EventHandle
	timerMultiplier       int
}

func (h *heapSorter) init(ctx context.Context, onError func(err error)) {
	state := &heapSorterRuntimeState{
		sorterConfig: config.GetSorterConfig(),
	}

	poolHandle := heapSorterPool.RegisterEvent(func(ctx context.Context, eventI interface{}) error {
		event := eventI.(*model.PolymorphicEvent)
		heap.Push(&h.heap, &sortItem{entry: event})
		isResolvedEvent := event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved

		if isResolvedEvent {
			if event.RawKV.CRTs < state.maxResolved {
				log.Panic("ResolvedTs regression, bug?", zap.Uint64("event-resolvedTs", event.RawKV.CRTs),
					zap.Uint64("max-resolvedTs", state.maxResolved))
			}
			state.maxResolved = event.RawKV.CRTs
		}

		if event.RawKV.CRTs < state.maxResolved {
			log.Panic("Bad input to sorter", zap.Uint64("cur-ts", event.RawKV.CRTs), zap.Uint64("maxResolved", state.maxResolved))
		}

		// 5 * 8 is for the 5 fields in PolymorphicEvent
		state.heapSizeBytesEstimate += event.RawKV.ApproximateSize() + 40
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

	state.poolHandle = poolHandle
	h.runtimeState = state
}
