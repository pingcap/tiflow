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
	"github.com/pingcap/ticdc/pkg/util"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
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
	backEndPool *backEndPool
}

func newHeapSorter(id int, pool *backEndPool, out chan *flushTask) *heapSorter {
	return &heapSorter{
		id:          id,
		inputCh:     make(chan *model.PolymorphicEvent, 1024*1024),
		outputCh:    out,
		heap:        make(sortHeap, 0, 65536),
		backEndPool: pool,
	}
}

// flush should only be called within the main loop in run().
func (h *heapSorter) flush(ctx context.Context, maxResolvedTs uint64) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	_, tableName := util.TableIDFromCtx(ctx)
	sorterFlushCountHistogram.WithLabelValues(captureAddr, changefeedID, tableName).Observe(float64(h.heap.Len()))

	isEmptyFlush := h.heap.Len() == 0
	if isEmptyFlush {
		return nil
	}
	var (
		backEnd    backEnd
		lowerBound uint64
	)

	if !isEmptyFlush {
		var err error
		backEnd, err = h.backEndPool.alloc(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		lowerBound = h.heap[0].entry.CRTs
	}

	task := &flushTask{
		taskID:        h.taskCounter,
		heapSorterID:  h.id,
		backend:       backEnd,
		tsLowerBound:  lowerBound,
		maxResolvedTs: maxResolvedTs,
		finished:      make(chan error, 2),
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

	log.Debug("Unified Sorter new flushTask",
		zap.String("table", tableNameFromCtx(ctx)),
		zap.Int("heap-id", task.heapSorterID),
		zap.Uint64("resolvedTs", task.maxResolvedTs))

	go func() {
		if isEmptyFlush {
			return
		}
		backEndFinal := backEnd
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
			select {
			case <-ctx.Done():
				task.finished <- ctx.Err()
			default:
			}

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
		task.finished <- nil // DO NOT access `task` beyond this point in this function
		log.Debug("Unified Sorter flushTask finished",
			zap.Int("heap-id", task.heapSorterID),
			zap.String("table", tableNameFromCtx(ctx)),
			zap.Uint64("resolvedTs", task.maxResolvedTs),
			zap.Uint64("data-size", dataSize),
			zap.Int("size", eventCount))
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.outputCh <- task:
	}
	return nil
}

func (h *heapSorter) run(ctx context.Context) error {
	var (
		maxResolved           uint64
		heapSizeBytesEstimate int64
		rateCounter           int
	)

	rateTicker := time.NewTicker(1 * time.Second)
	defer rateTicker.Stop()

	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	sorterConfig := config.GetSorterConfig()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-h.inputCh:
			heap.Push(&h.heap, &sortItem{entry: event})
			isResolvedEvent := event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved

			if isResolvedEvent {
				if event.RawKV.CRTs < maxResolved {
					log.Fatal("ResolvedTs regression, bug?", zap.Uint64("event-resolvedTs", event.RawKV.CRTs),
						zap.Uint64("max-resolvedTs", maxResolved))
				}
				maxResolved = event.RawKV.CRTs
			}

			if event.RawKV.CRTs < maxResolved {
				log.Fatal("Bad input to sorter", zap.Uint64("cur-ts", event.RawKV.CRTs), zap.Uint64("maxResolved", maxResolved))
			}

			// 5 * 8 is for the 5 fields in PolymorphicEvent
			heapSizeBytesEstimate += event.RawKV.ApproximateSize() + 40
			needFlush := heapSizeBytesEstimate >= int64(sorterConfig.ChunkSizeLimit) ||
				(isResolvedEvent && rateCounter < flushRateLimitPerSecond)

			if needFlush {
				rateCounter++
				err := h.flush(ctx, maxResolved)
				if err != nil {
					return errors.Trace(err)
				}
				heapSizeBytesEstimate = 0
			}
		case <-flushTicker.C:
			if rateCounter < flushRateLimitPerSecond {
				err := h.flush(ctx, maxResolved)
				if err != nil {
					return errors.Trace(err)
				}
				heapSizeBytesEstimate = 0
			}
		case <-rateTicker.C:
			rateCounter = 0
		}
	}
}
