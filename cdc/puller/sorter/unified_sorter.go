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
	"context"
	"go.uber.org/zap"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

// UnifiedSorter provides both sorting in memory and in file. Memory pressure is used to determine which one to use.
type UnifiedSorter struct {
	inputCh   chan *model.PolymorphicEvent
	outputCh  chan *model.PolymorphicEvent
	dir       string
	pool      *backEndPool
	tableName string // used only for debugging and tracing
}

type ctxKey struct {
}

// NewUnifiedSorter creates a new UnifiedSorter
func NewUnifiedSorter(dir string, tableName string) *UnifiedSorter {
	poolMu.Lock()
	defer poolMu.Unlock()

	if pool == nil {
		pool = newBackEndPool(dir)
	}

	return &UnifiedSorter{
		inputCh:   make(chan *model.PolymorphicEvent, 128000),
		outputCh:  make(chan *model.PolymorphicEvent, 128000),
		dir:       dir,
		pool:      pool,
		tableName: tableName,
	}
}

// Run implements the EventSorter interface
func (s *UnifiedSorter) Run(ctx context.Context) error {
	failpoint.Inject("sorterDebug", func() {
		log.Info("sorterDebug: Running Unified Sorter in debug mode")
	})

	finish := util.MonitorCancelLatency(ctx, "Unified Sorter")
	defer finish()

	valueCtx := context.WithValue(ctx, ctxKey{}, s)

	sorterConfig := config.GetSorterConfig()
	numConcurrentHeaps := sorterConfig.NumConcurrentWorker

	nextSorterID := 0
	heapSorters := make([]*heapSorter, sorterConfig.NumConcurrentWorker)

	errg, subctx := errgroup.WithContext(valueCtx)

	var activeHeapCount int32

	sorterOutCh := make(chan *flushTask, 4096)
	defer mergerCleanUp(sorterOutCh)

	for i := range heapSorters {
		finalI := i
		heapSorters[finalI] = newHeapSorter(finalI, s.pool, sorterOutCh)
		errg.Go(func() error {
			atomic.AddInt32(&activeHeapCount, 1)
			defer func() {
				if atomic.AddInt32(&activeHeapCount, -1) == 0 {
					close(sorterOutCh)
				}
			}()
			return printError(heapSorters[finalI].run(subctx))
		})
	}

	errg.Go(func() error {
		return printError(runMerger(subctx, numConcurrentHeaps, sorterOutCh, s.outputCh))
	})

	errg.Go(func() error {
		for {
			select {
			case <-subctx.Done():
				return subctx.Err()
			case event := <-s.inputCh:
				if event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved {
					// broadcast resolved events
					for _, sorter := range heapSorters {
						select {
						case <-subctx.Done():
							return subctx.Err()
						case sorter.inputCh <- event:
						}
					}
					continue
				}

				// dispatch a row changed event
				targetID := nextSorterID % numConcurrentHeaps
				nextSorterID++
				select {
				case <-subctx.Done():
					return subctx.Err()
				case heapSorters[targetID].inputCh <- event:
				}
			}
		}
	})

	return printError(errg.Wait())
}

// AddEntry implements the EventSorter interface
func (s *UnifiedSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case s.inputCh <- entry:
	}
}

// Output implements the EventSorter interface
func (s *UnifiedSorter) Output() <-chan *model.PolymorphicEvent {
	return s.outputCh
}

// tableNameFromCtx is used for retrieving the table's name from a context within the Unified Sorter
func tableNameFromCtx(ctx context.Context) string {
	if sorter, ok := ctx.Value(ctxKey{}).(*UnifiedSorter); ok {
		return sorter.tableName
	}
	return ""
}

func init() {
	log.Info("Unified Sorter initialized")
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			failpoint.Inject("sorterDebug", func() {
				log.Info("Unified Sorter: open file statistics", zap.Int64("num-open-fd", atomic.LoadInt64(&openFDCount)))
			})
		}
	}()

}
