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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

// UnifiedSorter provides both sorting in memory and in file. Memory pressure is used to determine which one to use.
type UnifiedSorter struct {
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *model.PolymorphicEvent
	dir         string
	pool        *backEndPool
	metricsInfo *metricsInfo
}

type metricsInfo struct {
	changeFeedID model.ChangeFeedID
	tableName    string
	tableID      model.TableID
	captureAddr  string
}

type ctxKey struct {
}

// NewUnifiedSorter creates a new UnifiedSorter
func NewUnifiedSorter(
	dir string,
	changeFeedID model.ChangeFeedID,
	tableName string,
	tableID model.TableID,
	captureAddr string) *UnifiedSorter {
	poolMu.Lock()
	defer poolMu.Unlock()

	if pool == nil {
		pool = newBackEndPool(dir, captureAddr)
	}

	lazyInitWorkerPool()
	return &UnifiedSorter{
		inputCh:  make(chan *model.PolymorphicEvent, 128000),
		outputCh: make(chan *model.PolymorphicEvent, 128000),
		dir:      dir,
		pool:     pool,
		metricsInfo: &metricsInfo{
			changeFeedID: changeFeedID,
			tableName:    tableName,
			tableID:      tableID,
			captureAddr:  captureAddr,
		},
	}
}

// UnifiedSorterCleanUp cleans up the files that might have been used.
func UnifiedSorterCleanUp() {
	poolMu.Lock()
	defer poolMu.Unlock()

	if pool != nil {
		log.Info("Unified Sorter: starting cleaning up files")
		pool.terminate()
		pool = nil
	}
}

// Run implements the EventSorter interface
func (s *UnifiedSorter) Run(ctx context.Context) error {
	failpoint.Inject("sorterDebug", func() {
		log.Info("sorterDebug: Running Unified Sorter in debug mode")
	})

	finish := util.MonitorCancelLatency(ctx, "Unified Sorter")
	defer finish()

	ctx = context.WithValue(ctx, ctxKey{}, s)
	ctx = util.PutCaptureAddrInCtx(ctx, s.metricsInfo.captureAddr)
	ctx = util.PutChangefeedIDInCtx(ctx, s.metricsInfo.changeFeedID)
	ctx = util.PutTableInfoInCtx(ctx, s.metricsInfo.tableID, s.metricsInfo.tableName)

	sorterConfig := config.GetSorterConfig()
	numConcurrentHeaps := sorterConfig.NumConcurrentWorker

	errg, subctx := errgroup.WithContext(ctx)
	heapSorterCollectCh := make(chan *flushTask, 4096)
	// mergerCleanUp will consumer the remaining elements in heapSorterCollectCh to prevent any FD leak.
	defer mergerCleanUp(heapSorterCollectCh)

	heapSorterErrCh := make(chan error, 1)
	defer close(heapSorterErrCh)
	heapSorterErrOnce := &sync.Once{}
	heapSorters := make([]*heapSorter, sorterConfig.NumConcurrentWorker)
	for i := range heapSorters {
		finalI := i
		heapSorters[finalI] = newHeapSorter(finalI, heapSorterCollectCh)
		heapSorters[finalI].init(subctx, func(err error) {
			heapSorterErrOnce.Do(func() {
				heapSorterErrCh <- err
			})
		})
	}

	errg.Go(func() error {
		defer func() {
			// cancelling the heapSorters from the outside
			for _, hs := range heapSorters {
				hs.poolHandle.Unregister()
			}
			// must wait for all writers to exit to close the channel.
			close(heapSorterCollectCh)
		}()

		for {
			select {
			case <-subctx.Done():
				return errors.Trace(subctx.Err())
			case err := <-heapSorterErrCh:
				return errors.Trace(err)
			}
		}
	})

	errg.Go(func() error {
		return printError(runMerger(subctx, numConcurrentHeaps, heapSorterCollectCh, s.outputCh))
	})

	errg.Go(func() error {
		captureAddr := util.CaptureAddrFromCtx(ctx)
		changefeedID := util.ChangefeedIDFromCtx(ctx)
		_, tableName := util.TableIDFromCtx(ctx)

		metricSorterConsumeCount := sorterConsumeCount.MustCurryWith(map[string]string{
			"capture":    captureAddr,
			"changefeed": changefeedID,
			"table":      tableName,
		})

		nextSorterID := 0
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
						default:
						}
						err := sorter.poolHandle.AddEvent(subctx, event)
						if err != nil {
							return errors.Trace(err)
						}
						metricSorterConsumeCount.WithLabelValues("resolved").Inc()
					}
					continue
				}

				// dispatch a row changed event
				targetID := nextSorterID % numConcurrentHeaps
				nextSorterID++
				select {
				case <-subctx.Done():
					return subctx.Err()
				default:
					err := heapSorters[targetID].poolHandle.AddEvent(subctx, event)
					if err != nil {
						return errors.Trace(err)
					}
					metricSorterConsumeCount.WithLabelValues("kv").Inc()
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

// RunWorkerPool runs the worker pool used by the heapSorters
// It **must** be running for Unified Sorter to work.
func RunWorkerPool(ctx context.Context) error {
	lazyInitWorkerPool()
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return errors.Trace(heapSorterPool.Run(ctx))
	})

	errg.Go(func() error {
		return errors.Trace(heapSorterIOPool.Run(ctx))
	})

	return errors.Trace(errg.Wait())
}
