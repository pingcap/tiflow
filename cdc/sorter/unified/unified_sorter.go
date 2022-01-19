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
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"golang.org/x/sync/errgroup"
)

const (
	inputChSize       = 128
	outputChSize      = 128
	heapCollectChSize = 128 // this should be not be too small, to guarantee IO concurrency
)

// Sorter provides both sorting in memory and in file. Memory pressure is used to determine which one to use.
type Sorter struct {
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *model.PolymorphicEvent
	dir         string
	metricsInfo *metricsInfo

	closeCh chan struct{}
}

type metricsInfo struct {
	changeFeedID model.ChangeFeedID
	tableName    string
	tableID      model.TableID
	captureAddr  string
}

type ctxKey struct{}

// NewUnifiedSorter creates a new Sorter
func NewUnifiedSorter(
	dir string,
	changeFeedID model.ChangeFeedID,
	tableName string,
	tableID model.TableID,
	captureAddr string) (*Sorter, error) {
	poolMu.Lock()
	defer poolMu.Unlock()

	if pool == nil {
		var err error
		pool, err = newBackEndPool(dir, captureAddr)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	lazyInitWorkerPool()
	return &Sorter{
		inputCh:  make(chan *model.PolymorphicEvent, inputChSize),
		outputCh: make(chan *model.PolymorphicEvent, outputChSize),
		dir:      dir,
		metricsInfo: &metricsInfo{
			changeFeedID: changeFeedID,
			tableName:    tableName,
			tableID:      tableID,
			captureAddr:  captureAddr,
		},
		closeCh: make(chan struct{}, 1),
	}, nil
}

// CleanUp cleans up the files that might have been used.
func CleanUp() {
	poolMu.Lock()
	defer poolMu.Unlock()

	if pool != nil {
		log.Info("Unified Sorter: starting cleaning up files")
		pool.terminate()
		pool = nil
	}
}

// ResetGlobalPoolWithoutCleanup reset the pool without cleaning up files.
// Note that it is used in tests only.
func ResetGlobalPoolWithoutCleanup() {
	poolMu.Lock()
	defer poolMu.Unlock()

	pool = nil
}

// Run implements the EventSorter interface
func (s *Sorter) Run(ctx context.Context) error {
	failpoint.Inject("sorterDebug", func() {
		log.Info("sorterDebug: Running Unified Sorter in debug mode")
	})

	defer close(s.closeCh)

	finish, startCancel := util.MonitorCancelLatency(ctx, "Unified Sorter")
	defer finish()

	ctx = context.WithValue(ctx, ctxKey{}, s)
	ctx = util.PutCaptureAddrInCtx(ctx, s.metricsInfo.captureAddr)
	ctx = util.PutChangefeedIDInCtx(ctx, s.metricsInfo.changeFeedID)
	ctx = util.PutTableInfoInCtx(ctx, s.metricsInfo.tableID, s.metricsInfo.tableName)

	sorterConfig := config.GetGlobalServerConfig().Sorter
	numConcurrentHeaps := sorterConfig.NumConcurrentWorker

	errg, subctx := errgroup.WithContext(ctx)
	heapSorterCollectCh := make(chan *flushTask, heapCollectChSize)
	// mergerCleanUp will consumer the remaining elements in heapSorterCollectCh to prevent any FD leak.
	defer mergerCleanUp(heapSorterCollectCh)

	heapSorterErrCh := make(chan error, 1)
	defer close(heapSorterErrCh)
	heapSorterErrOnce := &sync.Once{}
	heapSorters := make([]*heapSorter, sorterConfig.NumConcurrentWorker)
	for i := range heapSorters {
		heapSorters[i] = newHeapSorter(i, heapSorterCollectCh)
		heapSorters[i].init(subctx, func(err error) {
			heapSorterErrOnce.Do(func() {
				heapSorterErrCh <- err
			})
		})
	}

	ioCancelFunc := func() {
		for _, heapSorter := range heapSorters {
			// cancels async IO operations
			heapSorter.canceller.Cancel()
		}
	}

	errg.Go(func() error {
		defer func() {
			// cancelling the heapSorters from the outside
			for _, hs := range heapSorters {
				hs.poolHandle.Unregister()
			}
			// must wait for all writers to exit to close the channel.
			close(heapSorterCollectCh)
			failpoint.Inject("InjectHeapSorterExitDelay", func() {})
		}()

		select {
		case <-subctx.Done():
			startCancel()
			return errors.Trace(subctx.Err())
		case err := <-heapSorterErrCh:
			return errors.Trace(err)
		}
	})

	errg.Go(func() error {
		return printError(runMerger(subctx, numConcurrentHeaps, heapSorterCollectCh, s.outputCh, ioCancelFunc))
	})

	errg.Go(func() error {
		captureAddr := util.CaptureAddrFromCtx(ctx)
		changefeedID := util.ChangefeedIDFromCtx(ctx)

		metricSorterConsumeCount := sorterConsumeCount.MustCurryWith(map[string]string{
			"capture":    captureAddr,
			"changefeed": changefeedID,
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
						if cerror.ErrWorkerPoolHandleCancelled.Equal(err) {
							// no need to report ErrWorkerPoolHandleCancelled,
							// as it may confuse the user
							return nil
						}
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
						if cerror.ErrWorkerPoolHandleCancelled.Equal(err) {
							// no need to report ErrWorkerPoolHandleCancelled,
							// as it may confuse the user
							return nil
						}
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
func (s *Sorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case <-s.closeCh:
	case s.inputCh <- entry:
	}
}

// TryAddEntry implements the EventSorter interface
func (s *Sorter) TryAddEntry(ctx context.Context, entry *model.PolymorphicEvent) (bool, error) {
	// add two select to guarantee the done/close condition is checked first.
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.closeCh:
		return false, cerror.ErrSorterClosed.GenWithStackByArgs()
	default:
	}
	select {
	case s.inputCh <- entry:
		return true, nil
	default:
		return false, nil
	}
}

// Output implements the EventSorter interface
func (s *Sorter) Output() <-chan *model.PolymorphicEvent {
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
