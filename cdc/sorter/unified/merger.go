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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// TODO refactor this into a struct Merger.
func runMerger(ctx context.Context, numSorters int, in <-chan *flushTask, out chan *model.PolymorphicEvent, onExit func()) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	metricSorterEventCount := sorter.EventCount.MustCurryWith(map[string]string{
		"capture":    captureAddr,
		"changefeed": changefeedID,
	})
	metricSorterResolvedTsGauge := sorter.ResolvedTsGauge.WithLabelValues(captureAddr, changefeedID)
	metricSorterMergerStartTsGauge := sorterMergerStartTsGauge.WithLabelValues(captureAddr, changefeedID)
	metricSorterMergeCountHistogram := sorterMergeCountHistogram.WithLabelValues(captureAddr, changefeedID)

	lastResolvedTs := make([]uint64, numSorters)
	minResolvedTs := uint64(0)
	var workingSet map[*flushTask]struct{}
	pendingSet := &sync.Map{}

	defer func() {
		log.Debug("Unified Sorter: merger exiting, cleaning up resources")
		// cancel pending async IO operations.
		onExit()
		cleanUpTask := func(task *flushTask) {
			select {
			case err := <-task.finished:
				_ = printError(err)
			default:
				// The task has not finished, so we give up.
				// It does not matter because:
				// 1) if the async workerpool has exited, it means the CDC process is exiting, CleanUp will
				// take care of the temp files,
				// 2) if the async workerpool is not exiting, the unfinished tasks will eventually be executed,
				// and by that time, since the `onExit` have canceled them, they will not do any IO and clean up themselves.
				return
			}

			if task.reader != nil {
				_ = printError(task.reader.resetAndClose())
				task.reader = nil
			}
			_ = printError(task.dealloc())
		}

	LOOP:
		for {
			var task *flushTask
			select {
			case task = <-in:
			default:
				break LOOP
			}

			if task == nil {
				log.Debug("Merger exiting, in-channel is exhausted")
				break
			}

			cleanUpTask(task)
		}

		pendingSet.Range(func(task, _ interface{}) bool {
			cleanUpTask(task.(*flushTask))
			return true
		})
		for task := range workingSet {
			cleanUpTask(task)
		}
	}()

	lastOutputTs := uint64(0)
	lastOutputResolvedTs := uint64(0)
	var lastEvent *model.PolymorphicEvent
	var lastTask *flushTask

	sendResolvedEvent := func(ts uint64) error {
		lastOutputResolvedTs = ts
		if ts == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- model.NewResolvedPolymorphicEvent(0, ts):
			metricSorterEventCount.WithLabelValues("resolved").Inc()
			metricSorterResolvedTsGauge.Set(float64(oracle.ExtractPhysical(ts)))
			return nil
		}
	}

	onMinResolvedTsUpdate := func(minResolvedTs /* note the shadowing */ uint64) error {
		metricSorterMergerStartTsGauge.Set(float64(oracle.ExtractPhysical(minResolvedTs)))
		workingSet = make(map[*flushTask]struct{})
		sortHeap := new(sortHeap)

		// loopErr is used to return an error out of the closure taken by `pendingSet.Range`.
		var loopErr error
		// NOTE 1: We can block the closure passed to `pendingSet.Range` WITHOUT worrying about
		// deadlocks because the closure is NOT called with any lock acquired in the implementation
		// of Sync.Map.
		// NOTE 2: It is safe to used `Range` to iterate through the pendingSet, in spite of NOT having
		// a snapshot consistency because (1) pendingSet is updated first before minResolvedTs is updated,
		// which guarantees that useful new flushTasks are not missed, and (2) by design, once minResolvedTs is updated,
		// new flushTasks will satisfy `task.tsLowerBound > minResolvedTs`, and such flushTasks are ignored in
		// the closure.
		pendingSet.Range(func(iTask, iCache interface{}) bool {
			task := iTask.(*flushTask)
			var cache *model.PolymorphicEvent
			if iCache != nil {
				cache = iCache.(*model.PolymorphicEvent)
			}

			if task.tsLowerBound > minResolvedTs {
				// the condition above implies that for any event in task.backend, CRTs > minResolvedTs.
				return true
			}
			var event *model.PolymorphicEvent
			if cache != nil {
				event = cache
			} else {
				select {
				case <-ctx.Done():
					loopErr = ctx.Err()
					// terminates the loop
					return false
				case err := <-task.finished:
					if err != nil {
						loopErr = errors.Trace(err)
						// terminates the loop
						return false
					}
				}

				if task.reader == nil {
					var err error
					task.reader, err = task.GetBackEnd().reader()
					if err != nil {
						loopErr = errors.Trace(err)
						// terminates the loop
						return false
					}
				}

				var err error
				event, err = task.reader.readNext()
				if err != nil {
					loopErr = errors.Trace(err)
					// terminates the loop
					return false
				}

				if event == nil {
					log.Panic("Unexpected end of backEnd data, bug?",
						zap.Uint64("minResolvedTs", task.maxResolvedTs))
				}
			}

			if event.CRTs > minResolvedTs {
				pendingSet.Store(task, event)
				// continues the loop
				return true
			}

			pendingSet.Store(task, nil)
			workingSet[task] = struct{}{}

			heap.Push(sortHeap, &sortItem{
				entry: event,
				data:  task,
			})
			return true
		})
		if loopErr != nil {
			return errors.Trace(loopErr)
		}

		resolvedTicker := time.NewTicker(1 * time.Second)
		defer resolvedTicker.Stop()

		retire := func(task *flushTask) error {
			delete(workingSet, task)
			cached, ok := pendingSet.Load(task)
			if !ok {
				log.Panic("task not found in pendingSet")
			}

			if cached != nil {
				return nil
			}

			nextEvent, err := task.reader.readNext()
			if err != nil {
				_ = task.reader.resetAndClose() // prevents fd leak
				task.reader = nil
				return errors.Trace(err)
			}

			if nextEvent == nil {
				pendingSet.Delete(task)

				err := task.reader.resetAndClose()
				if err != nil {
					return errors.Trace(err)
				}
				task.reader = nil

				err = task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				pendingSet.Store(task, nextEvent)
				if nextEvent.CRTs < minResolvedTs {
					log.Panic("remaining event CRTs too small",
						zap.Uint64("next-ts", nextEvent.CRTs),
						zap.Uint64("minResolvedTs", minResolvedTs))
				}
			}
			return nil
		}

		failpoint.Inject("sorterDebug", func() {
			if sortHeap.Len() > 0 {
				tableID, tableName := util.TableIDFromCtx(ctx)
				log.Debug("Unified Sorter: start merging",
					zap.Int64("tableID", tableID),
					zap.String("tableName", tableName),
					zap.Uint64("minResolvedTs", minResolvedTs))
			}
		})

		counter := 0
		for sortHeap.Len() > 0 {
			failpoint.Inject("sorterMergeDelay", func() {})

			item := heap.Pop(sortHeap).(*sortItem)
			task := item.data.(*flushTask)
			event := item.entry

			if event.CRTs < task.lastTs {
				log.Panic("unified sorter: ts regressed in one backEnd, bug?", zap.Uint64("curTs", event.CRTs), zap.Uint64("lastTs", task.lastTs))
			}
			task.lastTs = event.CRTs

			if event.RawKV != nil && event.RawKV.OpType != model.OpTypeResolved {
				if event.CRTs < lastOutputTs {
					for sortHeap.Len() > 0 {
						item := heap.Pop(sortHeap).(*sortItem)
						task := item.data.(*flushTask)
						event := item.entry
						log.Debug("dump", zap.Reflect("event", event), zap.Int("heapID", task.heapSorterID))
					}
					log.Panic("unified sorter: output ts regressed, bug?",
						zap.Int("counter", counter),
						zap.Uint64("minResolvedTs", minResolvedTs),
						zap.Int("curHeapID", task.heapSorterID),
						zap.Int("curTaskID", task.taskID),
						zap.Uint64("curTaskResolved", task.maxResolvedTs),
						zap.Reflect("curEvent", event),
						zap.Uint64("curTs", event.CRTs),
						zap.Int("lastHeapID", lastTask.heapSorterID),
						zap.Int("lastTaskID", lastTask.taskID),
						zap.Uint64("lastTask-resolved", task.maxResolvedTs),
						zap.Reflect("lastEvent", lastEvent),
						zap.Uint64("lastTs", lastOutputTs),
						zap.Int("sortHeapLen", sortHeap.Len()))
				}

				if event.CRTs <= lastOutputResolvedTs {
					log.Panic("unified sorter: output ts smaller than resolved ts, bug?", zap.Uint64("minResolvedTs", minResolvedTs),
						zap.Uint64("lastOutputResolvedTs", lastOutputResolvedTs), zap.Uint64("event-crts", event.CRTs))
				}
				lastOutputTs = event.CRTs
				lastEvent = event
				lastTask = task
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- event:
					metricSorterEventCount.WithLabelValues("kv").Inc()
				}
			}
			counter += 1

			select {
			case <-resolvedTicker.C:
				err := sendResolvedEvent(event.CRTs - 1)
				if err != nil {
					return errors.Trace(err)
				}
			default:
			}

			event, err := task.reader.readNext()
			if err != nil {
				return errors.Trace(err)
			}

			if event == nil {
				// EOF
				delete(workingSet, task)
				pendingSet.Delete(task)

				err := task.reader.resetAndClose()
				if err != nil {
					return errors.Trace(err)
				}
				task.reader = nil

				err = task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}

				continue
			}

			if event.CRTs > minResolvedTs || (event.CRTs == minResolvedTs && event.RawKV.OpType == model.OpTypeResolved) {
				// we have processed all events from this task that need to be processed in this merge
				if event.CRTs > minResolvedTs || event.RawKV.OpType != model.OpTypeResolved {
					pendingSet.Store(task, event)
				}
				err := retire(task)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}

			failpoint.Inject("sorterDebug", func() {
				if counter%10 == 0 {
					tableID, tableName := util.TableIDFromCtx(ctx)
					log.Debug("Merging progress",
						zap.Int64("tableID", tableID),
						zap.String("tableName", tableName),
						zap.Int("counter", counter))
				}
			})

			heap.Push(sortHeap, &sortItem{
				entry: event,
				data:  task,
			})
		}

		if len(workingSet) != 0 {
			log.Panic("unified sorter: merging ended prematurely, bug?", zap.Uint64("resolvedTs", minResolvedTs))
		}

		failpoint.Inject("sorterDebug", func() {
			if counter > 0 {
				tableID, tableName := util.TableIDFromCtx(ctx)
				log.Debug("Unified Sorter: merging ended",
					zap.Int64("tableID", tableID),
					zap.String("tableName", tableName),
					zap.Uint64("resolvedTs", minResolvedTs), zap.Int("count", counter))
			}
		})
		err := sendResolvedEvent(minResolvedTs)
		if err != nil {
			return errors.Trace(err)
		}

		if counter > 0 {
			// ignore empty merges for better visualization of metrics
			metricSorterMergeCountHistogram.Observe(float64(counter))
		}

		return nil
	}

	resolvedTsNotifierChan := make(chan struct{}, 1)
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		for {
			var task *flushTask
			select {
			case <-ctx.Done():
				return ctx.Err()
			case task = <-in:
			}

			if task == nil {
				tableID, tableName := util.TableIDFromCtx(ctx)
				log.Debug("Merger input channel closed, exiting",
					zap.Int64("tableID", tableID),
					zap.String("tableName", tableName))
				return nil
			}

			if !task.isEmpty {
				pendingSet.Store(task, nil)
			} // otherwise it is an empty flush

			if lastResolvedTs[task.heapSorterID] < task.maxResolvedTs {
				lastResolvedTs[task.heapSorterID] = task.maxResolvedTs
			}

			minTemp := uint64(math.MaxUint64)
			for _, ts := range lastResolvedTs {
				if minTemp > ts {
					minTemp = ts
				}
			}

			if minTemp > minResolvedTs {
				atomic.StoreUint64(&minResolvedTs, minTemp)
				select {
				case resolvedTsNotifierChan <- struct{}{}:
				default:
				}
			}
		}
	})

	errg.Go(func() error {
		resolvedTsTicker := time.NewTicker(time.Second * 1)

		defer resolvedTsTicker.Stop()

		var lastResolvedTs uint64
		resolvedTsTickFunc := func() error {
			curResolvedTs := atomic.LoadUint64(&minResolvedTs)
			if curResolvedTs > lastResolvedTs {
				err := onMinResolvedTsUpdate(curResolvedTs)
				if err != nil {
					return errors.Trace(err)
				}
			} else if curResolvedTs < lastResolvedTs {
				log.Panic("resolved-ts regressed in sorter",
					zap.Uint64("curResolvedTs", curResolvedTs),
					zap.Uint64("lastResolvedTs", lastResolvedTs))
			}
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-resolvedTsTicker.C:
				if err := resolvedTsTickFunc(); err != nil {
					return err
				}
			case <-resolvedTsNotifierChan:
				if err := resolvedTsTickFunc(); err != nil {
					return err
				}
			}
		}
	})

	return errg.Wait()
}

func mergerCleanUp(in <-chan *flushTask) {
	for task := range in {
		select {
		case err := <-task.finished:
			_ = printError(err)
		default:
			break
		}

		if task.reader != nil {
			_ = printError(task.reader.resetAndClose())
		}
		_ = printError(task.dealloc())
	}
}

// printError is a helper for tracing errors on function returns
func printError(err error) error {
	if err != nil && errors.Cause(err) != context.Canceled &&
		errors.Cause(err) != context.DeadlineExceeded &&
		!strings.Contains(err.Error(), "context canceled") &&
		!strings.Contains(err.Error(), "context deadline exceeded") &&
		cerrors.ErrAsyncIOCancelled.NotEqual(errors.Cause(err)) {

		log.Warn("Unified Sorter: Error detected", zap.Error(err), zap.Stack("stack"))
	}
	return err
}
