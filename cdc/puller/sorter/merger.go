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
	"go.uber.org/zap"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

func runMerger(ctx context.Context, numSorters int, in <-chan *flushTask, out chan *model.PolymorphicEvent) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	_, tableName := util.TableIDFromCtx(ctx)

	metricSorterEventCount := sorterEventCount.MustCurryWith(map[string]string{
		"capture":    captureAddr,
		"changefeed": changefeedID,
		"table":      tableName})
	metricSorterResolvedTsGauge := sorterResolvedTsGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricSorterMergerStartTsGauge := sorterMergerStartTsGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricSorterMergeCountHistogram := sorterMergeCountHistogram.WithLabelValues(captureAddr, changefeedID, tableName)

	lastResolvedTs := make([]uint64, numSorters)
	minResolvedTs := uint64(0)

	pendingSet := make(map[*flushTask]*model.PolymorphicEvent)
	defer func() {
		log.Info("Unified Sorter: merger exiting, cleaning up resources", zap.Int("pending-set-size", len(pendingSet)))
		// clean up resources
		for task := range pendingSet {
			err := printError(task.dealloc())
			if err != nil && task.backend != nil {
				_ = task.backend.free()
			}
		}
	}()

	lastOutputTs := uint64(0)
	var lastEvent *model.PolymorphicEvent
	var lastTask *flushTask

	sendResolvedEvent := func(ts uint64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- model.NewResolvedPolymorphicEvent(0, ts):
			metricSorterEventCount.WithLabelValues("resolved").Inc()
			metricSorterResolvedTsGauge.Set(float64(ts))
			return nil
		}
	}

	onMinResolvedTsUpdate := func() error {
		metricSorterMergerStartTsGauge.Set(float64(minResolvedTs))

		workingSet := make(map[*flushTask]struct{})
		sortHeap := new(sortHeap)

		defer func() {
			// clean up
			for task := range workingSet {
				select {
				case <-ctx.Done():
					break
				case err := <-task.finished:
					_ = printError(err)
				}

				if task.reader != nil {
					err := task.reader.resetAndClose()
					task.reader = nil
					_ = printError(err)
				}
				err := task.dealloc()
				if printError(err) != nil {
					_ = task.backend.free()
				}
			}
		}()

		for task, cache := range pendingSet {
			if task.tsLowerBound > minResolvedTs {
				// the condition above implies that for any event in task.backend, CRTs > minResolvedTs.
				continue
			}
			var event *model.PolymorphicEvent
			if cache != nil {
				event = cache
			} else {
				var err error

				select {
				case <-ctx.Done():
					return ctx.Err()
				case err := <-task.finished:
					if err != nil {
						return errors.Trace(err)
					}
				}

				if task.reader == nil {
					task.reader, err = task.backend.reader()
					if err != nil {
						return errors.Trace(err)
					}
				}

				event, err = task.reader.readNext()
				if err != nil {
					return errors.Trace(err)
				}

				if event == nil {
					log.Fatal("Unexpected end of backEnd data, bug?",
						zap.Uint64("minResolvedTs", task.maxResolvedTs))
				}
			}

			if event.CRTs > minResolvedTs {
				pendingSet[task] = event
				continue
			}

			pendingSet[task] = nil
			workingSet[task] = struct{}{}

			heap.Push(sortHeap, &sortItem{
				entry: event,
				data:  task,
			})
		}

		resolvedTicker := time.NewTicker(1 * time.Second)
		defer resolvedTicker.Stop()

		retire := func(task *flushTask) error {
			delete(workingSet, task)
			if pendingSet[task] != nil {
				return nil
			}
			nextEvent, err := task.reader.readNext()
			if err != nil {
				_ = task.reader.resetAndClose() // prevents fd leak
				return errors.Trace(err)
			}

			if nextEvent == nil {
				delete(pendingSet, task)

				err := task.reader.resetAndClose()
				if err != nil {
					return errors.Trace(err)
				}

				err = task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				pendingSet[task] = nextEvent
				if nextEvent.CRTs < minResolvedTs {
					log.Fatal("remaining event CRTs too small", zap.Uint64("next-ts", nextEvent.CRTs), zap.Uint64("minResolvedTs", minResolvedTs))
				}
			}
			return nil
		}

		if sortHeap.Len() > 0 {
			log.Debug("Unified Sorter: start merging",
				zap.String("table", tableNameFromCtx(ctx)),
				zap.Uint64("minResolvedTs", minResolvedTs))
		}

		counter := 0
		for sortHeap.Len() > 0 {
			item := heap.Pop(sortHeap).(*sortItem)
			task := item.data.(*flushTask)
			event := item.entry

			if event.CRTs < task.lastTs {
				log.Fatal("unified sorter: ts regressed in one backEnd, bug?", zap.Uint64("cur-ts", event.CRTs), zap.Uint64("last-ts", task.lastTs))
			}
			task.lastTs = event.CRTs

			if event.RawKV != nil && event.RawKV.OpType != model.OpTypeResolved {
				if event.CRTs < lastOutputTs {
					for sortHeap.Len() > 0 {
						item := heap.Pop(sortHeap).(*sortItem)
						task := item.data.(*flushTask)
						event := item.entry
						log.Debug("dump", zap.Reflect("event", event), zap.Int("heap-id", task.heapSorterID))
					}
					log.Fatal("unified sorter: output ts regressed, bug?",
						zap.Int("counter", counter),
						zap.Uint64("minResolvedTs", minResolvedTs),
						zap.Int("cur-heap-id", task.heapSorterID),
						zap.Int("cur-task-id", task.taskID),
						zap.Uint64("cur-task-resolved", task.maxResolvedTs),
						zap.Reflect("cur-event", event),
						zap.Uint64("cur-ts", event.CRTs),
						zap.Int("last-heap-id", lastTask.heapSorterID),
						zap.Int("last-task-id", lastTask.taskID),
						zap.Uint64("last-task-resolved", task.maxResolvedTs),
						zap.Reflect("last-event", lastEvent),
						zap.Uint64("last-ts", lastOutputTs),
						zap.Int("sort-heap-len", sortHeap.Len()))
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
				delete(pendingSet, task)

				err := task.reader.resetAndClose()
				if err != nil {
					return errors.Trace(err)
				}

				err = task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}

				continue
			}

			if event.CRTs > minResolvedTs || (event.CRTs == minResolvedTs && event.RawKV.OpType == model.OpTypeResolved) {
				// we have processed all events from this task that need to be processed in this merge
				if event.CRTs > minResolvedTs || event.RawKV.OpType != model.OpTypeResolved {
					pendingSet[task] = event
				}
				err := retire(task)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}

			if counter%10 == 0 {
				log.Debug("Merging progress",
					zap.String("table", tableNameFromCtx(ctx)),
					zap.Int("counter", counter))
			}

			heap.Push(sortHeap, &sortItem{
				entry: event,
				data:  task,
			})
		}

		if len(workingSet) != 0 {
			log.Fatal("unified sorter: merging ended prematurely, bug?", zap.Uint64("resolvedTs", minResolvedTs))
		}

		if counter > 0 {
			log.Debug("Unified Sorter: merging ended",
				zap.String("table", tableNameFromCtx(ctx)),
				zap.Uint64("resolvedTs", minResolvedTs), zap.Int("count", counter))
		}
		err := sendResolvedEvent(minResolvedTs)
		if err != nil {
			return errors.Trace(err)
		}

		metricSorterMergeCountHistogram.Observe(float64(counter))

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-in:
			if task == nil {
				log.Info("Merger input channel closed, exiting",
					zap.String("table", tableNameFromCtx(ctx)),
					zap.Uint64("max-output", minResolvedTs))
				return nil
			}

			if task.backend != nil {
				pendingSet[task] = nil
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
				minResolvedTs = minTemp
				err := onMinResolvedTsUpdate()
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func mergerCleanUp(in <-chan *flushTask) {
	for task := range in {
		err := printError(task.dealloc())
		if err != nil && task.backend != nil {
			_ = task.backend.free()
		}
	}
}

// printError is a helper for tracing errors on function returns
func printError(err error) error {
	if err != nil && errors.Cause(err) != context.Canceled &&
		errors.Cause(err) != context.DeadlineExceeded &&
		!strings.Contains(err.Error(), "context canceled") &&
		!strings.Contains(err.Error(), "context deadline exceeded") {

		log.Warn("Unified Sorter: Error detected", zap.Error(err))
	}
	return err
}
