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
	"go.uber.org/zap"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

func runMerger(ctx context.Context, numSorters int, in <-chan *flushTask, out chan *model.PolymorphicEvent) error {
	lastResolvedTs := make([]uint64, numSorters)
	minResolvedTs := uint64(0)

	pendingSet := make(map[*flushTask]*model.PolymorphicEvent)
	defer func() {
		log.Info("Unified Sorter: merger exiting, cleaning up resources", zap.Int("pending-set-size", len(pendingSet)))
		// clean up resources
		for task, _ := range pendingSet {
			err := printError(task.dealloc())
			if err != nil && task.backend != nil {
				_ = task.backend.free()
			}
		}
	}()

	lastOutputTs := uint64(0)

	sendResolvedEvent := func(ts uint64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- model.NewResolvedPolymorphicEvent(0, ts):
			return nil
		}
	}

	onMinResolvedTsUpdate := func() error {
		workingSet := make(map[*flushTask]struct{})
		sortHeap := new(sortHeap)

		defer func() {
			// clean up
			for _, item := range *sortHeap {
				if item != nil {
					if task, ok := item.data.(*flushTask); ok {
						err := printError(task.dealloc())
						if err != nil {
							_ = task.backend.free()
						}
					}
				}
			}
		}()

		for task, cache := range pendingSet {
			if task.maxResolvedTs <= minResolvedTs {
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
				_ = task.reader.resetAndClose()   // prevents fd leak
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

			if event.RawKV != nil && event.RawKV.OpType != model.OpTypeResolved {
				if event.CRTs < lastOutputTs {
					log.Fatal("unified sorter: output ts regressed, bug?", zap.Uint64("cur-ts", event.CRTs), zap.Uint64("last-ts", lastOutputTs))
				}
				lastOutputTs = event.CRTs
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- event:
				}
			}
			counter += 1

			if event.CRTs < task.lastTs {
				log.Fatal("unified sorter: ts regressed in one backEnd, bug?", zap.Uint64("cur-ts", event.CRTs), zap.Uint64("last-ts", task.lastTs))
			}
			task.lastTs = event.CRTs

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

				err := task.dealloc()
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

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-in:
			if task == nil {
				return errors.New("Unified Sorter: nil flushTask, exiting")
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
