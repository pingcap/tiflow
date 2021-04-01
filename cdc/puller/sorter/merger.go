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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// TODO refactor this into a struct Merger.
func runMerger(ctx context.Context, numSorters int, in <-chan *flushTask, out chan *model.PolymorphicEvent, onExit func(), bufLen *int64) error {
	// TODO remove bufLenPlaceholder when refactoring
	if bufLen == nil {
		var bufLenPlaceholder int64
		bufLen = &bufLenPlaceholder
	}

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	_, tableName := util.TableIDFromCtx(ctx)

	metricSorterEventCount := sorterEventCount.MustCurryWith(map[string]string{
		"capture":    captureAddr,
		"changefeed": changefeedID,
		"table":      tableName,
	})
	metricSorterResolvedTsGauge := sorterResolvedTsGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricSorterMergerStartTsGauge := sorterMergerStartTsGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricSorterMergeCountHistogram := sorterMergeCountHistogram.WithLabelValues(captureAddr, changefeedID, tableName)

	lastResolvedTs := make([]uint64, numSorters)
	minResolvedTs := uint64(0)
	taskBuf := newTaskBuffer(bufLen)
	var workingSet map[*flushTask]struct{}
	pendingSet := make(map[*flushTask]*model.PolymorphicEvent)

	defer func() {
		log.Info("Unified Sorter: merger exiting, cleaning up resources", zap.Int("pending-set-size", len(pendingSet)))
		taskBuf.setClosed()
		// cancel pending async IO operations.
		onExit()
		cleanUpTask := func(task *flushTask) {
			select {
			case err := <-task.finished:
				_ = printError(err)
			default:
				// The task has not finished, so we give up.
				// It does not matter because:
				// 1) if the async workerpool has exited, it means the CDC process is exiting, UnifiedSorterCleanUp will
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

		for {
			task, err := taskBuf.get(ctx)
			if err != nil {
				_ = printError(err)
				break
			}

			if task == nil {
				log.Debug("Merger exiting, taskBuf is exhausted")
				break
			}

			cleanUpTask(task)
		}

		for task := range pendingSet {
			cleanUpTask(task)
		}
		for task := range workingSet {
			cleanUpTask(task)
		}

		taskBuf.close()
		log.Info("Merger has exited")
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

	onMinResolvedTsUpdate := func() error {
		metricSorterMergerStartTsGauge.Set(float64(oracle.ExtractPhysical(minResolvedTs)))
		workingSet = make(map[*flushTask]struct{})
		sortHeap := new(sortHeap)

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
					log.Panic("Unexpected end of backEnd data, bug?",
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
				task.reader = nil
				return errors.Trace(err)
			}

			if nextEvent == nil {
				delete(pendingSet, task)

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
				pendingSet[task] = nextEvent
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
					zap.Int64("table-id", tableID),
					zap.String("table-name", tableName),
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
				log.Panic("unified sorter: ts regressed in one backEnd, bug?", zap.Uint64("cur-ts", event.CRTs), zap.Uint64("last-ts", task.lastTs))
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
					log.Panic("unified sorter: output ts regressed, bug?",
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
				delete(pendingSet, task)

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
					pendingSet[task] = event
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
						zap.Int64("table-id", tableID),
						zap.String("table-name", tableName),
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
					zap.Int64("table-id", tableID),
					zap.String("table-name", tableName),
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

	resolveTicker := time.NewTicker(1 * time.Second)
	defer resolveTicker.Stop()

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
					zap.Int64("table-id", tableID),
					zap.String("table-name", tableName))
				return nil
			}

			taskBuf.put(task)
		}
	})

	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			task, err := taskBuf.get(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			if task == nil {
				tableID, tableName := util.TableIDFromCtx(ctx)
				log.Debug("Merger buffer exhausted and is closed, exiting",
					zap.Int64("table-id", tableID),
					zap.String("table-name", tableName),
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

		log.Warn("Unified Sorter: Error detected", zap.Error(err))
	}
	return err
}

// taskBuffer is used to store pending flushTasks.
// The design purpose is to reduce the backpressure caused by a congested output chan of the merger,
// so that heapSorter does not block.
type taskBuffer struct {
	mu    sync.Mutex // mu only protects queue
	queue deque.Deque

	notifier notify.Notifier
	len      *int64
	isClosed int32
}

func newTaskBuffer(len *int64) *taskBuffer {
	return &taskBuffer{
		queue:    deque.NewDeque(),
		notifier: notify.Notifier{},
		len:      len,
	}
}

func (b *taskBuffer) put(task *flushTask) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queue.PushBack(task)
	prevCount := atomic.AddInt64(b.len, 1)

	if prevCount == 1 {
		b.notifier.Notify()
	}
}

func (b *taskBuffer) get(ctx context.Context) (*flushTask, error) {
	if atomic.LoadInt32(&b.isClosed) == 1 && atomic.LoadInt64(b.len) == 0 {
		return nil, nil
	}

	if atomic.LoadInt64(b.len) == 0 {
		recv, err := b.notifier.NewReceiver(time.Millisecond * 50)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer recv.Stop()

		startTime := time.Now()
		for atomic.LoadInt64(b.len) == 0 {
			select {
			case <-ctx.Done():
				return nil, errors.Trace(ctx.Err())
			case <-recv.C:
				// Note that there can be spurious wake-ups
			}

			if atomic.LoadInt32(&b.isClosed) == 1 && atomic.LoadInt64(b.len) == 0 {
				return nil, nil
			}

			if time.Since(startTime) > time.Second*5 {
				log.Debug("taskBuffer reading blocked for too long", zap.Duration("duration", time.Since(startTime)))
			}
		}
	}

	postCount := atomic.AddInt64(b.len, -1)
	if postCount < 0 {
		log.Panic("taskBuffer: len < 0, report a bug", zap.Int64("len", postCount))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ret := b.queue.PopFront()
	if ret == nil {
		log.Panic("taskBuffer: PopFront() returned nil, report a bug")
	}

	return ret.(*flushTask), nil
}

func (b *taskBuffer) setClosed() {
	atomic.SwapInt32(&b.isClosed, 1)
}

// Only call this when the taskBuffer is NEVER going to be accessed again.
func (b *taskBuffer) close() {
	b.notifier.Close()
}
