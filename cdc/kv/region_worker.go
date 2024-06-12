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

package kv

import (
	"context"
	"encoding/hex"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	regionWorkerPool workerpool.WorkerPool
	workerPoolOnce   sync.Once
	workerPoolLock   sync.Mutex
	// The magic number here is keep the same with some magic numbers in some
	// other components in TiCDC, including worker pool task chan size, mounter
	// chan size etc.
	// TODO: unified channel buffer mechanism
	regionWorkerInputChanSize = 32
	// From benchmark, batch size ranges from 1 to 64(where 64 is in the extreme
	// incremental scan scenario or single region hotspot).
	// `batchEventsFactor * regionWorkerInputChanSize` equals to the count of
	// events that are hold in channel.
	batchEventsFactor         = 8
	regionWorkerLowWatermark  = int(float64(batchEventsFactor*regionWorkerInputChanSize) * 0.2)
	regionWorkerHighWatermark = int(float64(batchEventsFactor*regionWorkerInputChanSize) * 0.7)
)

const (
	maxWorkerPoolSize      = 64
	maxResolvedLockPerLoop = 64
)

type regionWorkerMetrics struct {
	metricReceivedEventSize prometheus.Observer
	metricDroppedEventSize  prometheus.Observer

	metricPullEventInitializedCounter prometheus.Counter
	metricPullEventCommittedCounter   prometheus.Counter
	metricPullEventPrewriteCounter    prometheus.Counter
	metricPullEventCommitCounter      prometheus.Counter
	metricPullEventRollbackCounter    prometheus.Counter

	metricSendEventResolvedCounter  prometheus.Counter
	metricSendEventCommitCounter    prometheus.Counter
	metricSendEventCommittedCounter prometheus.Counter
}

/*
`regionWorker` maintains N regions, it runs in background for each gRPC stream,
corresponding to one TiKV store. It receives `regionStatefulEvent` in a channel
from gRPC stream receiving goroutine, processes event as soon as possible and
sends `RegionFeedEvent` to output channel.
Besides the `regionWorker` maintains a background lock resolver, the lock resolver
maintains a resolved-ts based min heap to manager region resolved ts, so it doesn't
need to iterate each region every time when resolving lock.
Note: There exist two locks, one is lock for region states map, the other one is
lock for each region state(each region state has one lock).
`regionWorker` is single routine now, it will be extended to multiple goroutines
for event processing to increase throughput.
*/
type regionWorker struct {
	parentCtx context.Context
	session   *eventFeedSession
	stream    *eventFeedStream

	inputCh  chan []*regionStatefulEvent
	outputCh chan<- model.RegionFeedEvent
	errorCh  chan error

	// event handlers in region worker
	handles []workerpool.EventHandle
	// how many workers in worker pool will be used for this region worker
	concurrency   int
	statesManager *regionStateManager

	rtsManager  *regionTsManager
	rtsUpdateCh chan *rtsUpdateEvent

	metrics *regionWorkerMetrics

	// how many pending input events from the input channel
	inputPendingEvents int32
}

func newRegionWorkerMetrics(changefeedID model.ChangeFeedID) *regionWorkerMetrics {
	metrics := &regionWorkerMetrics{}
	metrics.metricReceivedEventSize = eventSize.WithLabelValues("received")
	metrics.metricDroppedEventSize = eventSize.WithLabelValues("dropped")

	metrics.metricPullEventInitializedCounter = pullEventCounter.
		WithLabelValues(cdcpb.Event_INITIALIZED.String(), changefeedID.Namespace, changefeedID.ID)
	metrics.metricPullEventCommittedCounter = pullEventCounter.
		WithLabelValues(cdcpb.Event_COMMITTED.String(), changefeedID.Namespace, changefeedID.ID)
	metrics.metricPullEventPrewriteCounter = pullEventCounter.
		WithLabelValues(cdcpb.Event_PREWRITE.String(), changefeedID.Namespace, changefeedID.ID)
	metrics.metricPullEventCommitCounter = pullEventCounter.
		WithLabelValues(cdcpb.Event_COMMIT.String(), changefeedID.Namespace, changefeedID.ID)
	metrics.metricPullEventRollbackCounter = pullEventCounter.
		WithLabelValues(cdcpb.Event_ROLLBACK.String(), changefeedID.Namespace, changefeedID.ID)

	metrics.metricSendEventResolvedCounter = sendEventCounter.
		WithLabelValues("native-resolved", changefeedID.Namespace, changefeedID.ID)
	metrics.metricSendEventCommitCounter = sendEventCounter.
		WithLabelValues("commit", changefeedID.Namespace, changefeedID.ID)
	metrics.metricSendEventCommittedCounter = sendEventCounter.
		WithLabelValues("committed", changefeedID.Namespace, changefeedID.ID)

	return metrics
}

func newRegionWorker(
	ctx context.Context,
	stream *eventFeedStream,
	s *eventFeedSession,
) *regionWorker {
	return &regionWorker{
		parentCtx:          ctx,
		session:            s,
		inputCh:            make(chan []*regionStatefulEvent, regionWorkerInputChanSize),
		outputCh:           s.eventCh,
		stream:             stream,
		errorCh:            make(chan error, 1),
		statesManager:      newRegionStateManager(-1),
		rtsManager:         newRegionTsManager(),
		rtsUpdateCh:        make(chan *rtsUpdateEvent, 1024),
		concurrency:        s.client.config.KVClient.WorkerConcurrent,
		metrics:            newRegionWorkerMetrics(s.changefeed),
		inputPendingEvents: 0,
	}
}

func (w *regionWorker) inputCalcSlot(regionID uint64) int {
	return int(regionID) % w.concurrency
}

func (w *regionWorker) getRegionState(regionID uint64) (*regionFeedState, bool) {
	return w.statesManager.getState(regionID)
}

func (w *regionWorker) setRegionState(regionID uint64, state *regionFeedState) {
	w.statesManager.setState(regionID, state)
}

func (w *regionWorker) delRegionState(regionID uint64) {
	w.statesManager.delState(regionID)
}

// checkRegionStateEmpty returns true if there is no region state maintained.
// Note this function is not thread-safe
func (w *regionWorker) checkRegionStateEmpty() (empty bool) {
	for _, states := range w.statesManager.states {
		if states.len() != 0 {
			return false
		}
	}
	return true
}

// checkShouldExit checks whether the region worker should exit, if exit return an error
func (w *regionWorker) checkShouldExit() error {
	empty := w.checkRegionStateEmpty()
	// If there is no region maintained by this region worker, exit it and
	// cancel the gRPC stream.
	if empty && w.stream.regions.len() == 0 {
		log.Info("A single region error happens before, "+
			"and there is no region maintained by this region worker, "+
			"exit it and cancel the gRPC stream",
			zap.String("namespace", w.session.client.changefeed.Namespace),
			zap.String("changefeed", w.session.client.changefeed.ID),
			zap.String("storeAddr", w.stream.addr),
			zap.Uint64("streamID", w.stream.id),
			zap.Int64("tableID", w.session.tableID),
			zap.String("tableName", w.session.tableName))
		w.cancelStream(time.Duration(0))
		return cerror.ErrRegionWorkerExit.GenWithStackByArgs()
	}
	return nil
}

func (w *regionWorker) handleSingleRegionError(err error, state *regionFeedState) error {
	regionID := state.getRegionID()
	isStale := state.isStale()
	log.Info("single region event feed disconnected",
		zap.String("namespace", w.session.client.changefeed.Namespace),
		zap.String("changefeed", w.session.client.changefeed.ID),
		zap.Uint64("regionID", regionID),
		zap.Uint64("requestID", state.requestID),
		zap.Stringer("span", state.sri.span),
		zap.Uint64("resolvedTs", state.sri.resolvedTs()),
		zap.Bool("isStale", isStale),
		zap.Error(err))
	// if state is already marked stopped, it must have been or would be processed by `onRegionFail`
	if isStale {
		return w.checkShouldExit()
	}
	// We need to ensure when the error is handled, `isStale` must be set. So set it before sending the error.
	state.markStopped()
	w.delRegionState(regionID)
	failpoint.Inject("kvClientSingleFeedProcessDelay", nil)

	failpoint.Inject("kvClientErrUnreachable", func() {
		if err == errUnreachable {
			failpoint.Return(err)
		}
	})

	// check and cancel gRPC stream before reconnecting region, in case of the
	// scenario that region connects to the same TiKV store again and reuses
	// resource in this region worker by accident.
	retErr := w.checkShouldExit()

	// `ErrPrewriteNotMatch` would cause duplicated request to the same region,
	// so cancel the original gRPC stream before restarts a new stream.
	if cerror.ErrPrewriteNotMatch.Equal(err) {
		log.Info("meet ErrPrewriteNotMatch error, cancel the gRPC stream",
			zap.String("namespace", w.session.client.changefeed.Namespace),
			zap.String("changefeed", w.session.client.changefeed.ID),
			zap.String("storeAddr", w.stream.addr),
			zap.Uint64("streamID", w.stream.id),
			zap.Int64("tableID", w.session.tableID),
			zap.String("tableName", w.session.tableName),
			zap.Uint64("regionID", regionID),
			zap.Uint64("requestID", state.requestID),
			zap.Stringer("span", &state.sri.span),
			zap.Uint64("resolvedTs", state.sri.resolvedTs()),
			zap.Error(err))
		w.cancelStream(time.Second)
	}

	// since the context used in region worker will be cancelled after region
	// worker exits, we must use the parent context to prevent regionErrorInfo loss.
	errInfo := newRegionErrorInfo(state.getRegionInfo(), err)
	w.session.onRegionFail(w.parentCtx, errInfo)

	return retErr
}

type rtsUpdateEvent struct {
	regions    []uint64
	resolvedTs uint64
}

// too many repeated logs when TiDB OOM
var resolveLockLogRateLimiter = rate.NewLimiter(rate.Every(time.Millisecond*2000), 10)

func (w *regionWorker) resolveLock(ctx context.Context) error {
	// tikv resolved update interval is 1s, use half of the resolve lock interval
	// as lock penalty.
	resolveLockPenalty := 10
	resolveLockInterval := 20 * time.Second
	failpoint.Inject("kvClientResolveLockInterval", func(val failpoint.Value) {
		resolveLockInterval = time.Duration(val.(int)) * time.Second
	})
	advanceCheckTicker := time.NewTicker(time.Second * 5)
	defer advanceCheckTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case rtsUpdate := <-w.rtsUpdateCh:
			resolvedTs := rtsUpdate.resolvedTs
			eventTime := time.Now()
			for _, regionID := range rtsUpdate.regions {
				w.rtsManager.Upsert(regionID, resolvedTs, eventTime)
			}
		case <-advanceCheckTicker.C:
			currentTimeFromPD := w.session.client.pdClock.CurrentTime()
			expired := make([]*regionTsInfo, 0)
			for w.rtsManager.Len() > 0 {
				item := w.rtsManager.Pop()
				sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(item.ts.resolvedTs))
				// region does not reach resolve lock boundary, put it back
				if sinceLastResolvedTs < resolveLockInterval {
					w.rtsManager.Insert(item)
					break
				}
				expired = append(expired, item)
				if len(expired) >= maxResolvedLockPerLoop {
					break
				}
			}
			if len(expired) == 0 {
				continue
			}
			maxVersion := oracle.ComposeTS(oracle.GetPhysical(currentTimeFromPD.Add(-10*time.Second)), 0)
			for _, rts := range expired {
				state, ok := w.getRegionState(rts.regionID)
				if !ok || state.isStale() {
					// state is already deleted or stopped, just continue,
					// and don't need to push resolved ts back to heap.
					continue
				}
				// recheck resolved ts from region state, which may be larger than that in resolved ts heap
				lastResolvedTs := state.getLastResolvedTs()
				sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(lastResolvedTs))
				if sinceLastResolvedTs >= resolveLockInterval {
					sinceLastEvent := time.Since(rts.ts.eventTime)
					if sinceLastResolvedTs > reconnectInterval && sinceLastEvent > reconnectInterval {
						log.Warn("kv client reconnect triggered",
							zap.String("namespace", w.session.client.changefeed.Namespace),
							zap.String("changefeed", w.session.client.changefeed.ID),
							zap.Duration("duration", sinceLastResolvedTs),
							zap.Duration("sinceLastEvent", sinceLastResolvedTs))
						return errReconnect
					}
					// Only resolve lock if the resolved-ts keeps unchanged for
					// more than resolveLockPenalty times.
					if rts.ts.penalty < resolveLockPenalty {
						if lastResolvedTs > rts.ts.resolvedTs {
							rts.ts.resolvedTs = lastResolvedTs
							rts.ts.eventTime = time.Now()
							rts.ts.penalty = 0
						}
						w.rtsManager.Insert(rts)
						continue
					}
					if resolveLockLogRateLimiter.Allow() {
						log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
							zap.String("namespace", w.session.client.changefeed.Namespace),
							zap.String("changefeed", w.session.client.changefeed.ID),
							zap.String("storeAddr", w.stream.addr),
							zap.Uint64("streamID", w.stream.id),
							zap.Int64("tableID", w.session.tableID),
							zap.String("tableName", w.session.tableName),
							zap.Uint64("regionID", rts.regionID),
							zap.Stringer("span", state.sri.span),
							zap.Duration("duration", sinceLastResolvedTs),
							zap.Duration("lastEvent", sinceLastEvent),
							zap.Uint64("resolvedTs", lastResolvedTs),
						)
					}
					err := w.session.lockResolver.Resolve(ctx, rts.regionID, maxVersion)
					if err != nil {
						log.Warn("failed to resolve lock",
							zap.Uint64("regionID", rts.regionID),
							zap.String("namespace", w.session.client.changefeed.Namespace),
							zap.String("changefeed", w.session.client.changefeed.ID),
							zap.Error(err))
						continue
					}
					rts.ts.penalty = 0
				}
				rts.ts.resolvedTs = lastResolvedTs
				w.rtsManager.Insert(rts)
			}
		}
	}
}

func (w *regionWorker) processEvent(ctx context.Context, event *regionStatefulEvent) error {
	// event.state is nil when resolvedTsEvent is not nil
	skipEvent := event.state != nil && event.state.isStale()
	if skipEvent {
		return nil
	}

	if event.finishedCallbackCh != nil {
		event.finishedCallbackCh <- struct{}{}
		return nil
	}
	var err error
	if event.changeEvent != nil {
		w.metrics.metricReceivedEventSize.Observe(float64(event.changeEvent.Event.Size()))
		switch x := event.changeEvent.Event.(type) {
		case *cdcpb.Event_Entries_:
			err = w.handleEventEntry(ctx, x, event.state)
			if err != nil {
				err = w.handleSingleRegionError(err, event.state)
			}
		case *cdcpb.Event_Admin_:
			log.Info("receive admin event",
				zap.String("namespace", w.session.client.changefeed.Namespace),
				zap.String("changefeed", w.session.client.changefeed.ID),
				zap.Stringer("event", event.changeEvent))
		case *cdcpb.Event_Error:
			err = w.handleSingleRegionError(
				cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error}),
				event.state,
			)
		case *cdcpb.Event_ResolvedTs:
			err = w.handleResolvedTs(ctx, &resolvedTsEvent{
				resolvedTs: x.ResolvedTs,
				regions:    []*regionFeedState{event.state},
			})
		}
		if err != nil {
			return err
		}
	}

	if event.resolvedTsEvent != nil {
		err = w.handleResolvedTs(ctx, event.resolvedTsEvent)
	}
	return err
}

func (w *regionWorker) initPoolHandles() {
	handles := make([]workerpool.EventHandle, 0, w.concurrency)
	workerPoolLock.Lock()
	defer workerPoolLock.Unlock()
	for i := 0; i < w.concurrency; i++ {
		poolHandle := regionWorkerPool.RegisterEvent(func(ctx context.Context, eventI interface{}) error {
			event := eventI.(*regionStatefulEvent)
			return w.processEvent(ctx, event)
		}).OnExit(func(err error) {
			w.onHandleExit(err)
		})
		handles = append(handles, poolHandle)
	}
	w.handles = handles
}

func (w *regionWorker) onHandleExit(err error) {
	select {
	case w.errorCh <- err:
	default:
	}
}

func (w *regionWorker) eventHandler(ctx context.Context) error {
	exitFn := func() error {
		log.Info("region worker closed by error",
			zap.String("namespace", w.session.client.changefeed.Namespace),
			zap.String("changefeed", w.session.client.changefeed.ID),
			zap.Int64("tableID", w.session.tableID),
			zap.String("tableName", w.session.tableName))
		return cerror.ErrRegionWorkerExit.GenWithStackByArgs()
	}

	highWatermarkMet := false
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-w.errorCh:
			return errors.Trace(err)
		case events, ok := <-w.inputCh:
			if !ok {
				return exitFn()
			}
			if len(events) == 0 {
				log.Panic("regionWorker.inputCh doesn't accept empty slice")
			}
			for _, event := range events {
				// event == nil means the region worker should exit and re-establish
				// all existing regions.
				if event == nil {
					return exitFn()
				}
			}

			regionEventsBatchSize.Observe(float64(len(events)))
			inputPending := atomic.LoadInt32(&w.inputPendingEvents)
			if highWatermarkMet {
				highWatermarkMet = int(inputPending) >= regionWorkerLowWatermark
			} else {
				highWatermarkMet = int(inputPending) >= regionWorkerHighWatermark
			}
			atomic.AddInt32(&w.inputPendingEvents, -int32(len(events)))

			if highWatermarkMet {
				// All events in one batch can be hashed into one handle slot.
				slot := w.inputCalcSlot(events[0].regionID)
				eventsX := make([]interface{}, 0, len(events))
				for _, event := range events {
					eventsX = append(eventsX, event)
				}
				err := w.handles[slot].AddEvents(ctx, eventsX)
				if err != nil {
					return err
				}
				// Principle: events from the same region must be processed linearly.
				//
				// When buffered events exceed high watermark, we start to use worker
				// pool to improve throughput, and we need a mechanism to quit worker
				// pool when buffered events are less than low watermark, which means
				// we should have a way to know whether events sent to the worker pool
				// are all processed.
				// Send a dummy event to each worker pool handler, after each of these
				// events are processed, we can ensure all events sent to worker pool
				// from this region worker are processed.
				finishedCallbackCh := make(chan struct{}, 1)
				err = w.handles[slot].AddEvent(ctx, &regionStatefulEvent{finishedCallbackCh: finishedCallbackCh})
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case err = <-w.errorCh:
					return err
				case <-finishedCallbackCh:
				}
			} else {
				// We measure whether the current worker is busy based on the input
				// channel size. If the buffered event count is larger than the high
				// watermark, we send events to worker pool to increase processing
				// throughput. Otherwise, we process event in local region worker to
				// ensure low processing latency.
				for _, event := range events {
					err := w.processEvent(ctx, event)
					if err != nil {
						return err
					}
				}
			}
			for _, ev := range events {
				// resolved ts event has been consumed, it is safe to put back.
				if ev.resolvedTsEvent != nil {
					w.session.resolvedTsPool.Put(ev)
				}
			}
		}
	}
}

func (w *regionWorker) collectWorkpoolError(ctx context.Context) error {
	cases := make([]reflect.SelectCase, 0, len(w.handles)+1)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for _, handle := range w.handles {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(handle.ErrCh()),
		})
	}
	idx, value, ok := reflect.Select(cases)
	if idx == 0 {
		return ctx.Err()
	}
	if !ok {
		return nil
	}
	return value.Interface().(error)
}

func (w *regionWorker) checkErrorReconnect(err error) error {
	if errors.Cause(err) == errReconnect {
		log.Info("kv client reconnect triggered, cancel the gRPC stream",
			zap.String("namespace", w.session.client.changefeed.Namespace),
			zap.String("changefeed", w.session.client.changefeed.ID),
			zap.String("storeAddr", w.stream.addr),
			zap.Uint64("streamID", w.stream.id),
			zap.Int64("tableID", w.session.tableID),
			zap.String("tableName", w.session.tableName))
		w.cancelStream(time.Second)
		// if stream is already deleted, just ignore errReconnect
		return nil
	}
	return err
}

// Note(dongmen): Please log the reason of calling this function in the caller.
// This will be helpful for troubleshooting.
func (w *regionWorker) cancelStream(delay time.Duration) {
	// cancel the stream to make strem.Recv returns a context cancel error
	// This will make the receiveFromStream goroutine exit and the stream can
	// be re-established by the caller.
	// Note: use context cancel is the only way to terminate a gRPC stream.
	w.stream.close()
	// Failover in stream.Recv has 0-100ms delay, the onRegionFail
	// should be called after stream has been deleted. Add a delay here
	// to avoid too frequent region rebuilt.
	time.Sleep(delay)
}

func (w *regionWorker) run() error {
	defer func() {
		for _, h := range w.handles {
			h.Unregister()
		}
	}()
	ctx, cancel := context.WithCancel(w.parentCtx)
	wg, ctx := errgroup.WithContext(ctx)
	w.initPoolHandles()

	var retErr error
	once := sync.Once{}
	handleError := func(err error) error {
		if err != nil {
			once.Do(func() {
				cancel()
				retErr = err
			})
		}
		return err
	}
	wg.Go(func() error {
		return handleError(w.checkErrorReconnect(w.resolveLock(ctx)))
	})
	wg.Go(func() error {
		return handleError(w.eventHandler(ctx))
	})
	_ = handleError(w.collectWorkpoolError(ctx))
	_ = wg.Wait()
	// ErrRegionWorkerExit means the region worker exits normally, but we don't
	// need to terminate the other goroutines in errgroup
	if cerror.ErrRegionWorkerExit.Equal(retErr) {
		return nil
	}
	return retErr
}

func (w *regionWorker) handleEventEntry(
	ctx context.Context,
	x *cdcpb.Event_Entries_,
	state *regionFeedState,
) error {
	emit := func(assembled model.RegionFeedEvent) bool {
		select {
		case w.outputCh <- assembled:
			return true
		case <-ctx.Done():
			return false
		}
	}
	return handleEventEntry(x, w.session.startTs, state, w.metrics, emit)
}

func handleEventEntry(
	x *cdcpb.Event_Entries_,
	startTs uint64,
	state *regionFeedState,
	metrics *regionWorkerMetrics,
	emit func(assembled model.RegionFeedEvent) bool,
) error {
	regionID, regionSpan, _ := state.getRegionMeta()
	for _, entry := range x.Entries.GetEntries() {
		// if a region with kv range [a, z), and we only want the get [b, c) from this region,
		// tikv will return all key events in the region, although specified [b, c) int the request.
		// we can make tikv only return the events about the keys in the specified range.
		comparableKey := regionspan.ToComparableKey(entry.GetKey())
		if entry.Type != cdcpb.Event_INITIALIZED &&
			!regionspan.KeyInSpan(comparableKey, regionSpan) {
			metrics.metricDroppedEventSize.Observe(float64(entry.Size()))
			continue
		}
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			metrics.metricPullEventInitializedCounter.Inc()
			state.setInitialized()
			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				revent, err := assembleRowEvent(regionID, cachedEvent)
				if err != nil {
					return errors.Trace(err)
				}
				if !emit(revent) {
					return nil
				}
				metrics.metricSendEventCommitCounter.Inc()
			}
			state.matcher.matchCachedRollbackRow(true)
		case cdcpb.Event_COMMITTED:
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}

			metrics.metricPullEventCommittedCounter.Inc()
			revent, err := assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if !emit(revent) {
				return nil
			}
			metrics.metricSendEventCommittedCounter.Inc()
		case cdcpb.Event_PREWRITE:
			metrics.metricPullEventPrewriteCounter.Inc()
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			metrics.metricPullEventCommitCounter.Inc()
			// NOTE: matchRow should always be called even if the event is stale.
			if !state.matcher.matchRow(entry, state.isInitialized()) {
				if !state.isInitialized() {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				return cerror.ErrPrewriteNotMatch.GenWithStackByArgs(
					hex.EncodeToString(entry.GetKey()),
					entry.GetStartTs(), entry.GetCommitTs(),
					entry.GetType(), entry.GetOpType())
			}

			// TiKV can send events with StartTs/CommitTs less than startTs.
			isStaleEvent := entry.CommitTs <= startTs
			if isStaleEvent {
				continue
			}

			// NOTE: state.getLastResolvedTs() will never less than startTs.
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}

			revent, err := assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if !emit(revent) {
				return nil
			}
			metrics.metricSendEventCommitCounter.Inc()
		case cdcpb.Event_ROLLBACK:
			metrics.metricPullEventRollbackCounter.Inc()
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
	return nil
}

func (w *regionWorker) handleResolvedTs(
	ctx context.Context,
	revents *resolvedTsEvent,
) error {
	resolvedTs := revents.resolvedTs
	resolvedSpans := make([]model.RegionComparableSpan, 0, len(revents.regions))
	regions := make([]uint64, 0, len(revents.regions))

	for _, state := range revents.regions {
		if state.isStale() || !state.isInitialized() {
			continue
		}
		regionID := state.getRegionID()
		regions = append(regions, regionID)
		lastResolvedTs := state.getLastResolvedTs()
		if resolvedTs < lastResolvedTs {
			log.Debug("The resolvedTs is fallen back in kvclient",
				zap.String("namespace", w.session.client.changefeed.Namespace),
				zap.String("changefeed", w.session.client.changefeed.ID),
				zap.String("EventType", "RESOLVED"),
				zap.Uint64("resolvedTs", resolvedTs),
				zap.Uint64("lastResolvedTs", lastResolvedTs),
				zap.Uint64("regionID", regionID))
			continue
		}
		// emit a resolvedTs
		resolvedSpans = append(resolvedSpans, model.RegionComparableSpan{
			Span:   state.sri.span,
			Region: regionID,
		})
	}
	if len(resolvedSpans) == 0 {
		return nil
	}
	// Send resolved ts update in non-blocking way, since we can re-query real
	// resolved ts from region state even if resolved ts update is discarded.
	// NOTICE: We send any regionTsInfo to resolveLock thread to give us a chance to trigger resolveLock logic
	// (1) if it is a fallback resolvedTs event, it will be discarded and accumulate penalty on the progress;
	// (2) if it is a normal one, update rtsManager and check sinceLastResolvedTs
	select {
	case w.rtsUpdateCh <- &rtsUpdateEvent{resolvedTs: resolvedTs, regions: regions}:
	default:
	}
	for _, state := range revents.regions {
		if state.isStale() || !state.isInitialized() {
			continue
		}
		state.updateResolvedTs(resolvedTs)
	}
	// emit a resolvedTs
	revent := model.RegionFeedEvent{Resolved: &model.ResolvedSpans{ResolvedTs: resolvedTs, Spans: resolvedSpans}}
	select {
	case w.outputCh <- revent:
		w.metrics.metricSendEventResolvedCounter.Add(float64(len(resolvedSpans)))
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	}
	return nil
}

// evictAllRegions is used when gRPC stream meets error and re-establish, notify
// all existing regions to re-establish
func (w *regionWorker) evictAllRegions() {
	deletes := make([]struct {
		regionID    uint64
		regionState *regionFeedState
	}, 0)
	for _, states := range w.statesManager.states {
		deletes = deletes[:0]
		states.iter(func(regionID uint64, regionState *regionFeedState) bool {
			if regionState.isStale() {
				return true
			}
			regionState.markStopped()
			deletes = append(deletes, struct {
				regionID    uint64
				regionState *regionFeedState
			}{
				regionID: regionID, regionState: regionState,
			})
			return true
		})
		for _, del := range deletes {
			w.delRegionState(del.regionID)
			// since the context used in region worker will be cancelled after
			// region worker exits, we must use the parent context to prevent
			// regionErrorInfo loss.
			errInfo := newRegionErrorInfo(
				del.regionState.sri, cerror.ErrEventFeedAborted.FastGenByArgs())
			w.session.onRegionFail(w.parentCtx, errInfo)
		}
	}
}

// sendEvents puts events into inputCh and updates some internal states.
// Callers must ensure that all items in events can be hashed into one handle slot.
func (w *regionWorker) sendEvents(ctx context.Context, events []*regionStatefulEvent) error {
	atomic.AddInt32(&w.inputPendingEvents, int32(len(events)))
	select {
	case <-ctx.Done():
		atomic.AddInt32(&w.inputPendingEvents, -int32(len(events)))
		return ctx.Err()
	case w.inputCh <- events:
		return nil
	}
}

func getWorkerPoolSize() (size int) {
	cfg := config.GetGlobalServerConfig().KVClient
	if cfg.WorkerPoolSize > 0 {
		size = cfg.WorkerPoolSize
	} else {
		size = runtime.NumCPU() * 2
	}
	if size > maxWorkerPoolSize {
		size = maxWorkerPoolSize
	}
	return
}

// InitWorkerPool initialize workerpool once, the workerpool must be initialized
// before any kv event is received.
func InitWorkerPool() {
	workerPoolOnce.Do(func() {
		size := getWorkerPoolSize()
		workerPoolLock.Lock()
		defer workerPoolLock.Unlock()
		regionWorkerPool = workerpool.NewDefaultWorkerPool(size)
	})
}

// RunWorkerPool runs the worker pool used by the region worker in kv client v2
// It must be running before region worker starts to work
func RunWorkerPool(ctx context.Context) error {
	InitWorkerPool()
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return errors.Trace(regionWorkerPool.Run(ctx))
	})
	return errg.Wait()
}
