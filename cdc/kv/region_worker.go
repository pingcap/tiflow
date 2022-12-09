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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	regionWorkerPool workerpool.WorkerPool
	workerPoolOnce   sync.Once
	// The magic number here is keep the same with some magic numbers in some
	// other components in TiCDC, including worker pool task chan size, mounter
	// chan size etc.
	// TODO: unified channel buffer mechanism
	regionWorkerInputChanSize = 128
	regionWorkerLowWatermark  = int(float64(regionWorkerInputChanSize) * 0.2)
	regionWorkerHighWatermark = int(float64(regionWorkerInputChanSize) * 0.7)
)

const (
	minRegionStateBucket = 4
	maxRegionStateBucket = 16

	maxWorkerPoolSize      = 64
	maxResolvedLockPerLoop = 64
)

// regionStateManager provides the get/put way like a sync.Map, and it is divided
// into several buckets to reduce lock contention
type regionStateManager struct {
	bucket int
	states []*sync.Map
}

func newRegionStateManager(bucket int) *regionStateManager {
	if bucket <= 0 {
		bucket = runtime.NumCPU()
		if bucket > maxRegionStateBucket {
			bucket = maxRegionStateBucket
		}
		if bucket < minRegionStateBucket {
			bucket = minRegionStateBucket
		}
	}
	rsm := &regionStateManager{
		bucket: bucket,
		states: make([]*sync.Map, bucket),
	}
	for i := range rsm.states {
		rsm.states[i] = new(sync.Map)
	}
	return rsm
}

func (rsm *regionStateManager) getBucket(regionID uint64) int {
	return int(regionID) % rsm.bucket
}

func (rsm *regionStateManager) getState(regionID uint64) (*regionFeedState, bool) {
	bucket := rsm.getBucket(regionID)
	if val, ok := rsm.states[bucket].Load(regionID); ok {
		return val.(*regionFeedState), true
	}
	return nil, false
}

func (rsm *regionStateManager) setState(regionID uint64, state *regionFeedState) {
	bucket := rsm.getBucket(regionID)
	rsm.states[bucket].Store(regionID, state)
}

func (rsm *regionStateManager) delState(regionID uint64) {
	bucket := rsm.getBucket(regionID)
	rsm.states[bucket].Delete(regionID)
}

type regionWorkerMetrics struct {
	// kv events related metrics
	metricReceivedEventSize           prometheus.Observer
	metricDroppedEventSize            prometheus.Observer
	metricPullEventInitializedCounter prometheus.Counter
	metricPullEventPrewriteCounter    prometheus.Counter
	metricPullEventCommitCounter      prometheus.Counter
	metricPullEventCommittedCounter   prometheus.Counter
	metricPullEventRollbackCounter    prometheus.Counter
	metricSendEventResolvedCounter    prometheus.Counter
	metricSendEventCommitCounter      prometheus.Counter
	metricSendEventCommittedCounter   prometheus.Counter

	// TODO: add region runtime related metrics
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

	inputCh  chan *regionStatefulEvent
	outputCh chan<- model.RegionFeedEvent
	errorCh  chan error

	// event handlers in region worker
	handles []workerpool.EventHandle
	// how many workers in worker pool will be used for this region worker
	concurrent    int
	statesManager *regionStateManager

	rtsManager  *regionTsManager
	rtsUpdateCh chan *regionTsInfo

	metrics *regionWorkerMetrics

	enableOldValue bool
	storeAddr      string
}

func newRegionWorker(s *eventFeedSession, addr string) *regionWorker {
	cfg := config.GetGlobalServerConfig().KVClient
	worker := &regionWorker{
		session:        s,
		inputCh:        make(chan *regionStatefulEvent, regionWorkerInputChanSize),
		outputCh:       s.eventCh,
		errorCh:        make(chan error, 1),
		statesManager:  newRegionStateManager(-1),
		rtsManager:     newRegionTsManager(),
		rtsUpdateCh:    make(chan *regionTsInfo, 1024),
		enableOldValue: s.enableOldValue,
		storeAddr:      addr,
		concurrent:     cfg.WorkerConcurrent,
	}
	return worker
}

func (w *regionWorker) initMetrics(ctx context.Context) {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	metrics := &regionWorkerMetrics{}
	metrics.metricReceivedEventSize = eventSize.WithLabelValues(captureAddr, "received")
	metrics.metricDroppedEventSize = eventSize.WithLabelValues(captureAddr, "dropped")
	metrics.metricPullEventInitializedCounter = pullEventCounter.WithLabelValues(cdcpb.Event_INITIALIZED.String(), captureAddr, changefeedID)
	metrics.metricPullEventCommittedCounter = pullEventCounter.WithLabelValues(cdcpb.Event_COMMITTED.String(), captureAddr, changefeedID)
	metrics.metricPullEventCommitCounter = pullEventCounter.WithLabelValues(cdcpb.Event_COMMIT.String(), captureAddr, changefeedID)
	metrics.metricPullEventPrewriteCounter = pullEventCounter.WithLabelValues(cdcpb.Event_PREWRITE.String(), captureAddr, changefeedID)
	metrics.metricPullEventRollbackCounter = pullEventCounter.WithLabelValues(cdcpb.Event_ROLLBACK.String(), captureAddr, changefeedID)
	metrics.metricSendEventResolvedCounter = sendEventCounter.WithLabelValues("native-resolved", captureAddr, changefeedID)
	metrics.metricSendEventCommitCounter = sendEventCounter.WithLabelValues("commit", captureAddr, changefeedID)
	metrics.metricSendEventCommittedCounter = sendEventCounter.WithLabelValues("committed", captureAddr, changefeedID)

	w.metrics = metrics
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
	empty = true
	for _, states := range w.statesManager.states {
		states.Range(func(_, _ interface{}) bool {
			empty = false
			return false
		})
		if !empty {
			return
		}
	}
	return
}

// checkShouldExit checks whether the region worker should exit, if should exit
// return an error
func (w *regionWorker) checkShouldExit() error {
	empty := w.checkRegionStateEmpty()
	// If there is not region maintained by this region worker, exit it and
	// cancel the gRPC stream.
	if empty {
		w.cancelStream(time.Duration(0))
		return cerror.ErrRegionWorkerExit.GenWithStackByArgs()
	}
	return nil
}

func (w *regionWorker) handleSingleRegionError(err error, state *regionFeedState) error {
	if state.lastResolvedTs > state.sri.ts {
		state.sri.ts = state.lastResolvedTs
	}
	regionID := state.sri.verID.GetID()
	log.Info("single region event feed disconnected",
		zap.String("changefeed", w.session.client.changefeed),
		zap.Uint64("regionID", regionID),
		zap.Uint64("requestID", state.requestID),
		zap.Stringer("span", state.sri.span),
		zap.Uint64("checkpoint", state.sri.ts),
		zap.String("error", err.Error()))
	// if state is already marked stopped, it must have been or would be processed by `onRegionFail`
	if state.isStopped() {
		return w.checkShouldExit()
	}
	// We need to ensure when the error is handled, `isStopped` must be set. So set it before sending the error.
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
		w.cancelStream(time.Second)
	}

	revokeToken := !state.initialized
	// since the context used in region worker will be cancelled after region
	// worker exits, we must use the parent context to prevent regionErrorInfo loss.
	errInfo := newRegionErrorInfo(state.sri, err)
	w.session.onRegionFail(w.parentCtx, errInfo, revokeToken)

	return retErr
}

func (w *regionWorker) resolveLock(ctx context.Context) error {
	// tikv resolved update interval is 1s, use half of the resolck lock interval
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
			w.rtsManager.Upsert(rtsUpdate)
		case <-advanceCheckTicker.C:
			version, err := w.session.kvStorage.GetCachedCurrentVersion()
			if err != nil {
				log.Warn("failed to get current version from PD",
					zap.Error(err), zap.String("changefeed", w.session.client.changefeed))
				continue
			}
			currentTimeFromPD := oracle.GetTimeFromTS(version.Ver)
			expired := make([]*regionTsInfo, 0)
			for w.rtsManager.Len() > 0 {
				item := w.rtsManager.Pop()
				sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(item.ts.resolvedTs))
				// region does not reach resolve lock boundary, put it back
				if sinceLastResolvedTs < resolveLockInterval {
					w.rtsManager.Upsert(item)
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
				if !ok || state.isStopped() {
					// state is already deleted or stoppped, just continue,
					// and don't need to push resolved ts back to heap.
					continue
				}
				// recheck resolved ts from region state, which may be larger than that in resolved ts heap
				lastResolvedTs := state.getLastResolvedTs()
				sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(lastResolvedTs))
				if sinceLastResolvedTs >= resolveLockInterval {
					sinceLastEvent := time.Since(rts.ts.eventTime)
					if sinceLastResolvedTs > reconnectInterval && sinceLastEvent > reconnectInterval {
						log.Warn("kv client reconnect triggered", zap.String("changefeed", w.session.client.changefeed),
							zap.Duration("duration", sinceLastResolvedTs), zap.Duration("since last event", sinceLastResolvedTs))
						return errReconnect
					}
					// Only resolve lock if the resovled-ts keeps unchanged for
					// more than resolveLockPenalty times.
					if rts.ts.penalty < resolveLockPenalty {
						if lastResolvedTs > rts.ts.resolvedTs {
							rts.ts.resolvedTs = lastResolvedTs
							rts.ts.eventTime = time.Now()
							rts.ts.penalty = 0
						}
						w.rtsManager.Upsert(rts)
						continue
					}
					log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
						zap.String("changefeed", w.session.client.changefeed),
						zap.Uint64("regionID", rts.regionID),
						zap.Stringer("span", state.getRegionSpan()),
						zap.Duration("duration", sinceLastResolvedTs),
						zap.Duration("lastEvent", sinceLastEvent),
						zap.Uint64("resolvedTs", lastResolvedTs),
					)
					err = w.session.lockResolver.Resolve(ctx, rts.regionID, maxVersion)
					if err != nil {
						log.Warn("failed to resolve lock",
							zap.Uint64("regionID", rts.regionID), zap.Error(err),
							zap.String("changefeed", w.session.client.changefeed))
						continue
					}
					rts.ts.penalty = 0
				}
				rts.ts.resolvedTs = lastResolvedTs
				w.rtsManager.Upsert(rts)
			}
		}
	}
}

func (w *regionWorker) processEvent(ctx context.Context, event *regionStatefulEvent) error {
	if event.finishedCallbackCh != nil {
		event.finishedCallbackCh <- struct{}{}
		return nil
	}
	var err error
	event.state.lock.Lock()
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
				zap.Stringer("event", event.changeEvent),
				zap.String("changefeed", w.session.client.changefeed))
		case *cdcpb.Event_Error:
			err = w.handleSingleRegionError(
				cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error}),
				event.state,
			)
		case *cdcpb.Event_ResolvedTs:
			if err = w.handleResolvedTs(ctx, x.ResolvedTs, event.state); err != nil {
				err = w.handleSingleRegionError(err, event.state)
			}
		}
	}

	if event.resolvedTs != nil {
		if err = w.handleResolvedTs(ctx, event.resolvedTs.Ts, event.state); err != nil {
			err = w.handleSingleRegionError(err, event.state)
		}
	}
	event.state.lock.Unlock()
	return err
}

func (w *regionWorker) initPoolHandles(handleCount int) {
	handles := make([]workerpool.EventHandle, 0, handleCount)
	for i := 0; i < handleCount; i++ {
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
	preprocess := func(event *regionStatefulEvent, ok bool) (
		exitEventHandler bool,
		skipEvent bool,
	) {
		// event == nil means the region worker should exit and re-establish
		// all existing regions.
		if !ok || event == nil {
			log.Info("region worker closed by error",
				zap.String("changefeed", w.session.client.changefeed))
			exitEventHandler = true
			return
		}
		if event.state.isStopped() {
			skipEvent = true
		}
		return
	}
	pollEvent := func() (event *regionStatefulEvent, ok bool, err error) {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
		case err = <-w.errorCh:
		case event, ok = <-w.inputCh:
		}
		return
	}
	for {
		event, ok, err := pollEvent()
		if err != nil {
			return err
		}
		exitEventHandler, skipEvent := preprocess(event, ok)
		if exitEventHandler {
			return cerror.ErrRegionWorkerExit.GenWithStackByArgs()
		}
		if skipEvent {
			continue
		}
		// We measure whether the current worker is busy based on the input
		// channel size. If the buffered event count is larger than the high
		// watermark, we send events to worker pool to increase processing
		// throughput. Otherwise we process event in local region worker to
		// ensure low processing latency.
		if len(w.inputCh) < regionWorkerHighWatermark {
			err = w.processEvent(ctx, event)
			if err != nil {
				return err
			}
		} else {
			err = w.handles[int(event.regionID)%w.concurrent].AddEvent(ctx, event)
			if err != nil {
				return err
			}
			// TODO: add events in batch
			for len(w.inputCh) >= regionWorkerLowWatermark {
				event, ok, err = pollEvent()
				if err != nil {
					return err
				}
				exitEventHandler, skipEvent := preprocess(event, ok)
				if exitEventHandler {
					return cerror.ErrRegionWorkerExit.GenWithStackByArgs()
				}
				if skipEvent {
					continue
				}
				err = w.handles[int(event.regionID)%w.concurrent].AddEvent(ctx, event)
				if err != nil {
					return err
				}
			}
			// Principle: events from the same region must be processed linearly.
			//
			// When buffered events exceed high watermark, we start to use worker
			// pool to improve throughtput, and we need a mechanism to quit worker
			// pool when buffered events are less than low watermark, which means
			// we should have a way to know whether events sent to the worker pool
			// are all processed.
			// Send a dummy event to each worker pool handler, after each of these
			// events are processed, we can ensure all events sent to worker pool
			// from this region worker are processed.
			finishedCallbackCh := make(chan struct{}, len(w.handles))
			for _, handle := range w.handles {
				err = handle.AddEvent(ctx, &regionStatefulEvent{finishedCallbackCh: finishedCallbackCh})
				if err != nil {
					return err
				}
			}
			counter := len(w.handles)
		checkEventsProcessed:
			for {
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case err = <-w.errorCh:
					return err
				case <-finishedCallbackCh:
					counter--
					if counter == 0 {
						break checkEventsProcessed
					}
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
		w.cancelStream(time.Second)
		// if stream is already deleted, just ignore errReconnect
		return nil
	}
	return err
}

func (w *regionWorker) cancelStream(delay time.Duration) {
	cancel, ok := w.session.getStreamCancel(w.storeAddr)
	if ok {
		// cancel the stream to trigger strem.Recv with context cancel error
		// Note use context cancel is the only way to terminate a gRPC stream
		cancel()
		// Failover in stream.Recv has 0-100ms delay, the onRegionFail
		// should be called after stream has been deleted. Add a delay here
		// to avoid too frequent region rebuilt.
		time.Sleep(delay)
	} else {
		log.Warn("gRPC stream cancel func not found",
			zap.String("addr", w.storeAddr),
			zap.String("changefeed", w.session.client.changefeed))
	}
}

func (w *regionWorker) run(parentCtx context.Context) error {
	defer func() {
		for _, h := range w.handles {
			h.Unregister()
		}
	}()
	w.parentCtx = parentCtx
	wg, ctx := errgroup.WithContext(parentCtx)
	w.initMetrics(ctx)
	w.initPoolHandles(w.concurrent)
	wg.Go(func() error {
		return w.checkErrorReconnect(w.resolveLock(ctx))
	})
	wg.Go(func() error {
		return w.eventHandler(ctx)
	})
	wg.Go(func() error {
		return w.collectWorkpoolError(ctx)
	})
	err := wg.Wait()
	// ErrRegionWorkerExit means the region worker exits normally, but we don't
	// need to terminate the other goroutines in errgroup
	if cerror.ErrRegionWorkerExit.Equal(err) {
		return nil
	}
	return err
}

func (w *regionWorker) handleEventEntry(
	ctx context.Context,
	x *cdcpb.Event_Entries_,
	state *regionFeedState,
) error {
	regionID := state.sri.verID.GetID()
	for _, entry := range x.Entries.GetEntries() {
		// if a region with kv range [a, z)
		// and we only want the get [b, c) from this region,
		// tikv will return all key events in the region although we specified [b, c) int the request.
		// we can make tikv only return the events about the keys in the specified range.
		comparableKey := regionspan.ToComparableKey(entry.GetKey())
		// key for initialized event is nil
		if !regionspan.KeyInSpan(comparableKey, state.sri.span) && entry.Type != cdcpb.Event_INITIALIZED {
			w.metrics.metricDroppedEventSize.Observe(float64(entry.Size()))
			continue
		}
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			if time.Since(state.startFeedTime) > 20*time.Second {
				log.Warn("The time cost of initializing is too much",
					zap.String("changefeed", w.session.client.changefeed),
					zap.Duration("duration", time.Since(state.startFeedTime)),
					zap.Uint64("regionID", regionID))
			}
			w.metrics.metricPullEventInitializedCounter.Inc()

			state.initialized = true
			w.session.regionRouter.Release(state.sri.rpcCtx.Addr)
			cachedEvents := state.matcher.matchCachedRow(state.initialized)
			for _, cachedEvent := range cachedEvents {
				revent, err := assembleRowEvent(regionID, cachedEvent, w.enableOldValue)
				if err != nil {
					return errors.Trace(err)
				}
				select {
				case w.outputCh <- revent:
					w.metrics.metricSendEventCommitCounter.Inc()
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				}
			}
			state.matcher.matchCachedRollbackRow(state.initialized)
		case cdcpb.Event_COMMITTED:
			w.metrics.metricPullEventCommittedCounter.Inc()
			revent, err := assembleRowEvent(regionID, entry, w.enableOldValue)
			if err != nil {
				return errors.Trace(err)
			}

			if entry.CommitTs <= state.lastResolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("Event Type", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", state.lastResolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			select {
			case w.outputCh <- revent:
				w.metrics.metricSendEventCommittedCounter.Inc()
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		case cdcpb.Event_PREWRITE:
			w.metrics.metricPullEventPrewriteCounter.Inc()
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			w.metrics.metricPullEventCommitCounter.Inc()
			if entry.CommitTs <= state.lastResolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("Event Type", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", state.lastResolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			ok := state.matcher.matchRow(entry, state.initialized)
			if !ok {
				if !state.initialized {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				return cerror.ErrPrewriteNotMatch.GenWithStackByArgs(
					hex.EncodeToString(entry.GetKey()),
					entry.GetStartTs(), entry.GetCommitTs(),
					entry.GetType(), entry.GetOpType())
			}

			revent, err := assembleRowEvent(regionID, entry, w.enableOldValue)
			if err != nil {
				return errors.Trace(err)
			}

			select {
			case w.outputCh <- revent:
				w.metrics.metricSendEventCommitCounter.Inc()
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		case cdcpb.Event_ROLLBACK:
			w.metrics.metricPullEventRollbackCounter.Inc()
			if !state.initialized {
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
	resolvedTs uint64,
	state *regionFeedState,
) error {
	if !state.initialized {
		return nil
	}
	regionID := state.sri.verID.GetID()
	// Send resolved ts update in non blocking way, since we can re-query real
	// resolved ts from region state even if resolved ts update is discarded.
	// NOTICE: We send any regionTsInfo to resolveLock thread to give us a chance to trigger resolveLock logic
	// (1) if it is a fallback resolvedTs event, it will be discarded and accumulate penalty on the progress;
	// (2) if it is a normal one, update rtsManager and check sinceLastResolvedTs
	select {
	case w.rtsUpdateCh <- &regionTsInfo{regionID: regionID, ts: newResolvedTsItem(resolvedTs)}:
	default:
	}

	if resolvedTs < state.lastResolvedTs {
		log.Warn("The resolvedTs is fallen back in kvclient",
			zap.String("changefeed", w.session.client.changefeed),
			zap.String("EventType", "RESOLVED"),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", state.lastResolvedTs),
			zap.Uint64("regionID", regionID))
		return nil
	}
	state.lastResolvedTs = resolvedTs
	// emit a checkpointTs
	revent := model.RegionFeedEvent{
		RegionID: regionID,
		Resolved: &model.ResolvedSpan{
			Span:       state.sri.span,
			ResolvedTs: resolvedTs,
		},
	}

	select {
	case w.outputCh <- revent:
		w.metrics.metricSendEventResolvedCounter.Inc()
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	}
	return nil
}

// evictAllRegions is used when gRPC stream meets error and re-establish, notify
// all existing regions to re-establish
func (w *regionWorker) evictAllRegions() {
	for _, states := range w.statesManager.states {
		states.Range(func(_, value interface{}) bool {
			state := value.(*regionFeedState)
			state.lock.Lock()
			// if state is marked as stopped, it must have been or would be processed by `onRegionFail`
			if state.isStopped() {
				state.lock.Unlock()
				return true
			}
			state.markStopped()
			w.delRegionState(state.sri.verID.GetID())
			if state.lastResolvedTs > state.sri.ts {
				state.sri.ts = state.lastResolvedTs
			}
			revokeToken := !state.initialized
			state.lock.Unlock()
			// since the context used in region worker will be cancelled after
			// region worker exits, we must use the parent context to prevent
			// regionErrorInfo loss.
			errInfo := newRegionErrorInfo(state.sri, cerror.ErrEventFeedAborted.FastGenByArgs())
			w.session.onRegionFail(w.parentCtx, errInfo, revokeToken)
			return true
		})
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
