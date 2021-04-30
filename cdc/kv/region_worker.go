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
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	minRegionStateBucket = 4
	maxRegionStateBucket = 16
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
	session *eventFeedSession
	limiter *rate.Limiter

	inputCh  chan *regionStatefulEvent
	outputCh chan<- *model.RegionFeedEvent

	statesManager *regionStateManager

	rtsManager  *regionTsManager
	rtsUpdateCh chan *regionTsInfo

	// evTimeManager maintains the time that last event is received of each
	// uninitialized region, note the regionTsManager is not thread safe, so we
	// use a single routine to handle evTimeUpdate and evTimeManager
	evTimeManager  *regionTsManager
	evTimeUpdateCh chan *evTimeUpdate

	enableOldValue bool
	storeAddr      string
}

func newRegionWorker(s *eventFeedSession, limiter *rate.Limiter, addr string) *regionWorker {
	worker := &regionWorker{
		session:        s,
		limiter:        limiter,
		inputCh:        make(chan *regionStatefulEvent, 1024),
		outputCh:       s.eventCh,
		statesManager:  newRegionStateManager(-1),
		rtsManager:     newRegionTsManager(),
		evTimeManager:  newRegionTsManager(),
		rtsUpdateCh:    make(chan *regionTsInfo, 1024),
		evTimeUpdateCh: make(chan *evTimeUpdate, 1024),
		enableOldValue: s.enableOldValue,
		storeAddr:      addr,
	}
	return worker
}

type evTimeUpdate struct {
	info     *regionTsInfo
	isDelete bool
}

// notifyEvTimeUpdate trys to send a evTimeUpdate to evTimeUpdateCh in region worker
// to upsert or delete the last received event time for a region
func (w *regionWorker) notifyEvTimeUpdate(regionID uint64, isDelete bool) {
	select {
	case w.evTimeUpdateCh <- &evTimeUpdate{
		info:     &regionTsInfo{regionID: regionID, ts: newEventTimeItem()},
		isDelete: isDelete,
	}:
	default:
	}
}

func (w *regionWorker) getRegionState(regionID uint64) (*regionFeedState, bool) {
	return w.statesManager.getState(regionID)
}

func (w *regionWorker) setRegionState(regionID uint64, state *regionFeedState) {
	w.statesManager.setState(regionID, state)
}

func (w *regionWorker) handleSingleRegionError(ctx context.Context, err error, state *regionFeedState) error {
	if state.lastResolvedTs > state.sri.ts {
		state.sri.ts = state.lastResolvedTs
	}
	regionID := state.sri.verID.GetID()
	log.Info("single region event feed disconnected",
		zap.Uint64("regionID", regionID),
		zap.Uint64("requestID", state.requestID),
		zap.Stringer("span", state.sri.span),
		zap.Uint64("checkpoint", state.sri.ts),
		zap.String("error", err.Error()))
	// if state is already marked stopped, it must have been or would be processed by `onRegionFail`
	if state.isStopped() {
		return nil
	}
	// We need to ensure when the error is handled, `isStopped` must be set. So set it before sending the error.
	state.markStopped()
	failpoint.Inject("kvClientSingleFeedProcessDelay", nil)
	now := time.Now()
	delay := w.limiter.ReserveN(now, 1).Delay()
	if delay != 0 {
		log.Info("EventFeed retry rate limited",
			zap.Duration("delay", delay), zap.Reflect("regionID", regionID))
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-t.C:
			// We can proceed.
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	failpoint.Inject("kvClientErrUnreachable", func() {
		if err == errUnreachable {
			failpoint.Return(err)
		}
	})

	return w.session.onRegionFail(ctx, regionErrorInfo{
		singleRegionInfo: state.sri,
		err:              err,
	})
}

func (w *regionWorker) checkUnInitRegions(ctx context.Context) error {
	checkInterval := time.Minute

	failpoint.Inject("kvClientCheckUnInitRegionInterval", func(val failpoint.Value) {
		checkInterval = time.Duration(val.(int)) * time.Second
	})

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case update := <-w.evTimeUpdateCh:
			if update.isDelete {
				w.evTimeManager.Remove(update.info.regionID)
			} else {
				w.evTimeManager.Upsert(update.info)
			}
		case <-ticker.C:
			for w.evTimeManager.Len() > 0 {
				item := w.evTimeManager.Pop()
				sinceLastEvent := time.Since(item.ts.eventTime)
				if sinceLastEvent < reconnectInterval {
					w.evTimeManager.Upsert(item)
					break
				}
				state, ok := w.getRegionState(item.regionID)
				if !ok || state.isStopped() || state.isInitialized() {
					// check state is deleted, stopped, or initialized, if
					// so just ignore this region, and don't need to push the
					// eventTimeItem back to heap.
					continue
				}
				log.Warn("kv client reconnect triggered",
					zap.Duration("duration", sinceLastEvent), zap.Uint64("region", item.regionID))
				return errReconnect
			}
		}
	}
}

func (w *regionWorker) resolveLock(ctx context.Context) error {
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
			version, err := w.session.kvStorage.(*StorageWithCurVersionCache).GetCachedCurrentVersion()
			if err != nil {
				log.Warn("failed to get current version from PD", zap.Error(err))
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
						log.Warn("kv client reconnect triggered",
							zap.Duration("duration", sinceLastResolvedTs), zap.Duration("since last event", sinceLastResolvedTs))
						return errReconnect
					}
					log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
						zap.Uint64("regionID", rts.regionID), zap.Stringer("span", state.getRegionSpan()),
						zap.Duration("duration", sinceLastResolvedTs), zap.Uint64("resolvedTs", lastResolvedTs))
					err = w.session.lockResolver.Resolve(ctx, rts.regionID, maxVersion)
					if err != nil {
						log.Warn("failed to resolve lock", zap.Uint64("regionID", rts.regionID), zap.Error(err))
						continue
					}
				}
				rts.ts.resolvedTs = lastResolvedTs
				w.rtsManager.Upsert(rts)
			}
		}
	}
}

func (w *regionWorker) eventHandler(ctx context.Context) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricEventSize := eventSize.WithLabelValues(captureAddr)
	metricPullEventInitializedCounter := pullEventCounter.WithLabelValues(cdcpb.Event_INITIALIZED.String(), captureAddr, changefeedID)
	metricPullEventCommittedCounter := pullEventCounter.WithLabelValues(cdcpb.Event_COMMITTED.String(), captureAddr, changefeedID)
	metricPullEventCommitCounter := pullEventCounter.WithLabelValues(cdcpb.Event_COMMIT.String(), captureAddr, changefeedID)
	metricPullEventPrewriteCounter := pullEventCounter.WithLabelValues(cdcpb.Event_PREWRITE.String(), captureAddr, changefeedID)
	metricPullEventRollbackCounter := pullEventCounter.WithLabelValues(cdcpb.Event_ROLLBACK.String(), captureAddr, changefeedID)
	metricSendEventResolvedCounter := sendEventCounter.WithLabelValues("native-resolved", captureAddr, changefeedID)
	metricSendEventCommitCounter := sendEventCounter.WithLabelValues("commit", captureAddr, changefeedID)
	metricSendEventCommittedCounter := sendEventCounter.WithLabelValues("committed", captureAddr, changefeedID)

	var err error
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-w.inputCh:
			// event == nil means the region worker should exit and re-establish
			// all existing regions.
			if !ok || event == nil {
				log.Info("region worker closed by error")
				return w.evictAllRegions(ctx)
			}
			if event.state.isStopped() {
				continue
			}
			event.state.lock.Lock()
			if event.changeEvent != nil {
				metricEventSize.Observe(float64(event.changeEvent.Event.Size()))
				if !event.state.initialized {
					w.notifyEvTimeUpdate(event.state.sri.verID.GetID(), false /* isDelete */)
				}
				switch x := event.changeEvent.Event.(type) {
				case *cdcpb.Event_Entries_:
					err = w.handleEventEntry(
						ctx, x, event.state,
						metricPullEventInitializedCounter,
						metricPullEventPrewriteCounter,
						metricPullEventCommitCounter,
						metricPullEventCommittedCounter,
						metricPullEventRollbackCounter,
						metricSendEventCommitCounter,
						metricSendEventCommittedCounter,
					)
					if err != nil {
						err = w.handleSingleRegionError(ctx, err, event.state)
					}
				case *cdcpb.Event_Admin_:
					log.Info("receive admin event", zap.Stringer("event", event.changeEvent))
				case *cdcpb.Event_Error:
					err = w.handleSingleRegionError(
						ctx,
						cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error}),
						event.state,
					)
				case *cdcpb.Event_ResolvedTs:
					if err = w.handleResolvedTs(ctx, x.ResolvedTs, event.state, metricSendEventResolvedCounter); err != nil {
						err = w.handleSingleRegionError(ctx, err, event.state)
					}
				}
			}

			if event.resolvedTs != nil {
				if err = w.handleResolvedTs(ctx, event.resolvedTs.Ts, event.state, metricSendEventResolvedCounter); err != nil {
					err = w.handleSingleRegionError(ctx, err, event.state)
				}
			}
			event.state.lock.Unlock()
			if err != nil {
				return err
			}
		}
	}
}

func (w *regionWorker) checkErrorReconnect(err error) error {
	if errors.Cause(err) == errReconnect {
		cancel, ok := w.session.getStreamCancel(w.storeAddr)
		if ok {
			// cancel the stream to trigger strem.Recv with context cancel error
			// Note use context cancel is the only way to terminate a gRPC stream
			cancel()
			// Failover in stream.Recv has 0-100ms delay, the onRegionFail
			// should be called after stream has been deleted. Add a delay here
			// to avoid too frequent region rebuilt.
			time.Sleep(time.Second)
		}
		// if stream is already deleted, just ignore errReconnect
		return nil
	}
	return err
}

func (w *regionWorker) run(ctx context.Context) error {
	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return w.checkErrorReconnect(w.resolveLock(ctx))
	})
	wg.Go(func() error {
		return w.checkErrorReconnect(w.checkUnInitRegions(ctx))
	})
	wg.Go(func() error {
		return w.eventHandler(ctx)
	})
	return wg.Wait()
}

func (w *regionWorker) handleEventEntry(
	ctx context.Context,
	x *cdcpb.Event_Entries_,
	state *regionFeedState,
	metricPullEventInitializedCounter prometheus.Counter,
	metricPullEventPrewriteCounter prometheus.Counter,
	metricPullEventCommitCounter prometheus.Counter,
	metricPullEventCommittedCounter prometheus.Counter,
	metricPullEventRollbackCounter prometheus.Counter,
	metricSendEventCommitCounter prometheus.Counter,
	metricSendEventCommittedCounter prometheus.Counter,
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
			continue
		}
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			if time.Since(state.startFeedTime) > 20*time.Second {
				log.Warn("The time cost of initializing is too mush",
					zap.Duration("timeCost", time.Since(state.startFeedTime)),
					zap.Uint64("regionID", regionID))
			}

			select {
			case w.rtsUpdateCh <- &regionTsInfo{regionID: regionID, ts: newResolvedTsItem(state.sri.ts)}:
			default:
				// rtsUpdateCh block often means too many regions are suffering
				// lock resolve, the kv client status is not very healthy.
				log.Warn("region is not upsert into rts manager", zap.Uint64("region-id", regionID))
			}
			w.notifyEvTimeUpdate(regionID, true /* isDelete */)

			metricPullEventInitializedCounter.Inc()
			state.initialized = true

			cachedEvents := state.matcher.matchCachedRow()
			for _, cachedEvent := range cachedEvents {
				revent, err := assembleRowEvent(regionID, cachedEvent, w.enableOldValue)
				if err != nil {
					return errors.Trace(err)
				}
				select {
				case w.outputCh <- revent:
					metricSendEventCommitCounter.Inc()
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				}
			}
		case cdcpb.Event_COMMITTED:
			metricPullEventCommittedCounter.Inc()
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
				metricSendEventCommittedCounter.Inc()
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		case cdcpb.Event_PREWRITE:
			metricPullEventPrewriteCounter.Inc()
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			metricPullEventCommitCounter.Inc()
			if entry.CommitTs <= state.lastResolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("Event Type", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", state.lastResolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			ok := state.matcher.matchRow(entry)
			if !ok {
				if !state.initialized {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				return cerror.ErrPrewriteNotMatch.GenWithStackByArgs(entry.GetKey(), entry.GetStartTs())
			}

			revent, err := assembleRowEvent(regionID, entry, w.enableOldValue)
			if err != nil {
				return errors.Trace(err)
			}

			select {
			case w.outputCh <- revent:
				metricSendEventCommitCounter.Inc()
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		case cdcpb.Event_ROLLBACK:
			metricPullEventRollbackCounter.Inc()
			state.matcher.rollbackRow(entry)
		}
	}
	return nil
}

func (w *regionWorker) handleResolvedTs(
	ctx context.Context,
	resolvedTs uint64,
	state *regionFeedState,
	metricSendEventResolvedCounter prometheus.Counter,
) error {
	if !state.initialized {
		return nil
	}
	regionID := state.sri.verID.GetID()
	if resolvedTs < state.lastResolvedTs {
		log.Warn("The resolvedTs is fallen back in kvclient",
			zap.String("Event Type", "RESOLVED"),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", state.lastResolvedTs),
			zap.Uint64("regionID", regionID))
		return nil
	}
	// emit a checkpointTs
	revent := &model.RegionFeedEvent{
		RegionID: regionID,
		Resolved: &model.ResolvedSpan{
			Span:       state.sri.span,
			ResolvedTs: resolvedTs,
		},
	}
	state.lastResolvedTs = resolvedTs
	// Send resolved ts update in non blocking way, since we can re-query real
	// resolved ts from region state even if resolved ts update is discarded.
	select {
	case w.rtsUpdateCh <- &regionTsInfo{regionID: regionID, ts: newResolvedTsItem(resolvedTs)}:
	default:
	}

	select {
	case w.outputCh <- revent:
		metricSendEventResolvedCounter.Inc()
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	}
	return nil
}

// evictAllRegions is used when gRPC stream meets error and re-establish, notify
// all existing regions to re-establish
func (w *regionWorker) evictAllRegions(ctx context.Context) error {
	var err error
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
			singleRegionInfo := state.sri.partialClone()
			if state.lastResolvedTs > singleRegionInfo.ts {
				singleRegionInfo.ts = state.lastResolvedTs
			}
			state.lock.Unlock()
			err = w.session.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: singleRegionInfo,
				err: &rpcCtxUnavailableErr{
					verID: singleRegionInfo.verID,
				},
			})
			return err == nil
		})
	}
	return err
}
