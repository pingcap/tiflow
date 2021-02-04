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
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type regionWorker struct {
	session        *eventFeedSession
	limiter        *rate.Limiter
	inputCh        chan *regionStatefulEvent
	outputCh       chan<- *model.RegionFeedEvent
	regionStates   map[uint64]*regionFeedState
	statesLock     sync.RWMutex
	enableOldValue bool
}

func (w *regionWorker) getRegionState(regionID uint64) (*regionFeedState, bool) {
	w.statesLock.RLock()
	defer w.statesLock.RUnlock()
	state, ok := w.regionStates[regionID]
	return state, ok
}

func (w *regionWorker) setRegionState(regionID uint64, state *regionFeedState) {
	w.statesLock.Lock()
	defer w.statesLock.Unlock()
	w.regionStates[regionID] = state
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
		case <-advanceCheckTicker.C:
			if !w.session.isPullerInit.IsInitialized() {
				// Initializing a puller may take a long time, skip resolved lock to save unnecessary overhead.
				continue
			}
			// TODO: add a better expired heap, so we don't need to iterate each region every time
			w.statesLock.RLock()
			version, err := w.session.kvStorage.(*StorageWithCurVersionCache).GetCachedCurrentVersion()
			for regionID, state := range w.regionStates {
				state.lock.RLock()
				sinceLastEvent := time.Since(state.lastReceivedEventTime)
				if sinceLastEvent > resolveLockInterval {
					log.Warn("region not receiving event from tikv for too long time",
						zap.Uint64("regionID", regionID), zap.Stringer("span", state.sri.span), zap.Duration("duration", sinceLastEvent))
				}
				if err != nil {
					log.Warn("failed to get current version from PD", zap.Error(err))
					state.lock.RUnlock()
					continue
				}
				currentTimeFromPD := oracle.GetTimeFromTS(version.Ver)
				sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(state.lastResolvedTs))
				if sinceLastResolvedTs > resolveLockInterval && state.initialized {
					log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
						zap.Uint64("regionID", regionID), zap.Stringer("span", state.sri.span),
						zap.Duration("duration", sinceLastResolvedTs),
						zap.Uint64("resolvedTs", state.lastResolvedTs))
					maxVersion := oracle.ComposeTS(oracle.GetPhysical(currentTimeFromPD.Add(-10*time.Second)), 0)
					err = w.session.lockResolver.Resolve(ctx, regionID, maxVersion)
					if err != nil {
						log.Warn("failed to resolve lock", zap.Uint64("regionID", regionID), zap.Error(err))
						state.lock.RUnlock()
						continue
					}
				}
				state.lock.RUnlock()
			}
			w.statesLock.RUnlock()
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
			if !ok {
				log.Debug("region worker receiver closed")
				return nil
			}
			if event == nil {
				log.Info("region worker closed by error")
				return w.evitAllRegions(ctx)
			}
			if event.state.isStopped() {
				continue
			}
			event.state.lock.Lock()
			event.state.lastReceivedEventTime = time.Now()
			if event.changeEvent != nil {
				metricEventSize.Observe(float64(event.changeEvent.Event.Size()))
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

func (w *regionWorker) run(ctx context.Context) error {
	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return w.resolveLock(ctx)
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
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			if time.Since(state.startFeedTime) > 20*time.Second {
				log.Warn("The time cost of initializing is too mush",
					zap.Duration("timeCost", time.Since(state.startFeedTime)),
					zap.Uint64("regionID", regionID))
			}
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

	select {
	case w.outputCh <- revent:
		metricSendEventResolvedCounter.Inc()
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	}
	return nil
}

func (w *regionWorker) evitAllRegions(ctx context.Context) error {
	w.statesLock.Lock()
	defer w.statesLock.Unlock()
	for _, state := range w.regionStates {
		log.Info("on region fail fired", zap.Reflect("state", state))
		err := w.session.onRegionFail(ctx, regionErrorInfo{
			singleRegionInfo: state.sri,
			err: &rpcCtxUnavailableErr{
				verID: state.sri.verID,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *eventFeedSession) receiveFromStreamV2(
	ctx context.Context,
	g *errgroup.Group,
	addr string,
	storeID uint64,
	stream cdcpb.ChangeData_EventFeedClient,
	pendingRegions *syncRegionFeedStateMap,
	limiter *rate.Limiter,
) error {
	// Cancel the pending regions if the stream failed. Otherwise it will remain unhandled in the pendingRegions list
	// however not registered in the new reconnected stream.
	defer func() {
		log.Info("stream to store closed", zap.String("addr", addr), zap.Uint64("storeID", storeID))

		remainingRegions := pendingRegions.takeAll()
		for _, state := range remainingRegions {
			err := s.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: state.sri,
				err:              cerror.ErrPendingRegionCancel.GenWithStackByArgs(),
			})
			if err != nil {
				// The only possible is that the ctx is cancelled. Simply return.
				return
			}
		}
		s.workersLock.Lock()
		delete(s.workers, addr)
		s.workersLock.Unlock()
	}()

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricSendEventBatchResolvedSize := batchResolvedEventSize.WithLabelValues(captureAddr, changefeedID)

	s.workersLock.Lock()
	worker, ok := s.workers[addr]
	if !ok {
		worker = &regionWorker{
			session:        s,
			limiter:        limiter,
			inputCh:        make(chan *regionStatefulEvent, 1024),
			outputCh:       s.eventCh,
			regionStates:   make(map[uint64]*regionFeedState),
			enableOldValue: s.enableOldValue,
		}
		s.workers[addr] = worker
	}
	s.workersLock.Unlock()

	g.Go(func() error {
		return worker.run(ctx)
	})

	for {
		cevent, err := stream.Recv()

		failpoint.Inject("kvClientStreamRecvError", func() {
			err = errors.New("injected stream recv error")
		})
		if err == io.EOF {
			close(worker.inputCh)
			return nil
		}
		if err != nil {
			if status.Code(errors.Cause(err)) == codes.Canceled {
				log.Debug(
					"receive from stream canceled",
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
				)
			} else {
				log.Error(
					"failed to receive from stream",
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
					zap.Error(err),
				)
			}

			// Use the same delay mechanism as `stream.Send` error handling, since
			// these two errors often mean upstream store suffers an accident, which
			// needs time to recover, kv client doesn't need to retry frequently.
			// TODO: add a better retry backoff or rate limitter
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))

			// TODO: better to closes the send direction of the stream to notify
			// the other side, but it is not safe to call CloseSend concurrently
			// with SendMsg, in future refactor we should refine the recv loop
			s.deleteStream(addr)

			// send nil regionStatefulEvent to signal worker exit
			select {
			case worker.inputCh <- nil:
			case <-ctx.Done():
				return ctx.Err()
			}

			// Do no return error but gracefully stop the goroutine here. Then the whole job will not be canceled and
			// connection will be retried.
			return nil
		}

		size := cevent.Size()
		if size > warnRecvMsgSizeThreshold {
			regionCount := 0
			if cevent.ResolvedTs != nil {
				regionCount = len(cevent.ResolvedTs.Regions)
			}
			log.Warn("change data event size too large",
				zap.Int("size", size), zap.Int("event length", len(cevent.Events)),
				zap.Int("resolved region count", regionCount))
		}

		for _, event := range cevent.Events {
			err = s.sendRegionChangeEventV2(ctx, g, event, worker, pendingRegions, addr, limiter)
			if err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTsV2(ctx, g, cevent.ResolvedTs, worker, addr)
			if err != nil {
				return err
			}
		}
	}
}
