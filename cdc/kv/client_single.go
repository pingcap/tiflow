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
// TODO: this file will be removed after region_worker is stable

package kv

import (
	"context"
	"io"
	"math/rand"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *eventFeedSession) singleSendRegionChangeEvent(
	ctx context.Context,
	g *errgroup.Group,
	event *cdcpb.Event,
	regionStates map[uint64]*regionFeedState,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
	limiter *rate.Limiter,
) error {
	state, ok := regionStates[event.RegionId]
	// Every region's range is locked before sending requests and unlocked after exiting, and the requestID
	// is allocated while holding the range lock. Therefore the requestID is always incrementing. If a region
	// is receiving messages with different requestID, only the messages with the larges requestID is valid.
	isNewSubscription := !ok
	if ok {
		if state.requestID < event.RequestId {
			log.Debug("region state entry will be replaced because received message of newer requestID",
				zap.Uint64("regionID", event.RegionId),
				zap.Uint64("oldRequestID", state.requestID),
				zap.Uint64("requestID", event.RequestId),
				zap.String("addr", addr))
			isNewSubscription = true
		} else if state.requestID > event.RequestId {
			log.Warn("drop event due to event belongs to a stale request",
				zap.Uint64("regionID", event.RegionId),
				zap.Uint64("requestID", event.RequestId),
				zap.Uint64("currRequestID", state.requestID),
				zap.String("addr", addr))
			return nil
		}
	}

	if isNewSubscription {
		// It's the first response for this region. If the region is newly connected, the region info should
		// have been put in `pendingRegions`. So here we load the region info from `pendingRegions` and start
		// a new goroutine to handle messages from this region.
		// Firstly load the region info.
		state, ok = pendingRegions.take(event.RequestId)
		if !ok {
			log.Error("received an event but neither pending region nor running region was found",
				zap.Uint64("regionID", event.RegionId),
				zap.Uint64("requestID", event.RequestId),
				zap.String("addr", addr))
			return cerror.ErrNoPendingRegion.GenWithStackByArgs(event.RegionId, event.RequestId, addr)
		}

		// Then spawn the goroutine to process messages of this region.
		regionStates[event.RegionId] = state

		g.Go(func() error {
			return s.partialRegionFeed(ctx, state, limiter)
		})
	} else if state.isStopped() {
		log.Warn("drop event due to region feed stopped",
			zap.Uint64("regionID", event.RegionId),
			zap.Uint64("requestID", event.RequestId),
			zap.String("addr", addr))
		return nil
	}

	select {
	case state.regionEventCh <- &regionEvent{
		changeEvent: event,
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *eventFeedSession) singleSendResolvedTs(
	ctx context.Context,
	g *errgroup.Group,
	resolvedTs *cdcpb.ResolvedTs,
	regionStates map[uint64]*regionFeedState,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
) error {
	for _, regionID := range resolvedTs.Regions {
		state, ok := regionStates[regionID]
		if ok {
			if state.isStopped() {
				log.Warn("drop resolved ts due to region feed stopped",
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("addr", addr))
				continue
			}
			select {
			case state.regionEventCh <- &regionEvent{
				resolvedTs: resolvedTs,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (s *eventFeedSession) singleReceiveFromStream(
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
	}()

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricSendEventBatchResolvedSize := batchResolvedEventSize.WithLabelValues(captureAddr, changefeedID)

	// Each region has it's own goroutine to handle its messages. `regionStates` stores states of these regions.
	regionStates := make(map[uint64]*regionFeedState)

	for {
		cevent, err := stream.Recv()

		failpoint.Inject("kvClientStreamRecvError", func() {
			err = errors.New("injected stream recv error")
		})
		// TODO: Should we have better way to handle the errors?
		if err == io.EOF {
			for _, state := range regionStates {
				close(state.regionEventCh)
			}
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

			for _, state := range regionStates {
				select {
				case state.regionEventCh <- nil:
				case <-ctx.Done():
					return ctx.Err()
				}
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
			err = s.singleSendRegionChangeEvent(ctx, g, event, regionStates, pendingRegions, addr, limiter)
			if err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.singleSendResolvedTs(ctx, g, cevent.ResolvedTs, regionStates, pendingRegions, addr)
			if err != nil {
				return err
			}
		}
	}
}

// partialRegionFeed establishes a EventFeed to the region specified by regionInfo.
// It manages lifecycle events of the region in order to maintain the EventFeed
// connection. If any error happens (region split, leader change, etc), the region
// and error info will be sent to `errCh`, and the receiver of `errCh` is
// responsible for handling the error and re-establish the connection to the region.
func (s *eventFeedSession) partialRegionFeed(
	ctx context.Context,
	state *regionFeedState,
	limiter *rate.Limiter,
) error {
	receiver := state.regionEventCh
	defer func() {
		state.markStopped()
		// Workaround to avoid remaining messages in the channel blocks the receiver thread.
		// TODO: Find a better solution.
		timer := time.After(time.Second * 2)
		for {
			select {
			case <-receiver:
			case <-timer:
				return
			}
		}
	}()

	ts := state.sri.ts
	maxTs, err := s.singleEventFeed(ctx, state.sri.verID.GetID(), state.sri.span, state.sri.ts, receiver)
	log.Debug("singleEventFeed quit")

	if err == nil || errors.Cause(err) == context.Canceled {
		return nil
	}

	failpoint.Inject("kvClientErrUnreachable", func() {
		if err == errUnreachable {
			failpoint.Return(err)
		}
	})

	if maxTs > ts {
		ts = maxTs
	}

	regionID := state.sri.verID.GetID()
	log.Info("EventFeed disconnected",
		zap.Uint64("regionID", regionID),
		zap.Uint64("requestID", state.requestID),
		zap.Stringer("span", state.sri.span),
		zap.Uint64("checkpoint", ts),
		zap.String("error", err.Error()))

	state.sri.ts = ts

	// We need to ensure when the error is handled, `isStopped` must be set. So set it before sending the error.
	state.markStopped()

	failpoint.Inject("kvClientSingleFeedProcessDelay", nil)

	now := time.Now()
	delay := limiter.ReserveN(now, 1).Delay()
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

	return s.onRegionFail(ctx, regionErrorInfo{
		singleRegionInfo: state.sri,
		err:              err,
	})
}
func (s *eventFeedSession) handleResolvedTs(
	ctx context.Context,
	resolvedTs uint64,
	regionID uint64,
	span regionspan.ComparableSpan,
	lastResolvedTs *uint64,
	initialized bool,
	metricSendEventResolvedCounter prometheus.Counter,
) error {
	if !initialized {
		return nil
	}
	if resolvedTs < *lastResolvedTs {
		log.Warn("The resolvedTs is fallen back in kvclient",
			zap.String("Event Type", "RESOLVED"),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", *lastResolvedTs),
			zap.Uint64("regionID", regionID))
		return nil
	}
	// emit a checkpointTs
	revent := &model.RegionFeedEvent{
		RegionID: regionID,
		Resolved: &model.ResolvedSpan{
			Span:       span,
			ResolvedTs: resolvedTs,
		},
	}
	*lastResolvedTs = resolvedTs

	select {
	case s.eventCh <- revent:
		metricSendEventResolvedCounter.Inc()
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	}
	return nil
}

func (s *eventFeedSession) handleEventEntry(
	ctx context.Context,
	x *cdcpb.Event_Entries_,
	regionID uint64,
	lastResolvedTs *uint64,
	initialized *bool,
	startFeedTime time.Time,
	matcher *matcher,

	metricPullEventInitializedCounter prometheus.Counter,
	metricPullEventPrewriteCounter prometheus.Counter,
	metricPullEventCommitCounter prometheus.Counter,
	metricPullEventCommittedCounter prometheus.Counter,
	metricPullEventRollbackCounter prometheus.Counter,
	metricSendEventCommitCounter prometheus.Counter,
	metricSendEventCommittedCounter prometheus.Counter,
) error {
	for _, entry := range x.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			if time.Since(startFeedTime) > 20*time.Second {
				log.Warn("The time cost of initializing is too mush",
					zap.Duration("timeCost", time.Since(startFeedTime)),
					zap.Uint64("regionID", regionID))
			}
			metricPullEventInitializedCounter.Inc()
			*initialized = true
			cachedEvents := matcher.matchCachedRow()
			for _, cachedEvent := range cachedEvents {
				revent, err := assembleRowEvent(regionID, cachedEvent, s.enableOldValue)
				if err != nil {
					return errors.Trace(err)
				}
				select {
				case s.eventCh <- revent:
					metricSendEventCommitCounter.Inc()
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				}
			}
		case cdcpb.Event_COMMITTED:
			metricPullEventCommittedCounter.Inc()
			revent, err := assembleRowEvent(regionID, entry, s.enableOldValue)
			if err != nil {
				return errors.Trace(err)
			}

			if entry.CommitTs <= *lastResolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("Event Type", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", *lastResolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			select {
			case s.eventCh <- revent:
				metricSendEventCommittedCounter.Inc()
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		case cdcpb.Event_PREWRITE:
			metricPullEventPrewriteCounter.Inc()
			matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			metricPullEventCommitCounter.Inc()
			if entry.CommitTs <= *lastResolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("Event Type", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", *lastResolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			ok := matcher.matchRow(entry)
			if !ok {
				if !*initialized {
					matcher.cacheCommitRow(entry)
					continue
				}
				return cerror.ErrPrewriteNotMatch.GenWithStackByArgs(entry.GetKey(), entry.GetStartTs())
			}

			revent, err := assembleRowEvent(regionID, entry, s.enableOldValue)
			if err != nil {
				return errors.Trace(err)
			}

			select {
			case s.eventCh <- revent:
				metricSendEventCommitCounter.Inc()
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		case cdcpb.Event_ROLLBACK:
			metricPullEventRollbackCounter.Inc()
			matcher.rollbackRow(entry)
		}
	}
	return nil
}

// singleEventFeed handles events of a single EventFeed stream.
// Results will be send to eventCh
// EventFeed RPC will not return resolved event directly
// Resolved event is generate while there's not non-match pre-write
// Return the maximum resolved
func (s *eventFeedSession) singleEventFeed(
	ctx context.Context,
	regionID uint64,
	span regionspan.ComparableSpan,
	startTs uint64,
	receiverCh <-chan *regionEvent,
) (uint64, error) {
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

	initialized := false

	matcher := newMatcher()
	advanceCheckTicker := time.NewTicker(time.Second * 5)
	defer advanceCheckTicker.Stop()
	lastReceivedEventTime := time.Now()
	startFeedTime := time.Now()
	lastResolvedTs := startTs

	select {
	case s.eventCh <- &model.RegionFeedEvent{
		RegionID: regionID,
		Resolved: &model.ResolvedSpan{
			Span:       span,
			ResolvedTs: startTs,
		},
	}:
	case <-ctx.Done():
		return lastResolvedTs, errors.Trace(ctx.Err())
	}
	resolveLockInterval := 20 * time.Second
	failpoint.Inject("kvClientResolveLockInterval", func(val failpoint.Value) {
		resolveLockInterval = time.Duration(val.(int)) * time.Second
	})

	for {
		var event *regionEvent
		var ok bool
		select {
		case <-ctx.Done():
			return lastResolvedTs, ctx.Err()
		case <-advanceCheckTicker.C:
			if time.Since(startFeedTime) < resolveLockInterval {
				continue
			}
			if !s.isPullerInit.IsInitialized() {
				// Initializing a puller may take a long time, skip resolved lock to save unnecessary overhead.
				continue
			}
			sinceLastEvent := time.Since(lastReceivedEventTime)
			if sinceLastEvent > resolveLockInterval {
				log.Warn("region not receiving event from tikv for too long time",
					zap.Uint64("regionID", regionID), zap.Stringer("span", span), zap.Duration("duration", sinceLastEvent))
			}
			version, err := s.kvStorage.(*StorageWithCurVersionCache).GetCachedCurrentVersion()
			if err != nil {
				log.Warn("failed to get current version from PD", zap.Error(err))
				continue
			}
			currentTimeFromPD := oracle.GetTimeFromTS(version.Ver)
			sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(lastResolvedTs))
			if sinceLastResolvedTs > resolveLockInterval && initialized {
				log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
					zap.Uint64("regionID", regionID), zap.Stringer("span", span),
					zap.Duration("duration", sinceLastResolvedTs),
					zap.Uint64("resolvedTs", lastResolvedTs))
				maxVersion := oracle.ComposeTS(oracle.GetPhysical(currentTimeFromPD.Add(-10*time.Second)), 0)
				err = s.lockResolver.Resolve(ctx, regionID, maxVersion)
				if err != nil {
					log.Warn("failed to resolve lock", zap.Uint64("regionID", regionID), zap.Error(err))
					continue
				}
			}
			continue
		case event, ok = <-receiverCh:
		}
		if !ok {
			log.Debug("singleEventFeed receiver closed")
			return lastResolvedTs, nil
		}

		if event == nil {
			log.Debug("singleEventFeed closed by error")
			return lastResolvedTs, cerror.ErrEventFeedAborted.GenWithStackByArgs()
		}
		lastReceivedEventTime = time.Now()
		if event.changeEvent != nil {
			metricEventSize.Observe(float64(event.changeEvent.Event.Size()))
			switch x := event.changeEvent.Event.(type) {
			case *cdcpb.Event_Entries_:
				err := s.handleEventEntry(
					ctx, x, regionID, &lastResolvedTs, &initialized, startFeedTime, matcher,
					metricPullEventInitializedCounter,
					metricPullEventPrewriteCounter,
					metricPullEventCommitCounter,
					metricPullEventCommittedCounter,
					metricPullEventRollbackCounter,
					metricSendEventCommitCounter,
					metricSendEventCommittedCounter,
				)
				if err != nil {
					return lastResolvedTs, err
				}
			case *cdcpb.Event_Admin_:
				log.Info("receive admin event", zap.Stringer("event", event.changeEvent))
			case *cdcpb.Event_Error:
				return lastResolvedTs, cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error})
			case *cdcpb.Event_ResolvedTs:
				if err := s.handleResolvedTs(ctx, x.ResolvedTs, regionID, span, &lastResolvedTs, initialized, metricSendEventResolvedCounter); err != nil {
					return lastResolvedTs, errors.Trace(err)
				}
			}
		}

		if event.resolvedTs != nil {
			if err := s.handleResolvedTs(ctx, event.resolvedTs.Ts, regionID, span, &lastResolvedTs, initialized, metricSendEventResolvedCounter); err != nil {
				return lastResolvedTs, errors.Trace(err)
			}
		}
	}
}
