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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type regionWorker struct {
	session        *eventFeedSession
	limiter        *rate.Limiter
	inputCh        chan *regionStatefulEvent
	outputCh       chan<- *model.RegionFeedEvent
	regionStates   map[uint64]*regionFeedState
	enableOldValue bool
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

	return w.session.onRegionFail(ctx, regionErrorInfo{
		singleRegionInfo: state.sri,
		err:              err,
	})
}

func (w *regionWorker) run(ctx context.Context) error {
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
				log.Debug("region worker closed by error")
				return cerror.ErrEventFeedAborted.GenWithStackByArgs()
			}
			if event.state.isStopped() {
				continue
			}
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
					if err := w.handleResolvedTs(ctx, x.ResolvedTs, event.state, metricSendEventResolvedCounter); err != nil {
						err = w.handleSingleRegionError(ctx, err, event.state)
					}
				}
			}

			if event.resolvedTs != nil {
				if err := w.handleResolvedTs(ctx, event.resolvedTs.Ts, event.state, metricSendEventResolvedCounter); err != nil {
					err = w.handleSingleRegionError(ctx, err, event.state)
				}
			}
			if err != nil {
				return err
			}
		}
	}
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
