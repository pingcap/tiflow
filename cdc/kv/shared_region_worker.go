// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type workerMetrics struct {
	metricPullEventInitializedCounter prometheus.Counter
	metricPullEventCommittedCounter   prometheus.Counter
	metricPullEventPrewriteCounter    prometheus.Counter
	metricPullEventCommitCounter      prometheus.Counter
	metricPullEventRollbackCounter    prometheus.Counter

	metricSendEventResolvedCounter  prometheus.Counter
	metricSendEventCommitCounter    prometheus.Counter
	metricSendEventCommittedCounter prometheus.Counter
}

// NOTE: eventItem and resolvedTs shouldn't appear simultaneously.
// All contents come from one same TiKV store.
type statefulEvent struct {
	eventItem       eventItem
	resolvedTsBatch resolvedTsBatch
	rs              *requestedStore
}

type eventItem struct {
	// All items come from one same region.
	item  *cdcpb.Event
	state *regionFeedState
}

type resolvedTsBatch struct {
	ts      uint64
	regions []*regionFeedState
}

type sharedRegionWorker struct {
	changefeed model.ChangeFeedID

	client *SharedClient

	statesManager *regionStateManager

	inputCh chan statefulEvent
	errorCh chan error

	metrics *workerMetrics
	ctx     context.Context
}

func newWorkerMetrics(changefeedID model.ChangeFeedID) *workerMetrics {
	metrics := &workerMetrics{}

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

func newSharedRegionWorker(c *SharedClient, metrics *workerMetrics) *sharedRegionWorker {
	return &sharedRegionWorker{
		changefeed:    c.changefeed,
		client:        c,
		inputCh:       make(chan statefulEvent, regionWorkerInputChanSize),
		statesManager: newRegionStateManager(-1),
		metrics:       metrics,
	}
}

func (w *sharedRegionWorker) sendEvent(ctx context.Context, event statefulEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.inputCh <- event:
		return nil
	}
}

// An empty event can trigger a region state check.
func (w *sharedRegionWorker) sendEmptyEvent(ctx context.Context, state *regionFeedState) error {
	sfEvent := statefulEvent{eventItem: eventItem{state: state}}
	return w.sendEvent(ctx, sfEvent)
}

func (w *sharedRegionWorker) run(ctx context.Context) error {
	for {
		var event statefulEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event = <-w.inputCh:
		}
		w.processEvent(ctx, event)
	}
}

func (w *sharedRegionWorker) handleSingleRegionError(
	ctx context.Context, err error,
	state *regionFeedState,
	rs *requestedStore,
) {
	state.markStopped()
	state.setRegionInfoResolvedTs()

	regionID := state.getRegionID()
	rs.takeState(regionID, state.requestID)

	log.Info("single region event feed disconnected",
		zap.String("namespace", w.changefeed.Namespace),
		zap.String("changefeed", w.changefeed.ID),
		zap.Uint64("regionID", regionID),
		zap.Uint64("requestID", state.requestID),
		zap.Stringer("span", &state.sri.span),
		zap.Uint64("resolvedTs", state.sri.resolvedTs()),
		zap.Error(err))

	w.client.onRegionFail(ctx, newRegionErrorInfo(state.getRegionInfo(), err))
}

func (w *sharedRegionWorker) processEvent(ctx context.Context, event statefulEvent) {
	if event.eventItem.state != nil {
		state := event.eventItem.state
		if state.isStopped() {
			if err := state.takeError(); err != nil {
				w.handleSingleRegionError(ctx, err, state, event.rs)
				return
			}
		}
		switch x := event.eventItem.item.Event.(type) {
		case *cdcpb.Event_Entries_:
			if err := w.handleEventEntry(ctx, x, state); err != nil {
				w.handleSingleRegionError(ctx, err, state, event.rs)
				return
			}
		case *cdcpb.Event_Admin_:
		case *cdcpb.Event_Error:
			err := cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error})
			w.handleSingleRegionError(ctx, err, state, event.rs)
			return
		case *cdcpb.Event_ResolvedTs:
			w.handleResolvedTs(ctx, resolvedTsBatch{
				ts:      x.ResolvedTs,
				regions: []*regionFeedState{state},
			})
		}
	} else if event.resolvedTsBatch.ts != 0 {
		w.handleResolvedTs(ctx, event.resolvedTsBatch)
	}
}

// NOTE: context.Canceled won't be treated as an error.
func (w *sharedRegionWorker) handleEventEntry(ctx context.Context, x *cdcpb.Event_Entries_, state *regionFeedState) error {
	emit := func(assembled model.RegionFeedEvent) bool {
		x := state.sri.requestedTable.associateSubscriptionID(assembled)
		select {
		case state.sri.requestedTable.eventCh <- x:
			return true
		case <-ctx.Done():
			return false
		}
	}

	regionID := state.sri.verID.GetID()
	for _, entry := range x.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			if time.Since(state.startFeedTime) > 20*time.Second {
				log.Warn("The time cost of initializing is too much",
					zap.String("namespace", w.changefeed.Namespace),
					zap.String("changefeed", w.changefeed.ID),
					zap.Uint64("regionID", regionID),
					zap.Duration("duration", time.Since(state.startFeedTime)))
			}

			w.metrics.metricPullEventInitializedCounter.Inc()
			state.setInitialized()
			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				revent, err := assembleRowEvent(regionID, cachedEvent)
				if err != nil {
					return errors.Trace(err)
				}
				if !emit(revent) {
					return nil
				}
				w.metrics.metricSendEventCommitCounter.Inc()
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

			w.metrics.metricPullEventCommittedCounter.Inc()
			revent, err := assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}
			if !emit(revent) {
				return nil
			}
			w.metrics.metricSendEventCommittedCounter.Inc()
		case cdcpb.Event_PREWRITE:
			w.metrics.metricPullEventPrewriteCounter.Inc()
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			w.metrics.metricPullEventCommitCounter.Inc()
			// NOTE: state.getLastResolvedTs() will never less than session.startTs.
			resolvedTs := state.getLastResolvedTs()
			// TiKV can send events with StartTs/CommitTs less than startTs.
			isStaleEvent := entry.CommitTs <= state.sri.requestedTable.startTs
			if entry.CommitTs <= resolvedTs && !isStaleEvent {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}

			if !state.matcher.matchRow(entry, state.isInitialized()) {
				if !state.isInitialized() {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				return errors.New("prewrite not match")
			}

			if !isStaleEvent {
				revent, err := assembleRowEvent(regionID, entry)
				if err != nil {
					return errors.Trace(err)
				}
				if !emit(revent) {
					return nil
				}
				w.metrics.metricSendEventCommitCounter.Inc()
			}
		case cdcpb.Event_ROLLBACK:
			w.metrics.metricPullEventRollbackCounter.Inc()
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
	return nil
}

func (w *sharedRegionWorker) handleResolvedTs(ctx context.Context, event resolvedTsBatch) {
	if event.ts == 0 {
		return
	}

	resolvedSpans := make(map[uint64]*struct {
		spans          []model.RegionComparableSpan
		requestedTable *requestedTable
	})

	for _, state := range event.regions {
		spansAndChan := resolvedSpans[state.sri.requestedTable.requestID]
		if spansAndChan == nil {
			spansAndChan = &struct {
				spans          []model.RegionComparableSpan
				requestedTable *requestedTable
			}{requestedTable: state.sri.requestedTable}
			resolvedSpans[state.sri.requestedTable.requestID] = spansAndChan
		}

		if state == nil || state.isStopped() || !state.isInitialized() {
			continue
		}
		regionID := state.getRegionID()
		lastResolvedTs := state.getLastResolvedTs()
		if event.ts < lastResolvedTs {
			log.Debug("The resolvedTs is fallen back in kvclient",
				zap.String("namespace", w.changefeed.Namespace),
				zap.String("changefeed", w.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", event.ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs))
			continue
		}
		state.updateResolvedTs(event.ts)

		span := model.RegionComparableSpan{Span: state.sri.span, Region: regionID}
		span.Span.TableID = state.sri.requestedTable.span.TableID
		spansAndChan.spans = append(spansAndChan.spans, span)
	}

	for requestID, spansAndChan := range resolvedSpans {
		log.Debug("region worker get a ResolvedTs",
			zap.String("namespace", w.changefeed.Namespace),
			zap.String("changefeed", w.changefeed.ID),
			zap.Uint64("ResolvedTs", event.ts),
			zap.Uint64("requestID", requestID),
			zap.Int("spanCount", len(spansAndChan.spans)))
		if len(spansAndChan.spans) > 0 {
			revent := model.RegionFeedEvent{Resolved: &model.ResolvedSpans{spansAndChan.spans, event.ts}}
			x := spansAndChan.requestedTable.associateSubscriptionID(revent)
			select {
			case spansAndChan.requestedTable.eventCh <- x:
				w.metrics.metricSendEventResolvedCounter.Add(float64(len(resolvedSpans)))
			case <-ctx.Done():
			}
		}
	}
}
