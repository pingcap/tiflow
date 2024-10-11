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
	"encoding/hex"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// The magic number here is keep the same with some magic numbers in some
// other components in TiCDC, including worker pool task chan size, mounter
// chan size etc.
// TODO: unified channel buffer mechanism
var regionWorkerInputChanSize = 32

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

	metricQueueDuration prometheus.Observer

	metricWorkerBusyRatio   prometheus.Gauge
	metricWorkerChannelSize prometheus.Gauge
}

func newRegionWorkerMetrics(changefeedID model.ChangeFeedID, tableID string, storeAddr string) *regionWorkerMetrics {
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

	metrics.metricQueueDuration = regionWorkerQueueDuration.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID)

	metrics.metricWorkerBusyRatio = workerBusyRatio.WithLabelValues(
		changefeedID.Namespace, changefeedID.ID, tableID, storeAddr, "event-handler")
	metrics.metricWorkerChannelSize = workerChannelSize.WithLabelValues(
		changefeedID.Namespace, changefeedID.ID, tableID, storeAddr, "input")

	return metrics
}

// NOTE:
//  1. all contents come from one same TiKV store stream;
//  2. eventItem and resolvedTs shouldn't appear simultaneously;
type statefulEvent struct {
	eventItem       eventItem
	resolvedTsBatch resolvedTsBatch
	stream          *requestedStream
	start           time.Time
}

type eventItem struct {
	// All items come from one same region.
	item  *cdcpb.Event
	state *regionFeedState
}

// NOTE: all regions must come from the same subscribedTable, and regions will never be empty.
type resolvedTsBatch struct {
	ts      uint64
	regions []*regionFeedState
}

func newEventItem(item *cdcpb.Event, state *regionFeedState, stream *requestedStream) statefulEvent {
	return statefulEvent{
		eventItem: eventItem{item, state},
		stream:    stream,
		start:     time.Now(),
	}
}

func newResolvedTsBatch(ts uint64, stream *requestedStream) statefulEvent {
	return statefulEvent{
		resolvedTsBatch: resolvedTsBatch{ts: ts},
		stream:          stream,
		start:           time.Now(),
	}
}

type sharedRegionWorker struct {
	changefeed model.ChangeFeedID
	client     *SharedClient
	inputCh    chan statefulEvent
	metrics    *regionWorkerMetrics
}

func newSharedRegionWorker(c *SharedClient) *sharedRegionWorker {
	return &sharedRegionWorker{
		changefeed: c.changefeed,
		client:     c,
		inputCh:    make(chan statefulEvent, regionWorkerInputChanSize),
		metrics:    newRegionWorkerMetrics(c.changefeed, "shared", "shared"),
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

func (w *sharedRegionWorker) run(ctx context.Context) error {
	for {
		var event statefulEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event = <-w.inputCh:
		}

		w.metrics.metricQueueDuration.Observe(float64(time.Since(event.start).Milliseconds()))
		w.processEvent(ctx, event)
	}
}

func (w *sharedRegionWorker) handleSingleRegionError(state *regionFeedState, stream *requestedStream) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	if err != nil {
		w.client.logRegionDetails("region worker get a region error",
			zap.String("namespace", w.changefeed.Namespace),
			zap.String("changefeed", w.changefeed.ID),
			zap.Uint64("streamID", stream.streamID),
			zap.Any("subscriptionID", state.getRegionID()),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Int64("tableID", state.region.span.TableID),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		stream.takeState(SubscriptionID(state.requestID), state.getRegionID())
		w.client.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

func (w *sharedRegionWorker) processEvent(ctx context.Context, event statefulEvent) {
	if event.eventItem.state != nil {
		state := event.eventItem.state
		if state.isStale() {
			w.handleSingleRegionError(state, event.stream)
			return
		}
		w.metrics.metricReceivedEventSize.Observe(float64(event.eventItem.item.Event.Size()))
		switch x := event.eventItem.item.Event.(type) {
		case *cdcpb.Event_Entries_:
			if err := w.handleEventEntry(ctx, x, state); err != nil {
				state.markStopped(err)
				w.handleSingleRegionError(state, event.stream)
				return
			}
		case *cdcpb.Event_ResolvedTs:
			w.handleResolvedTs(ctx, resolvedTsBatch{
				ts:      x.ResolvedTs,
				regions: []*regionFeedState{state},
			})
		case *cdcpb.Event_Error:
			state.markStopped(&eventError{err: x.Error})
			w.handleSingleRegionError(state, event.stream)
			return
		case *cdcpb.Event_Admin_:
		}
	} else if len(event.resolvedTsBatch.regions) > 0 {
		w.handleResolvedTs(ctx, event.resolvedTsBatch)
	}
}

// NOTE: context.Canceled won't be treated as an error.
func (w *sharedRegionWorker) handleEventEntry(ctx context.Context, x *cdcpb.Event_Entries_, state *regionFeedState) error {
	startTs := state.region.subscribedTable.startTs
	emit := func(assembled model.RegionFeedEvent) bool {
		e := newMultiplexingEvent(assembled, state.region.subscribedTable)
		select {
		case state.region.subscribedTable.eventCh <- e:
			return true
		case <-ctx.Done():
			return false
		}
	}
	tableID := state.region.subscribedTable.span.TableID
	return handleEventEntry(x, startTs, state, w.metrics, emit, w.changefeed, tableID, w.client.logRegionDetails)
}

func handleEventEntry(
	x *cdcpb.Event_Entries_,
	startTs uint64,
	state *regionFeedState,
	metrics *regionWorkerMetrics,
	emit func(assembled model.RegionFeedEvent) bool,
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	logRegionDetails func(msg string, fields ...zap.Field),
) error {
	regionID, regionSpan, _ := state.getRegionMeta()
	for _, entry := range x.Entries.GetEntries() {
		// NOTE: from TiKV 7.0.0, entries are already filtered out in TiKV side.
		// We can remove the check in the future.
		comparableKey := spanz.ToComparableKey(entry.GetKey())
		if entry.Type != cdcpb.Event_INITIALIZED &&
			!spanz.KeyInSpan(comparableKey, regionSpan) {
			metrics.metricDroppedEventSize.Observe(float64(entry.Size()))
			continue
		}
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			metrics.metricPullEventInitializedCounter.Inc()
			state.setInitialized()
			logRegionDetails("region is initialized",
				zap.String("namespace", changefeed.Namespace),
				zap.String("changefeed", changefeed.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("regionID", regionID),
				zap.Int64("tableID", state.region.span.TableID),
				zap.Uint64("requestID", state.requestID),
				zap.Stringer("span", &state.region.span))

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

func assembleRowEvent(regionID uint64, entry *cdcpb.Event_Row) (model.RegionFeedEvent, error) {
	var opType model.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = model.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = model.OpTypePut
	default:
		return model.RegionFeedEvent{}, cerror.ErrUnknownKVEventType.GenWithStackByArgs(entry.GetOpType(), entry)
	}

	revent := model.RegionFeedEvent{
		RegionID: regionID,
		Val: &model.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		},
	}

	return revent, nil
}

func (w *sharedRegionWorker) handleResolvedTs(ctx context.Context, batch resolvedTsBatch) {
	if w.client.config.KVClient.AdvanceIntervalInMs > 0 {
		w.advanceTableSpan(ctx, batch)
	} else {
		w.forwardResolvedTsToPullerFrontier(ctx, batch)
	}
}

func (w *sharedRegionWorker) forwardResolvedTsToPullerFrontier(ctx context.Context, batch resolvedTsBatch) {
	resolvedSpans := make(map[SubscriptionID]*struct {
		spans           []model.RegionComparableSpan
		subscribedTable *subscribedTable
	})

	for _, state := range batch.regions {
		if state.isStale() || !state.isInitialized() {
			continue
		}

		spansAndChan := resolvedSpans[state.region.subscribedTable.subscriptionID]
		if spansAndChan == nil {
			spansAndChan = &struct {
				spans           []model.RegionComparableSpan
				subscribedTable *subscribedTable
			}{subscribedTable: state.region.subscribedTable}
			resolvedSpans[state.region.subscribedTable.subscriptionID] = spansAndChan
		}

		regionID := state.getRegionID()
		lastResolvedTs := state.getLastResolvedTs()
		if batch.ts < lastResolvedTs {
			log.Debug("The resolvedTs is fallen back in kvclient",
				zap.String("namespace", w.changefeed.Namespace),
				zap.String("changefeed", w.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", batch.ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs))
			continue
		}
		state.updateResolvedTs(batch.ts)

		span := model.RegionComparableSpan{Span: state.region.span, Region: regionID}
		span.Span.TableID = state.region.subscribedTable.span.TableID
		spansAndChan.spans = append(spansAndChan.spans, span)
	}

	for subscriptionID, spansAndChan := range resolvedSpans {
		log.Debug("region worker get a ResolvedTs",
			zap.String("namespace", w.changefeed.Namespace),
			zap.String("changefeed", w.changefeed.ID),
			zap.Any("subscriptionID", subscriptionID),
			zap.Uint64("ResolvedTs", batch.ts),
			zap.Int("spanCount", len(spansAndChan.spans)))
		if len(spansAndChan.spans) > 0 {
			revent := model.RegionFeedEvent{Resolved: &model.ResolvedSpans{
				Spans: spansAndChan.spans, ResolvedTs: batch.ts,
			}}
			e := newMultiplexingEvent(revent, spansAndChan.subscribedTable)
			select {
			case spansAndChan.subscribedTable.eventCh <- e:
				w.metrics.metricSendEventResolvedCounter.Add(float64(len(resolvedSpans)))
			case <-ctx.Done():
			}
		}
	}
}

func (w *sharedRegionWorker) advanceTableSpan(ctx context.Context, batch resolvedTsBatch) {
	for _, state := range batch.regions {
		if state.isStale() || !state.isInitialized() {
			continue
		}

		regionID := state.getRegionID()
		lastResolvedTs := state.getLastResolvedTs()
		if batch.ts < lastResolvedTs {
			log.Debug("The resolvedTs is fallen back in kvclient",
				zap.String("namespace", w.changefeed.Namespace),
				zap.String("changefeed", w.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", batch.ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs))
			continue
		}
		state.updateResolvedTs(batch.ts)
	}

	table := batch.regions[0].region.subscribedTable
	now := time.Now().UnixMilli()
	lastAdvance := table.lastAdvanceTime.Load()
	if now-lastAdvance > int64(w.client.config.KVClient.AdvanceIntervalInMs) && table.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := table.rangeLock.ResolvedTs()
		if ts > table.startTs {
			revent := model.RegionFeedEvent{
				Resolved: &model.ResolvedSpans{
					Spans:      []model.RegionComparableSpan{{Span: table.span, Region: 0}},
					ResolvedTs: ts,
				},
			}
			e := newMultiplexingEvent(revent, table)
			select {
			case table.eventCh <- e:
				w.metrics.metricSendEventResolvedCounter.Add(float64(len(batch.regions)))
			case <-ctx.Done():
			}
		}
	}
}
