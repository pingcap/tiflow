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

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

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
	changefeed    model.ChangeFeedID
	client        *SharedClient
	statesManager *regionStateManager
	inputCh       chan statefulEvent
	metrics       *regionWorkerMetrics
}

func newSharedRegionWorker(c *SharedClient) *sharedRegionWorker {
	return &sharedRegionWorker{
		changefeed:    c.changefeed,
		client:        c,
		inputCh:       make(chan statefulEvent, regionWorkerInputChanSize),
		statesManager: newRegionStateManager(-1),
		metrics:       newRegionWorkerMetrics(c.changefeed),
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

func (w *sharedRegionWorker) handleSingleRegionError(
	ctx context.Context, err error,
	state *regionFeedState,
	stream *requestedStream,
) {
	state.markStopped(nil)
	if stream != nil {
		stream.takeState(state.requestID, state.getRegionID())
	}
	if state.markRemoved() {
		w.client.onRegionFail(ctx, newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

func (w *sharedRegionWorker) processEvent(ctx context.Context, event statefulEvent) {
	if event.eventItem.state != nil {
		state := event.eventItem.state
		if state.isStopped() {
			if err := state.takeError(); err != nil {
				w.handleSingleRegionError(ctx, err, state, event.stream)
				return
			}
		}
		w.metrics.metricReceivedEventSize.Observe(float64(event.eventItem.item.Event.Size()))
		switch x := event.eventItem.item.Event.(type) {
		case *cdcpb.Event_Entries_:
			if err := w.handleEventEntry(ctx, x, state); err != nil {
				w.handleSingleRegionError(ctx, err, state, event.stream)
				return
			}
		case *cdcpb.Event_Admin_:
		case *cdcpb.Event_Error:
			err := cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error})
			w.handleSingleRegionError(ctx, err, state, event.stream)
			return
		case *cdcpb.Event_ResolvedTs:
			w.handleResolvedTs(ctx, resolvedTsBatch{
				ts:      x.ResolvedTs,
				regions: []*regionFeedState{state},
			})
		}
	} else if len(event.resolvedTsBatch.regions) > 0 {
		w.handleResolvedTs(ctx, event.resolvedTsBatch)
	}
}

// NOTE: context.Canceled won't be treated as an error.
func (w *sharedRegionWorker) handleEventEntry(ctx context.Context, x *cdcpb.Event_Entries_, state *regionFeedState) error {
	startTs := state.sri.requestedTable.startTs
	emit := func(assembled model.RegionFeedEvent) bool {
		x := state.sri.requestedTable.associateSubscriptionID(assembled)
		select {
		case state.sri.requestedTable.eventCh <- x:
			return true
		case <-ctx.Done():
			return false
		}
	}

	return handleEventEntry(w.changefeed, x, startTs, state, w.metrics, emit)
}

func (w *sharedRegionWorker) handleResolvedTs(ctx context.Context, batch resolvedTsBatch) {
	resolvedSpans := make(map[uint64]*struct {
		spans          []model.RegionComparableSpan
		requestedTable *requestedTable
	})

	for _, state := range batch.regions {
		if state.isStopped() || !state.isInitialized() {
			continue
		}

		spansAndChan := resolvedSpans[state.sri.requestedTable.requestID]
		if spansAndChan == nil {
			spansAndChan = &struct {
				spans          []model.RegionComparableSpan
				requestedTable *requestedTable
			}{requestedTable: state.sri.requestedTable}
			resolvedSpans[state.sri.requestedTable.requestID] = spansAndChan
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

		span := model.RegionComparableSpan{Span: state.sri.span, Region: regionID}
		span.Span.TableID = state.sri.requestedTable.span.TableID
		spansAndChan.spans = append(spansAndChan.spans, span)
	}

	for requestID, spansAndChan := range resolvedSpans {
		log.Debug("region worker get a ResolvedTs",
			zap.String("namespace", w.changefeed.Namespace),
			zap.String("changefeed", w.changefeed.ID),
			zap.Uint64("ResolvedTs", batch.ts),
			zap.Uint64("requestID", requestID),
			zap.Int("spanCount", len(spansAndChan.spans)))
		if len(spansAndChan.spans) > 0 {
			revent := model.RegionFeedEvent{Resolved: &model.ResolvedSpans{
				Spans: spansAndChan.spans, ResolvedTs: batch.ts,
			}}
			x := spansAndChan.requestedTable.associateSubscriptionID(revent)
			select {
			case spansAndChan.requestedTable.eventCh <- x:
				w.metrics.metricSendEventResolvedCounter.Add(float64(len(resolvedSpans)))
			case <-ctx.Done():
			}
		}
	}
}
