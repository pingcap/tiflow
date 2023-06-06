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

type sharedRegionWorker struct {
	changefeed model.ChangeFeedID

	client *SharedClient

	statesManager *regionStateManager

	inputCh  chan []*regionStatefulEvent
	outputCh chan<- model.RegionFeedEvent
	errorCh  chan error

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
		inputCh:       make(chan []*regionStatefulEvent, regionWorkerInputChanSize),
		outputCh:      nil, // FIXME(qupeng): from rs.
		statesManager: newRegionStateManager(-1),
		metrics:       metrics,
	}
}

func (w *sharedRegionWorker) run(ctx context.Context) error {
	preprocess := func(event *regionStatefulEvent) (skipEvent bool) {
		if event.state != nil && event.state.isStopped() {
			skipEvent = true
		}
		return
	}

	for {
		var events []*regionStatefulEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case events = <-w.inputCh:
		}

		for _, event := range events {
			if skip := preprocess(event); skip {
				continue
			}
			if err := w.processEvent(ctx, event); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *sharedRegionWorker) processEvent(ctx context.Context, event *regionStatefulEvent) (err error) {
	if event.finishedCallbackCh != nil {
		event.finishedCallbackCh <- struct{}{}
		return nil
	}

	if event.changeEvent != nil {
		switch x := event.changeEvent.Event.(type) {
		case *cdcpb.Event_Entries_:
			return w.handleEventEntry(ctx, x, event.state)
		case *cdcpb.Event_Admin_:
		case *cdcpb.Event_Error:
			// FIXME(qupeng): handle it correctly.
		case *cdcpb.Event_ResolvedTs:
			return w.handleResolvedTs(ctx, &resolvedTsEvent{
				resolvedTs: x.ResolvedTs,
				regions:    []*regionFeedState{event.state},
			})
		}
	} else if event.resolvedTsEvent != nil {
		return w.handleResolvedTs(ctx, event.resolvedTsEvent)
	}

	return nil
}

func (w *sharedRegionWorker) handleEventEntry(ctx context.Context, x *cdcpb.Event_Entries_, state *regionFeedState) error {
	regionID, _, startTime, _ := state.getRegionMeta()
	for _, entry := range x.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			if time.Since(startTime) > 20*time.Second {
				log.Warn("The time cost of initializing is too much",
					zap.String("namespace", w.changefeed.Namespace),
					zap.String("changefeed", w.changefeed.ID),
					zap.Duration("duration", time.Since(startTime)),
					zap.Uint64("regionID", regionID))
			}
			w.metrics.metricPullEventInitializedCounter.Inc()

			state.setInitialized()
			cachedEvents := state.matcher.matchCachedRow(true)
			for _, cachedEvent := range cachedEvents {
				revent, err := assembleRowEvent(regionID, cachedEvent)
				if err != nil {
					return errors.Trace(err)
				}
				select {
				case w.outputCh <- revent:
					w.metrics.metricSendEventCommitCounter.Inc()
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			state.matcher.matchCachedRollbackRow(true)
		case cdcpb.Event_COMMITTED:
			w.metrics.metricPullEventCommittedCounter.Inc()
			revent, err := assembleRowEvent(regionID, entry)
			if err != nil {
				return errors.Trace(err)
			}

			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				logPanic("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return errUnreachable
			}
			select {
			case w.outputCh <- revent:
				w.metrics.metricSendEventCommittedCounter.Inc()
			case <-ctx.Done():
				return ctx.Err()
			}
		case cdcpb.Event_PREWRITE:
			w.metrics.metricPullEventPrewriteCounter.Inc()
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			w.metrics.metricPullEventCommitCounter.Inc()
			// NOTE: state.getLastResolvedTs() will never less than session.startTs.
			resolvedTs := state.getLastResolvedTs()
			// TiKV can send events with StartTs/CommitTs less than startTs.
			// FIXME(qupeng): is `startTs` correct? No!
			isStaleEvent := entry.CommitTs <= w.client.startTs
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
				return cerror.ErrPrewriteNotMatch.GenWithStackByArgs(
					hex.EncodeToString(entry.GetKey()),
					entry.GetStartTs(), entry.GetCommitTs(),
					entry.GetType(), entry.GetOpType())
			}

			if !isStaleEvent {
				revent, err := assembleRowEvent(regionID, entry)
				if err != nil {
					return errors.Trace(err)
				}
				select {
				case w.outputCh <- revent:
					w.metrics.metricSendEventCommitCounter.Inc()
				case <-ctx.Done():
					return ctx.Err()
				}
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

func (w *sharedRegionWorker) handleResolvedTs(ctx context.Context, revents *resolvedTsEvent) error {
	resolvedTs := revents.resolvedTs
	resolvedSpans := make([]model.RegionComparableSpan, 0, len(revents.regions))
	regions := make([]uint64, 0, len(revents.regions))

	for _, state := range revents.regions {
		if state.isStopped() || !state.isInitialized() {
			continue
		}
		regionID := state.getRegionID()
		regions = append(regions, regionID)
		lastResolvedTs := state.getLastResolvedTs()
		if resolvedTs < lastResolvedTs {
			log.Debug("The resolvedTs is fallen back in kvclient",
				zap.String("namespace", w.changefeed.Namespace),
				zap.String("changefeed", w.changefeed.ID),
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
	for _, state := range revents.regions {
		if state.isStopped() || !state.isInitialized() {
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
		return ctx.Err()
	}
	return nil
}
