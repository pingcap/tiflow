// Copyright 2022 PingCAP, Inc.
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

package tablesink

import (
	"context"
	"sort"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/prometheus/client_golang/prometheus"
)

// Assert TableSink implementation
var _ TableSink = (*eventTableSink[*model.RowChangedEvent])(nil)
var _ TableSink = (*eventTableSink[*model.SingleTableTxn])(nil)

type eventTableSink[E eventsink.TableEvent] struct {
	tableID         model.TableID
	eventID         uint64
	maxResolvedTs   model.ResolvedTs
	backendSink     eventsink.EventSink[E]
	progressTracker *progressTracker
	eventAppender   eventsink.Appender[E]
	// NOTICE: It is ordered by commitTs.
	eventBuffer []E
	state       state.TableSinkState

	// For dataflow metrics.
	metricsTableSinkTotalRows prometheus.Counter
}

// New an eventTableSink with given backendSink and event appender.
func New[E eventsink.TableEvent](
	tableID model.TableID,
	backendSink eventsink.EventSink[E],
	appender eventsink.Appender[E],
	totalRowsCounter prometheus.Counter,
) *eventTableSink[E] {
	return &eventTableSink[E]{
		tableID:                   tableID,
		eventID:                   0,
		maxResolvedTs:             model.NewResolvedTs(0),
		backendSink:               backendSink,
		progressTracker:           newProgressTracker(tableID, defaultBufferSize),
		eventAppender:             appender,
		eventBuffer:               make([]E, 0, 1024),
		state:                     state.TableSinkSinking,
		metricsTableSinkTotalRows: totalRowsCounter,
	}
}

func (e *eventTableSink[E]) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	e.eventBuffer = e.eventAppender.Append(e.eventBuffer, rows...)
	e.metricsTableSinkTotalRows.Add(float64(len(rows)))
}

func (e *eventTableSink[E]) UpdateResolvedTs(resolvedTs model.ResolvedTs) error {
	// If resolvedTs is not greater than maxResolvedTs,
	// the flush is unnecessary.
	if !e.maxResolvedTs.Less(resolvedTs) {
		return nil
	}
	e.maxResolvedTs = resolvedTs

	i := sort.Search(len(e.eventBuffer), func(i int) bool {
		return e.eventBuffer[i].GetCommitTs() > resolvedTs.Ts
	})
	// Despite the lack of data, we have to move forward with progress.
	if i == 0 {
		e.progressTracker.addResolvedTs(resolvedTs)
		return nil
	}
	resolvedEvents := e.eventBuffer[:i]
	e.eventBuffer = append(make([]E, 0, len(e.eventBuffer[i:])), e.eventBuffer[i:]...)

	// We have to create a new slice for the rest of the elements,
	// otherwise we cannot GC the flushed values as soon as possible.
	resolvedCallbackableEvents := make([]*eventsink.CallbackableEvent[E], 0, len(resolvedEvents))

	for _, ev := range resolvedEvents {
		// We have to record the event ID for the callback.
		ce := &eventsink.CallbackableEvent[E]{
			Event:     ev,
			Callback:  e.progressTracker.addEvent(),
			SinkState: &e.state,
		}
		resolvedCallbackableEvents = append(resolvedCallbackableEvents, ce)
	}
	// Do not forget to add the resolvedTs to progressTracker.
	e.progressTracker.addResolvedTs(resolvedTs)
	return e.backendSink.WriteEvents(resolvedCallbackableEvents...)
}

func (e *eventTableSink[E]) GetCheckpointTs() model.ResolvedTs {
	return e.progressTracker.advance()
}

// Close the table sink and wait for all callbacks be called.
// Notice: It will be blocked until all callbacks be called.
func (e *eventTableSink[E]) Close(ctx context.Context) error {
	e.state.Store(state.TableSinkStopping)
	err := e.progressTracker.close(ctx)
	if err != nil {
		return err
	}
	e.state.Store(state.TableSinkStopped)

	return nil
}
