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
	"sort"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
)

// Assert TableSink implementation
var _ TableSink = (*eventTableSink[*model.RowChangedEvent])(nil)
var _ TableSink = (*eventTableSink[*model.SingleTableTxn])(nil)

type eventTableSink[E eventsink.TableEvent] struct {
	eventID         uint64
	maxResolvedTs   model.ResolvedTs
	backendSink     eventsink.EventSink[E]
	progressTracker *progressTracker
	eventAppender   eventsink.Appender[E]
	// NOTICE: It is ordered by commitTs.
	eventBuffer []E
	state       pipeline.TableState
}

// New an eventTableSink with given backendSink and event appender.
func New[E eventsink.TableEvent](
	backendSink eventsink.EventSink[E],
	appender eventsink.Appender[E],
) *eventTableSink[E] {
	return &eventTableSink[E]{
		eventID:         0,
		maxResolvedTs:   model.NewResolvedTs(0),
		backendSink:     backendSink,
		progressTracker: newProgressTracker(),
		eventAppender:   appender,
		eventBuffer:     make([]E, 0, 1024),
		state:           pipeline.TableStatePreparing,
	}
}

func (e *eventTableSink[E]) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	e.eventBuffer = e.eventAppender.Append(e.eventBuffer, rows...)
}

func (e *eventTableSink[E]) UpdateResolvedTs(resolvedTs model.ResolvedTs) {
	// If resolvedTs is not greater than maxResolvedTs,
	// the flush is unnecessary.
	if !e.maxResolvedTs.Less(resolvedTs) {
		return
	}
	e.maxResolvedTs = resolvedTs

	i := sort.Search(len(e.eventBuffer), func(i int) bool {
		return e.eventBuffer[i].GetCommitTs() > resolvedTs.Ts
	})
	// Despite the lack of data, we have to move forward with progress.
	if i == 0 {
		e.progressTracker.addResolvedTs(e.genEventID(), resolvedTs)
		return
	}
	resolvedEvents := e.eventBuffer[:i]
	e.eventBuffer = append(make([]E, 0, len(e.eventBuffer[i:])), e.eventBuffer[i:]...)

	// We have to create a new slice for the rest of the elements,
	// otherwise we cannot GC the flushed values as soon as possible.
	resolvedCallbackableEvents := make([]*eventsink.CallbackableEvent[E], 0, len(resolvedEvents))

	for _, ev := range resolvedEvents {
		// We have to record the event ID for the callback.
		eventID := e.genEventID()
		ce := &eventsink.CallbackableEvent[E]{
			Event: ev,
			Callback: func() {
				e.progressTracker.remove(eventID)
			},
			TableStatus: &e.state,
		}
		resolvedCallbackableEvents = append(resolvedCallbackableEvents, ce)
		e.progressTracker.addEvent(eventID)
	}
	// Do not forget to add the resolvedTs to progressTracker.
	e.progressTracker.addResolvedTs(e.genEventID(), resolvedTs)
	e.backendSink.WriteEvents(resolvedCallbackableEvents...)
}

func (e *eventTableSink[E]) GetCheckpointTs() model.ResolvedTs {
	return e.progressTracker.minTs()
}

func (e *eventTableSink[E]) Close() {
	e.state.Store(pipeline.TableStateStopped)
}

// genEventID generates an unique ID for event.
func (e *eventTableSink[E]) genEventID() uint64 {
	res := e.eventID
	e.eventID++
	return res
}
