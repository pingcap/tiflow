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
	"github.com/pingcap/tiflow/cdc/sinkv2/tableevent"
)

// Assert TableSink implementation
var _ TableSink = (*eventTableSink[*model.RowChangedEvent])(nil)
var _ TableSink = (*eventTableSink[*model.SingleTableTxn])(nil)

type eventTableSink[E tableevent.TableEvent] struct {
	eventID         uint64
	maxResolvedTs   model.ResolvedTs
	backendSink     eventsink.EventSink[E]
	progressTracker *progressTracker
	eventAppender   tableevent.Appender[E]
	// NOTICE: It is ordered by commitTs.
	eventBuffer []E
	state       *pipeline.TableState
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
		e.progressTracker.addResolvedTs(e.eventID, resolvedTs)
		e.eventID++
		return
	}
	resolvedEvents := e.eventBuffer[:i]

	resolvedTxnEvents := make([]*tableevent.CallbackableEvent[E], 0, len(resolvedEvents))
	for _, ev := range resolvedEvents {
		txnEvent := &tableevent.CallbackableEvent[E]{
			Event: ev,
			Callback: func() {
				e.progressTracker.remove(e.eventID)
			},
			TableStatus: e.state,
		}
		resolvedTxnEvents = append(resolvedTxnEvents, txnEvent)
		e.progressTracker.addEvent(e.eventID)
		e.eventID++
	}
	// Do not forget to add the resolvedTs to progressTracker.
	e.progressTracker.addResolvedTs(e.eventID, resolvedTs)
	e.eventID++
	e.backendSink.WriteEvents(resolvedTxnEvents...)
}

func (e *eventTableSink[E]) GetCheckpointTs() model.ResolvedTs {
	return e.progressTracker.minTs()
}

func (e *eventTableSink[E]) Close() {
	e.state.Store(pipeline.TableStateStopped)
}
