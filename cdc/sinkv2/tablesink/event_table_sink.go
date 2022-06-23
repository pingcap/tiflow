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
	"github.com/pingcap/tiflow/cdc/sinkv2/event"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"go.uber.org/atomic"
)

// Assert TableSink implementation
var _ TableSink = (*eventTableSink[*model.RowChangedEvent])(nil)
var _ TableSink = (*eventTableSink[*model.SingleTableTxn])(nil)

type eventTableSink[E event.TableEvent] struct {
	eventID         uint64
	maxResolvedTs   model.ResolvedTs
	backendSink     eventsink.EventSink[E]
	progressTracker *progressTracker
	eventAppender   event.Appender[E]
	// NOTICE: It is ordered by commitTs.
	eventBuffer []E
	TableStatus *atomic.Uint32
}

func (e *eventTableSink[E]) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	e.eventAppender.Append(e.eventBuffer, rows...)
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
	if i == 0 {
		return
	}
	resolvedEvents := e.eventBuffer[:i]

	resolvedTxnEvents := make([]*event.CallbackableEvent[E], 0, len(resolvedEvents))
	for _, ev := range resolvedEvents {
		txnEvent := &event.CallbackableEvent[E]{
			Event: ev,
			Callback: func() {
				e.progressTracker.remove(e.eventID)
			},
			TableStatus: e.TableStatus,
		}
		resolvedTxnEvents = append(resolvedTxnEvents, txnEvent)
		e.progressTracker.addEvent(e.eventID)
		e.eventID++
	}
	e.progressTracker.addResolvedTs(e.eventID, resolvedTs)
	e.eventID++
	e.backendSink.WriteEvents(resolvedTxnEvents...)
}

func (e *eventTableSink[E]) GetCheckpointTs() model.ResolvedTs {
	return e.progressTracker.minTs()
}

func (e *eventTableSink[E]) Close() {
	// TODO implement me
	panic("implement me")
}
