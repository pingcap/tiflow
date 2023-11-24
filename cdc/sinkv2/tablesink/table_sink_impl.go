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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Assert TableSink implementation
var (
	_ TableSink = (*EventTableSink[*model.RowChangedEvent])(nil)
	_ TableSink = (*EventTableSink[*model.SingleTableTxn])(nil)
)

// EventTableSink is a table sink that can write events.
type EventTableSink[E eventsink.TableEvent] struct {
	changefeedID model.ChangeFeedID
	tableID      model.TableID
	eventID      uint64
	// startTs is the initial checkpointTs of the table sink.
	startTs         model.Ts
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
	changefeedID model.ChangeFeedID,
	tableID model.TableID,
	startTs model.Ts,
	backendSink eventsink.EventSink[E],
	appender eventsink.Appender[E],
	totalRowsCounter prometheus.Counter,
) *EventTableSink[E] {
	return &EventTableSink[E]{
		changefeedID:              changefeedID,
		tableID:                   tableID,
		eventID:                   0,
		startTs:                   startTs,
		maxResolvedTs:             model.NewResolvedTs(0),
		backendSink:               backendSink,
		progressTracker:           newProgressTracker(tableID, defaultBufferSize),
		eventAppender:             appender,
		eventBuffer:               make([]E, 0, 1024),
		state:                     state.TableSinkSinking,
		metricsTableSinkTotalRows: totalRowsCounter,
	}
}

// AppendRowChangedEvents appends row changed or txn events to the table sink.
func (e *EventTableSink[E]) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	e.eventBuffer = e.eventAppender.Append(e.eventBuffer, rows...)
	e.metricsTableSinkTotalRows.Add(float64(len(rows)))
}

// UpdateResolvedTs advances the resolved ts of the table sink.
func (e *EventTableSink[E]) UpdateResolvedTs(resolvedTs model.ResolvedTs) error {
	// If resolvedTs is not greater than maxResolvedTs,
	// the flush is unnecessary.
	if e.maxResolvedTs.EqualOrGreater(resolvedTs) {
		return nil
	}
	e.maxResolvedTs = resolvedTs

	i := sort.Search(len(e.eventBuffer), func(i int) bool {
		return e.eventBuffer[i].GetCommitTs() > resolvedTs.Ts
	})
	// Despite the lack of data, we have to move forward with progress.
	if i == 0 {
		// WriteEvents must be called to check whether the backend sink is dead
		// or not, even if there is no more events. So if the backend is dead
		// and re-initialized, we can know it and re-build a table sink.
		e.progressTracker.addResolvedTs(resolvedTs)
		if err := e.backendSink.WriteEvents(); err != nil {
			return SinkInternalError{err}
		}
		return nil
	}
	resolvedEvents := e.eventBuffer[:i]

	// We have to create a new slice for the rest of the elements,
	// otherwise we cannot GC the flushed values as soon as possible.
	e.eventBuffer = append(make([]E, 0, len(e.eventBuffer[i:])), e.eventBuffer[i:]...)

	resolvedCallbackableEvents := make([]*eventsink.CallbackableEvent[E], 0, len(resolvedEvents))
	for _, ev := range resolvedEvents {
		if err := ev.TrySplitAndSortUpdateEvent(e.backendSink.Scheme()); err != nil {
			return SinkInternalError{err}
		}
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
	if err := e.backendSink.WriteEvents(resolvedCallbackableEvents...); err != nil {
		return SinkInternalError{err}
	}
	return nil
}

// GetCheckpointTs returns the checkpoint ts of the table sink.
func (e *EventTableSink[E]) GetCheckpointTs() model.ResolvedTs {
	if e.state.Load() == state.TableSinkStopping {
		if e.progressTracker.checkClosed(e.backendSink.Dead()) {
			e.markAsClosed()
		}
	}
	return e.progressTracker.advance()
}

// Close closes the table sink.
// After it returns, no more events will be sent out from this capture.
func (e *EventTableSink[E]) Close() {
	e.freeze()
	e.progressTracker.waitClosed(e.backendSink.Dead())
	e.markAsClosed()
}

// AsyncClose closes the table sink asynchronously. Returns true if it's closed.
func (e *EventTableSink[E]) AsyncClose() bool {
	e.freeze()
	if e.progressTracker.checkClosed(e.backendSink.Dead()) {
		e.markAsClosed()
		return true
	}
	return false
}

// CheckHealth checks whether the associated sink backend is healthy or not.
func (e *EventTableSink[E]) CheckHealth() error {
	if err := e.backendSink.WriteEvents(); err != nil {
		return SinkInternalError{err}
	}
	return nil
}

func (e *EventTableSink[E]) freeze() {
	// Notice: We have to set the state to stopping first,
	// otherwise the progressTracker may be advanced incorrectly.
	// For example, if we do not freeze it and set the state to stooping
	// then the progressTracker may be advanced to the checkpointTs
	// because backend sink drops some events.
	e.progressTracker.freezeProcess()

	for {
		currentState := e.state.Load()
		if currentState == state.TableSinkStopping || currentState == state.TableSinkStopped {
			break
		}
		if e.state.CompareAndSwap(currentState, state.TableSinkStopping) {
			stoppingCheckpointTs := e.GetCheckpointTs()
			log.Info("Stopping table sink",
				zap.String("namespace", e.changefeedID.Namespace),
				zap.String("changefeed", e.changefeedID.ID),
				zap.Uint64("tableID", uint64(e.tableID)),
				zap.Uint64("checkpointTs", stoppingCheckpointTs.Ts))
			break
		}
	}
}

func (e *EventTableSink[E]) markAsClosed() (modified bool) {
	for {
		currentState := e.state.Load()
		if currentState == state.TableSinkStopped {
			return
		}
		if e.state.CompareAndSwap(currentState, state.TableSinkStopped) {
			stoppedCheckpointTs := e.GetCheckpointTs()
			log.Info("Table sink stopped",
				zap.String("namespace", e.changefeedID.Namespace),
				zap.String("changefeed", e.changefeedID.ID),
				zap.Uint64("tableID", uint64(e.tableID)),
				zap.Uint64("checkpointTs", stoppedCheckpointTs.Ts))
			return true
		}
	}
}
