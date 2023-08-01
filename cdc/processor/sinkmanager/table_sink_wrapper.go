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

package sinkmanager

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var version uint64 = 0

// tableSinkWrapper is a wrapper of TableSink, it is used in SinkManager to manage TableSink.
// Because in the SinkManager, we write data to TableSink and RedoManager concurrently,
// so current sink node can not be reused.
type tableSinkWrapper struct {
	version uint64

	// changefeed used for logging.
	changefeed model.ChangeFeedID
	// tableSpan used for logging.
	span tablepb.Span

	tableSinkCreater func() tablesink.TableSink

	// tableSink is the underlying sink.
	tableSink             tablesink.TableSink
	tableSinkCheckpointTs model.ResolvedTs
	tableSinkMu           sync.RWMutex

	// state used to control the lifecycle of the table.
	state *tablepb.TableState
	// startTs is the start ts of the table.
	startTs model.Ts
	// targetTs is the upper bound of the table sink.
	targetTs model.Ts
	// barrierTs is the barrier bound of the table sink.
	barrierTs atomic.Uint64
	// receivedSorterResolvedTs is the resolved ts received from the sorter.
	// We use this to advance the redo log.
	receivedSorterResolvedTs atomic.Uint64

	// replicateTs is the ts that the table sink has started to replicate.
	replicateTs    model.Ts
	genReplicateTs func(ctx context.Context) (model.Ts, error)

	// lastCleanTime indicates the last time the table has been cleaned.
	lastCleanTime time.Time

	// rangeEventCounts is for clean the table engine.
	// If rangeEventCounts[i].events is greater than 0, it means there must be
	// events in the range (rangeEventCounts[i-1].lastPos, rangeEventCounts[i].lastPos].
	rangeEventCounts   []rangeEventCount
	rangeEventCountsMu sync.Mutex
}

type rangeEventCount struct {
	// firstPos and lastPos are used to merge many rangeEventCount into one.
	firstPos engine.Position
	lastPos  engine.Position
	events   int
}

func newRangeEventCount(pos engine.Position, events int) rangeEventCount {
	return rangeEventCount{
		firstPos: pos,
		lastPos:  pos,
		events:   events,
	}
}

func newTableSinkWrapper(
	changefeed model.ChangeFeedID,
	span tablepb.Span,
	tableSinkCreater func() tablesink.TableSink,
	state tablepb.TableState,
	startTs model.Ts,
	targetTs model.Ts,
	genReplicateTs func(ctx context.Context) (model.Ts, error),
) *tableSinkWrapper {
	res := &tableSinkWrapper{
		version:          atomic.AddUint64(&version, 1),
		changefeed:       changefeed,
		span:             span,
		tableSinkCreater: tableSinkCreater,
		state:            &state,
		startTs:          startTs,
		targetTs:         targetTs,
		genReplicateTs:   genReplicateTs,
	}
	res.tableSinkCheckpointTs = model.NewResolvedTs(startTs)
	res.receivedSorterResolvedTs.Store(startTs)
	res.barrierTs.Store(startTs)
	return res
}

func (t *tableSinkWrapper) start(ctx context.Context, startTs model.Ts) (err error) {
	if t.replicateTs != 0 {
		log.Panic("The table sink has already started",
			zap.String("namespace", t.changefeed.Namespace),
			zap.String("changefeed", t.changefeed.ID),
			zap.Stringer("span", &t.span),
			zap.Uint64("startTs", startTs),
			zap.Uint64("oldReplicateTs", t.replicateTs),
		)
	}

	// FIXME(qupeng): it can be re-fetched later instead of fails.
	if t.replicateTs, err = t.genReplicateTs(ctx); err != nil {
		return errors.Trace(err)
	}

	log.Info("Sink is started",
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID),
		zap.Stringer("span", &t.span),
		zap.Uint64("startTs", startTs),
		zap.Uint64("replicateTs", t.replicateTs),
	)

	// This start ts maybe greater than the initial start ts of the table sink.
	// Because in two phase scheduling, the table sink may be advanced to a later ts.
	// And we can just continue to replicate the table sink from the new start ts.
	for {
		old := t.receivedSorterResolvedTs.Load()
		if startTs <= old || t.receivedSorterResolvedTs.CompareAndSwap(old, startTs) {
			break
		}
	}
	t.state.Store(tablepb.TableStateReplicating)
	return nil
}

func (t *tableSinkWrapper) appendRowChangedEvents(events ...*model.RowChangedEvent) error {
	t.tableSinkMu.RLock()
	defer t.tableSinkMu.RUnlock()
	if t.tableSink == nil {
		// If it's nil it means it's closed.
		return tablesink.NewSinkInternalError(errors.New("table sink cleared"))
	}
	t.tableSink.AppendRowChangedEvents(events...)
	return nil
}

func (t *tableSinkWrapper) updateReceivedSorterResolvedTs(ts model.Ts) {
	for {
		old := t.receivedSorterResolvedTs.Load()
		if ts <= old {
			return
		}
		if t.receivedSorterResolvedTs.CompareAndSwap(old, ts) {
			if t.state.Load() == tablepb.TableStatePreparing {
				t.state.Store(tablepb.TableStatePrepared)
			}
			return
		}
	}
}

func (t *tableSinkWrapper) updateResolvedTs(ts model.ResolvedTs) error {
	t.tableSinkMu.RLock()
	defer t.tableSinkMu.RUnlock()
	if t.tableSink == nil {
		// If it's nil it means it's closed.
		return tablesink.NewSinkInternalError(errors.New("table sink cleared"))
	}
	return t.tableSink.UpdateResolvedTs(ts)
}

func (t *tableSinkWrapper) getCheckpointTs() model.ResolvedTs {
	t.tableSinkMu.RLock()
	defer t.tableSinkMu.RUnlock()
	if t.tableSink != nil {
		checkpointTs := t.tableSink.GetCheckpointTs()
		if t.tableSinkCheckpointTs.Less(checkpointTs) {
			t.tableSinkCheckpointTs = checkpointTs
		}
	}
	return t.tableSinkCheckpointTs
}

func (t *tableSinkWrapper) getReceivedSorterResolvedTs() model.Ts {
	return t.receivedSorterResolvedTs.Load()
}

func (t *tableSinkWrapper) getState() tablepb.TableState {
	return t.state.Load()
}

// getUpperBoundTs returns the upperbound of the table sink.
// It is used by sinkManager to generate sink task.
// upperBoundTs should be the minimum of the following two values:
// 1. the resolved ts of the sorter
// 2. the barrier ts of the table
func (t *tableSinkWrapper) getUpperBoundTs() model.Ts {
	resolvedTs := t.getReceivedSorterResolvedTs()
	barrierTs := t.barrierTs.Load()
	if resolvedTs > barrierTs {
		resolvedTs = barrierTs
	}
	return resolvedTs
}

func (t *tableSinkWrapper) markAsClosing() {
	for {
		curr := t.state.Load()
		if curr == tablepb.TableStateStopping || curr == tablepb.TableStateStopped {
			break
		}
		if t.state.CompareAndSwap(curr, tablepb.TableStateStopping) {
			log.Info("Sink is closing",
				zap.String("namespace", t.changefeed.Namespace),
				zap.String("changefeed", t.changefeed.ID),
				zap.Stringer("span", &t.span))
			break
		}
	}
}

func (t *tableSinkWrapper) markAsClosed() {
	for {
		curr := t.state.Load()
		if curr == tablepb.TableStateStopped {
			return
		}
		if t.state.CompareAndSwap(curr, tablepb.TableStateStopped) {
			log.Info("Sink is closed",
				zap.String("namespace", t.changefeed.Namespace),
				zap.String("changefeed", t.changefeed.ID),
				zap.Stringer("span", &t.span))
			return
		}
	}
}

func (t *tableSinkWrapper) asyncStop() bool {
	t.markAsClosing()
	if t.asyncCloseAndClearTableSink() {
		t.markAsClosed()
		return true
	}
	return false
}

func (t *tableSinkWrapper) stop() {
	t.markAsClosing()
	t.closeAndClearTableSink()
	t.markAsClosed()
}

// Return true means the internal table sink has been initialized.
func (t *tableSinkWrapper) initTableSink() bool {
	t.tableSinkMu.Lock()
	defer t.tableSinkMu.Unlock()
	if t.tableSink == nil {
		t.tableSink = t.tableSinkCreater()
		return t.tableSink != nil
	}
	return true
}

func (t *tableSinkWrapper) asyncCloseTableSink() bool {
	t.tableSinkMu.RLock()
	defer t.tableSinkMu.RUnlock()
	if t.tableSink == nil {
		return true
	}
	return t.tableSink.AsyncClose()
}

func (t *tableSinkWrapper) closeTableSink() {
	t.tableSinkMu.RLock()
	defer t.tableSinkMu.RUnlock()
	if t.tableSink == nil {
		return
	}
	t.tableSink.Close()
}

func (t *tableSinkWrapper) asyncCloseAndClearTableSink() bool {
	t.asyncCloseTableSink()
	t.doTableSinkClear()
	return true
}

func (t *tableSinkWrapper) closeAndClearTableSink() {
	t.closeTableSink()
	t.doTableSinkClear()
}

func (t *tableSinkWrapper) doTableSinkClear() {
	t.tableSinkMu.Lock()
	defer t.tableSinkMu.Unlock()
	if t.tableSink == nil {
		return
	}
	checkpointTs := t.tableSink.GetCheckpointTs()
	if t.tableSinkCheckpointTs.Less(checkpointTs) {
		t.tableSinkCheckpointTs = checkpointTs
	}
	t.tableSink = nil
}

// When the attached sink fail, there can be some events that have already been
// committed at downstream but we don't know. So we need to update `replicateTs`
// of the table so that we can re-send those events later.
func (t *tableSinkWrapper) restart(ctx context.Context) (err error) {
	if t.replicateTs, err = t.genReplicateTs(ctx); err != nil {
		return errors.Trace(err)
	}
	log.Info("Sink is restarted",
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID),
		zap.Stringer("span", &t.span),
		zap.Uint64("replicateTs", t.replicateTs))
	return nil
}

func (t *tableSinkWrapper) updateRangeEventCounts(eventCount rangeEventCount) {
	t.rangeEventCountsMu.Lock()
	defer t.rangeEventCountsMu.Unlock()

	countsLen := len(t.rangeEventCounts)
	if countsLen == 0 {
		t.rangeEventCounts = append(t.rangeEventCounts, eventCount)
		return
	}
	if t.rangeEventCounts[countsLen-1].lastPos.Compare(eventCount.lastPos) < 0 {
		// If two rangeEventCounts are close enough, we can merge them into one record
		// to save memory usage. When merging B into A, A.lastPos will be updated but
		// A.firstPos will be kept so that we can determine whether to continue to merge
		// more events or not based on timeDiff(C.lastPos, A.firstPos).
		lastPhy := oracle.ExtractPhysical(t.rangeEventCounts[countsLen-1].firstPos.CommitTs)
		currPhy := oracle.ExtractPhysical(eventCount.lastPos.CommitTs)
		if (currPhy - lastPhy) >= 1000 { // 1000 means 1000ms.
			t.rangeEventCounts = append(t.rangeEventCounts, eventCount)
		} else {
			t.rangeEventCounts[countsLen-1].lastPos = eventCount.lastPos
			t.rangeEventCounts[countsLen-1].events += eventCount.events
		}
	}
}

func (t *tableSinkWrapper) cleanRangeEventCounts(upperBound engine.Position, minEvents int) bool {
	t.rangeEventCountsMu.Lock()
	defer t.rangeEventCountsMu.Unlock()

	idx := sort.Search(len(t.rangeEventCounts), func(i int) bool {
		return t.rangeEventCounts[i].lastPos.Compare(upperBound) > 0
	})
	if len(t.rangeEventCounts) == 0 || idx == 0 {
		return false
	}

	count := 0
	for _, events := range t.rangeEventCounts[0:idx] {
		count += events.events
	}
	shouldClean := count >= minEvents

	if !shouldClean {
		// To reduce engine.CleanByTable calls.
		t.rangeEventCounts[idx-1].events = count
		t.rangeEventCounts = t.rangeEventCounts[idx-1:]
	} else {
		t.rangeEventCounts = t.rangeEventCounts[idx:]
	}
	return shouldClean
}

// convertRowChangedEvents uses to convert RowChangedEvents to TableSinkRowChangedEvents.
// It will deal with the old value compatibility.
func convertRowChangedEvents(
	changefeed model.ChangeFeedID, span tablepb.Span, enableOldValue bool,
	events ...*model.PolymorphicEvent,
) ([]*model.RowChangedEvent, uint64, error) {
	size := 0
	rowChangedEvents := make([]*model.RowChangedEvent, 0, len(events))
	for _, e := range events {
		if e == nil || e.Row == nil {
			log.Warn("skip emit nil event",
				zap.String("namespace", changefeed.Namespace),
				zap.String("changefeed", changefeed.ID),
				zap.Stringer("span", &span),
				zap.Any("event", e))
			continue
		}

		colLen := len(e.Row.Columns)
		preColLen := len(e.Row.PreColumns)
		// Some transactions could generate empty row change event, such as
		// begin; insert into t (id) values (1); delete from t where id=1; commit;
		// Just ignore these row changed events.
		if colLen == 0 && preColLen == 0 {
			log.Warn("skip emit empty row event",
				zap.Stringer("span", &span),
				zap.String("namespace", changefeed.Namespace),
				zap.String("changefeed", changefeed.ID),
				zap.Any("event", e))
			continue
		}

		size += e.Row.ApproximateBytes()

		// This indicates that it is an update event,
		// and after enable old value internally by default(but disable in the configuration).
		// We need to handle the update event to be compatible with the old format.
		if e.Row.IsUpdate() && !enableOldValue {
			if shouldSplitUpdateEvent(e) {
				deleteEvent, insertEvent, err := splitUpdateEvent(e)
				if err != nil {
					return nil, 0, errors.Trace(err)
				}
				// NOTICE: Please do not change the order, the delete event always comes before the insert event.
				rowChangedEvents = append(rowChangedEvents, deleteEvent.Row, insertEvent.Row)
			} else {
				// If the handle key columns are not updated, PreColumns is directly ignored.
				e.Row.PreColumns = nil
				rowChangedEvents = append(rowChangedEvents, e.Row)
			}
		} else {
			rowChangedEvents = append(rowChangedEvents, e.Row)
		}
	}
	return rowChangedEvents, uint64(size), nil
}

// shouldSplitUpdateEvent determines if the split event is needed to align the old format based on
// whether the handle key column has been modified.
// If the handle key column is modified,
// we need to use splitUpdateEvent to split the update event into a delete and an insert event.
func shouldSplitUpdateEvent(updateEvent *model.PolymorphicEvent) bool {
	// nil event will never be split.
	if updateEvent == nil {
		return false
	}

	for i := range updateEvent.Row.Columns {
		col := updateEvent.Row.Columns[i]
		preCol := updateEvent.Row.PreColumns[i]
		if col != nil && col.Flag.IsHandleKey() && preCol != nil && preCol.Flag.IsHandleKey() {
			colValueString := model.ColumnValueString(col.Value)
			preColValueString := model.ColumnValueString(preCol.Value)
			// If one handle key columns is updated, we need to split the event row.
			if colValueString != preColValueString {
				return true
			}
		}
	}
	return false
}

// splitUpdateEvent splits an update event into a delete and an insert event.
func splitUpdateEvent(
	updateEvent *model.PolymorphicEvent,
) (*model.PolymorphicEvent, *model.PolymorphicEvent, error) {
	if updateEvent == nil {
		return nil, nil, errors.New("nil event cannot be split")
	}

	// If there is an update to handle key columns,
	// we need to split the event into two events to be compatible with the old format.
	// NOTICE: Here we don't need a full deep copy because
	// our two events need Columns and PreColumns respectively,
	// so it won't have an impact and no more full deep copy wastes memory.
	deleteEvent := *updateEvent
	deleteEventRow := *updateEvent.Row
	deleteEventRowKV := *updateEvent.RawKV
	deleteEvent.Row = &deleteEventRow
	deleteEvent.RawKV = &deleteEventRowKV

	deleteEvent.Row.Columns = nil

	insertEvent := *updateEvent
	insertEventRow := *updateEvent.Row
	insertEventRowKV := *updateEvent.RawKV
	insertEvent.Row = &insertEventRow
	insertEvent.RawKV = &insertEventRowKV
	// NOTICE: clean up pre cols for insert event.
	insertEvent.Row.PreColumns = nil

	return &deleteEvent, &insertEvent, nil
}

func genReplicateTs(ctx context.Context, pdClient pd.Client) (model.Ts, error) {
	backoffBaseDelayInMs := int64(100)
	totalRetryDuration := 10 * time.Second
	var replicateTs model.Ts
	err := retry.Do(ctx, func() error {
		phy, logic, err := pdClient.GetTS(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		replicateTs = oracle.ComposeTS(phy, logic)
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithTotalRetryDuratoin(totalRetryDuration),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	if err != nil {
		return model.Ts(0), errors.Trace(err)
	}
	return replicateTs, nil
}
