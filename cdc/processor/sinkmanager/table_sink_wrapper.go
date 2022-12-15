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
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	"github.com/tikv/client-go/v2/oracle"
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
	// tableID used for logging.
	tableID model.TableID
	// tableSink is the underlying sink.
	tableSink sinkv2.TableSink
	// state used to control the lifecycle of the table.
	state *tablepb.TableState
	// startTs is the start ts of the table.
	startTs model.Ts
	// targetTs is the upper bound of the table sink.
	targetTs model.Ts
	// replicateTs is the ts that the table sink has started to replicate.
	replicateTs model.Ts
	// receivedSorterResolvedTs is the resolved ts received from the sorter.
	// We use this to advance the redo log.
	receivedSorterResolvedTs atomic.Uint64
	// receivedSorterCommitTs is the commit ts received from the sorter.
	// We use this to statistics the latency of the table sorter.
	receivedSorterCommitTs atomic.Uint64
	// receivedEventCount is the number of events received from the sorter.
	receivedEventCount atomic.Int64
	// lastCleanTime indicates the last time the table has been cleaned.
	lastCleanTime time.Time
	// checkpointTs is the checkpoint ts of the table sink.
	checkpointTs atomic.Uint64

	// rangeEventCounts is for clean the table engine.
	// If rangeEventCounts[i].events is greater than 0, it means there must be
	// events in the range (rangeEventCounts[i-1].pos, rangeEventCounts[i].pos].
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
	tableID model.TableID,
	tableSink sinkv2.TableSink,
	state tablepb.TableState,
	startTs model.Ts,
	targetTs model.Ts,
) *tableSinkWrapper {
	return &tableSinkWrapper{
		version:    atomic.AddUint64(&version, 1),
		changefeed: changefeed,
		tableID:    tableID,
		tableSink:  tableSink,
		state:      &state,
		startTs:    startTs,
		targetTs:   targetTs,
	}
}

func (t *tableSinkWrapper) start(startTs model.Ts, replicateTs model.Ts) {
	if t.replicateTs != 0 {
		log.Panic("The table sink has already started",
			zap.String("namespace", t.changefeed.Namespace),
			zap.String("changefeed", t.changefeed.ID),
			zap.Int64("tableID", t.tableID),
			zap.Uint64("startTs", startTs),
			zap.Uint64("replicateTs", replicateTs),
			zap.Uint64("oldReplicateTs", t.replicateTs),
		)
	}
	log.Info("Sink is started",
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("replicateTs", replicateTs),
	)
	// This start ts maybe greater than the initial start ts of the table sink.
	// Because in two phase scheduling, the table sink may be advanced to a later ts.
	// And we can just continue to replicate the table sink from the new start ts.
	t.checkpointTs.Store(startTs)
	t.replicateTs = replicateTs
	t.state.Store(tablepb.TableStateReplicating)
}

func (t *tableSinkWrapper) appendRowChangedEvents(events ...*model.RowChangedEvent) {
	t.tableSink.AppendRowChangedEvents(events...)
}

func (t *tableSinkWrapper) updateReceivedSorterResolvedTs(ts model.Ts) {
	if t.state.Load() == tablepb.TableStatePreparing && ts > t.startTs {
		t.state.Store(tablepb.TableStatePrepared)
	}
	t.receivedSorterResolvedTs.Store(ts)
}

func (t *tableSinkWrapper) updateReceivedSorterCommitTs(ts model.Ts) {
	if ts > t.receivedSorterCommitTs.Load() {
		t.receivedSorterCommitTs.Store(ts)
	}
}

func (t *tableSinkWrapper) updateResolvedTs(ts model.ResolvedTs) error {
	if err := t.tableSink.UpdateResolvedTs(ts); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *tableSinkWrapper) getCheckpointTs() model.ResolvedTs {
	currentCheckpointTs := t.checkpointTs.Load()
	newCheckpointTs := t.tableSink.GetCheckpointTs()
	if currentCheckpointTs > newCheckpointTs.ResolvedMark() {
		return model.NewResolvedTs(currentCheckpointTs)
	}
	return newCheckpointTs
}

func (t *tableSinkWrapper) getReceivedSorterResolvedTs() model.Ts {
	return t.receivedSorterResolvedTs.Load()
}

func (t *tableSinkWrapper) getReceivedSorterCommitTs() model.Ts {
	return t.receivedSorterCommitTs.Load()
}

func (t *tableSinkWrapper) getReceivedEventCount() int64 {
	return t.receivedEventCount.Load()
}

func (t *tableSinkWrapper) getState() tablepb.TableState {
	return t.state.Load()
}

func (t *tableSinkWrapper) close(ctx context.Context) {
	t.state.Store(tablepb.TableStateStopping)
	// table stopped state must be set after underlying sink is closed
	defer t.state.Store(tablepb.TableStateStopped)
	t.tableSink.Close(ctx)
	log.Info("Sink is closed",
		zap.Int64("tableID", t.tableID),
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID))
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
func convertRowChangedEvents(changefeed model.ChangeFeedID, tableID model.TableID, enableOldValue bool, events ...*model.PolymorphicEvent) ([]*model.RowChangedEvent, uint64, error) {
	size := 0
	rowChangedEvents := make([]*model.RowChangedEvent, 0, len(events))
	for _, e := range events {
		if e == nil || e.Row == nil {
			log.Warn("skip emit nil event",
				zap.String("namespace", changefeed.Namespace),
				zap.String("changefeed", changefeed.ID),
				zap.Int64("tableID", tableID),
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
				zap.Int64("tableID", tableID),
				zap.String("namespace", changefeed.Namespace),
				zap.String("changefeed", changefeed.ID),
				zap.Any("event", e))
			continue
		}

		size += e.Row.ApproximateBytes()

		// This indicates that it is an update event,
		// and after enable old value internally by default(but disable in the configuration).
		// We need to handle the update event to be compatible with the old format.
		if !enableOldValue && colLen != 0 && preColLen != 0 && colLen == preColLen {
			if pipeline.ShouldSplitUpdateEvent(e) {
				deleteEvent, insertEvent, err := pipeline.SplitUpdateEvent(e)
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
