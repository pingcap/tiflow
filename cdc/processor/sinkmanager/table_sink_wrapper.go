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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// tableSinkWrapper is a wrapper of TableSink, it is used in SinkManager to manage TableSink.
// Because in the SinkManager, we write data to TableSink and RedoManager concurrently,
// so current sink node can not be reused.
type tableSinkWrapper struct {
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
	// receivedSorterResolvedTs is the resolved ts received from the sorter.
	// We use this to advance the redo log.
	receivedSorterResolvedTs atomic.Uint64
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
		changefeed: changefeed,
		tableID:    tableID,
		tableSink:  tableSink,
		state:      &state,
		startTs:    startTs,
		targetTs:   targetTs,
	}
}

func (t *tableSinkWrapper) start() {
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

func (t *tableSinkWrapper) updateResolvedTs(ts model.ResolvedTs) error {
	if err := t.tableSink.UpdateResolvedTs(ts); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *tableSinkWrapper) getCheckpointTs() model.ResolvedTs {
	return t.tableSink.GetCheckpointTs()
}

func (t *tableSinkWrapper) getReceivedSorterResolvedTs() model.Ts {
	return t.receivedSorterResolvedTs.Load()
}

func (t *tableSinkWrapper) getState() tablepb.TableState {
	return t.state.Load()
}

func (t *tableSinkWrapper) close(ctx context.Context) error {
	t.state.Store(tablepb.TableStateStopping)
	// table stopped state must be set after underlying sink is closed
	defer t.state.Store(tablepb.TableStateStopped)
	err := t.tableSink.Close(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("sink is closed",
		zap.Int64("tableID", t.tableID),
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID))
	return nil
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
