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
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type tableSinkWrapper struct {
	changefeed              model.ChangeFeedID
	tableID                 model.TableID
	tableSink               sinkv2.TableSink
	state                   *tablepb.TableState
	targetTs                model.Ts
	currentSorterResolvedTs atomic.Uint64
	enableOldValue          bool
	splitTxn                bool
}

// newTableSinkWrapper creates a new table sink wrapper.
//
//nolint:deadcode
func newTableSinkWrapper(
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableSink sinkv2.TableSink,
	state *tablepb.TableState,
	targetTs model.Ts,
	enableOldValue bool,
	splitTxn bool,
) *tableSinkWrapper {
	return &tableSinkWrapper{
		changefeed:     changefeed,
		tableID:        tableID,
		tableSink:      tableSink,
		state:          state,
		targetTs:       targetTs,
		enableOldValue: enableOldValue,
		splitTxn:       splitTxn,
	}
}

func (t *tableSinkWrapper) emitRowChangedEvent(events ...*model.RowChangedEvent) {
	t.tableSink.AppendRowChangedEvents(events...)
}

func (t *tableSinkWrapper) updateCurrentSorterResolvedTs(resolvedTs model.Ts) {
	t.currentSorterResolvedTs.Store(resolvedTs)
}

func (t *tableSinkWrapper) updateTableSinkResolvedTs(resolvedTs model.ResolvedTs) (err error) {
	if resolvedTs.Ts > t.targetTs {
		resolvedTs = model.NewResolvedTs(t.targetTs)
	}
	return t.tableSink.UpdateResolvedTs(resolvedTs)
}

func (t *tableSinkWrapper) getTableSinkCheckpointTs() model.ResolvedTs {
	return t.tableSink.GetCheckpointTs()
}

func (t *tableSinkWrapper) getCurrentSorterResolvedTs() model.Ts {
	return t.currentSorterResolvedTs.Load()
}

func (t *tableSinkWrapper) getState() *tablepb.TableState {
	return t.state
}

func (t *tableSinkWrapper) close(ctx context.Context) error {
	t.state.Store(tablepb.TableStateStopping)
	// table stopped state must be set after underlying sink is closed
	defer t.state.Store(tablepb.TableStateStopped)
	err := t.tableSink.Close(ctx)
	if err != nil {
		return err
	}
	log.Info("sinkV2 is closed",
		zap.Int64("tableID", t.tableID),
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID))
	return cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
}

func (t *tableSinkWrapper) verifyAndTrySplitEvent(event *model.PolymorphicEvent) ([]*model.RowChangedEvent, error) {
	if err := t.verifySplitTxn(event); err != nil {
		return nil, err
	}
	if event == nil || event.Row == nil {
		log.Warn("skip emit nil event",
			zap.Int64("tableID", t.tableID),
			zap.String("namespace", t.changefeed.Namespace),
			zap.String("changefeed", t.changefeed.ID),
			zap.Any("event", event))
		return nil, nil
	}

	colLen := len(event.Row.Columns)
	preColLen := len(event.Row.PreColumns)
	// Some transactions could generate empty row change event, such as
	// begin; insert into t (id) values (1); delete from t where id=1; commit;
	// Just ignore these row changed events.
	if colLen == 0 && preColLen == 0 {
		log.Warn("skip emit empty row event",
			zap.Int64("tableID", t.tableID),
			zap.String("namespace", t.changefeed.Namespace),
			zap.String("changefeed", t.changefeed.ID),
			zap.Any("event", event))
		return nil, nil
	}

	// This indicates that it is an update event,
	// and after enable old value internally by default(but disable in the configuration).
	// We need to handle the update event to be compatible with the old format.
	if !t.enableOldValue && colLen != 0 && preColLen != 0 && colLen == preColLen {
		if pipeline.ShouldSplitUpdateEvent(event) {
			deleteEvent, insertEvent, err := pipeline.SplitUpdateEvent(event)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// NOTICE: Please do not change the order, the delete event always comes before the insert event.
			return []*model.RowChangedEvent{deleteEvent.Row, insertEvent.Row}, nil
		}
		// If the handle key columns are not updated, PreColumns is directly ignored.
		event.Row.PreColumns = nil
	}

	return []*model.RowChangedEvent{event.Row}, nil
}

// verifySplitTxn that TxnAtomicity compatibility with BatchResolved event and RowChangedEvent
// with `SplitTxn==true`.
func (t *tableSinkWrapper) verifySplitTxn(e *model.PolymorphicEvent) error {
	if t.splitTxn {
		return nil
	}

	// Fail-fast check, this situation should never happen normally when split transactions
	// are not supported.
	if e.Resolved != nil && e.Resolved.IsBatchMode() {
		msg := fmt.Sprintf("batch mode resolved ts is not supported "+
			"when sink.splitTxn is %+v", t.splitTxn)
		return cerror.ErrSinkInvalidConfig.GenWithStackByArgs(msg)
	}

	if e.Row != nil && e.Row.SplitTxn {
		msg := fmt.Sprintf("should not split txn when sink.splitTxn is %+v", t.splitTxn)
		return cerror.ErrSinkInvalidConfig.GenWithStackByArgs(msg)
	}
	return nil
}
