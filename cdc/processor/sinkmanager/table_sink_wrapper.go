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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type tableSinkWrapper struct {
	// changefeed used for logging.
	changefeed model.ChangeFeedID
	// tableID used for logging.
	tableID model.TableID
	// tableSink is the underlying sink.
	tableSink sinkv2.TableSink
	// state used to control the lifecycle of the table.
	state *tablepb.TableState
	// targetTs is the upper bound of the table sink.
	targetTs model.Ts
	// receivedSorterResolvedTs is the resolved ts received from the sorter.
	// We use this to advance the redo log.
	receivedSorterResolvedTs atomic.Uint64
}

//nolint:deadcode
func newTableSinkWrapper(
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableSink sinkv2.TableSink,
	state *tablepb.TableState,
	targetTs model.Ts,
) *tableSinkWrapper {
	return &tableSinkWrapper{
		changefeed: changefeed,
		tableID:    tableID,
		tableSink:  tableSink,
		state:      state,
		targetTs:   targetTs,
	}
}

func (t *tableSinkWrapper) appendRowChangedEvents(events ...*model.RowChangedEvent) {
	t.tableSink.AppendRowChangedEvents(events...)
}

func (t *tableSinkWrapper) updateReceivedSorterResolvedTs(ts model.Ts) {
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
	log.Info("sinkV2 is closed",
		zap.Int64("tableID", t.tableID),
		zap.String("namespace", t.changefeed.Namespace),
		zap.String("changefeed", t.changefeed.ID))
	return cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
}
