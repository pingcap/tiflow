// Copyright 2021 PingCAP, Inc.
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

package sink

import (
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"go.uber.org/zap"
)

type tableSink struct {
	tableID     model.TableID
	manager     *Manager
	buffer      []*model.RowChangedEvent
	redoManager redo.LogManager
}

var _ Sink = (*tableSink)(nil)

func (t *tableSink) TryEmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) (bool, error) {
	t.buffer = append(t.buffer, rows...)
	t.manager.metricsTableSinkTotalRows.Add(float64(len(rows)))
	if t.redoManager.Enabled() {
		return t.redoManager.TryEmitRowChangedEvents(ctx, t.tableID, rows...)
	}
	return true, nil
}

func (t *tableSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	t.buffer = append(t.buffer, rows...)
	t.manager.metricsTableSinkTotalRows.Add(float64(len(rows)))
	if t.redoManager.Enabled() {
		return t.redoManager.EmitRowChangedEvents(ctx, t.tableID, rows...)
	}
	return nil
}

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

// FlushRowChangedEvents flushes sorted rows to sink manager, note the resolvedTs
// is required to be no more than global resolvedTs, table barrierTs and table
// redo log watermarkTs.
func (t *tableSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	if tableID != t.tableID {
		log.Panic("inconsistent table sink",
			zap.Int64("tableID", tableID), zap.Int64("sinkTableID", t.tableID))
	}
	i := sort.Search(len(t.buffer), func(i int) bool {
		return t.buffer[i].CommitTs > resolvedTs
	})
	if i == 0 {
		return t.flushResolvedTs(ctx, resolvedTs)
	}
	resolvedRows := t.buffer[:i]
	t.buffer = append(make([]*model.RowChangedEvent, 0, len(t.buffer[i:])), t.buffer[i:]...)

	err := t.manager.bufSink.EmitRowChangedEvents(ctx, resolvedRows...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return t.flushResolvedTs(ctx, resolvedTs)
}

func (t *tableSink) flushResolvedTs(ctx context.Context, resolvedTs uint64) (uint64, error) {
	redoTs, err := t.flushRedoLogs(ctx, resolvedTs)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if redoTs < resolvedTs {
		resolvedTs = redoTs
	}

	checkpointTs, err := t.manager.bufSink.FlushRowChangedEvents(ctx, t.tableID, resolvedTs)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return checkpointTs, nil
}

// flushRedoLogs flush redo logs and returns redo log resolved ts which means
// all events before the ts have been persisted to redo log storage.
func (t *tableSink) flushRedoLogs(ctx context.Context, resolvedTs uint64) (uint64, error) {
	if t.redoManager.Enabled() {
		err := t.redoManager.FlushLog(ctx, t.tableID, resolvedTs)
		if err != nil {
			return 0, err
		}
		return t.redoManager.GetMinResolvedTs(), nil
	}
	return resolvedTs, nil
}

func (t *tableSink) EmitCheckpointTs(_ context.Context, _ uint64, _ []model.TableName) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

// Init table sink resources
func (t *tableSink) Init(tableID model.TableID) error {
	return t.manager.bufSink.Init(tableID)
}

// Close once the method is called, no more events can be written to this table sink
func (t *tableSink) Close(ctx context.Context) error {
	return t.manager.destroyTableSink(ctx, t.tableID)
}

// Barrier is not used in table sink
func (t *tableSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}
