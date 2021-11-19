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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	"go.uber.org/zap"
)

type tableSink struct {
	tableID model.TableID
	manager *Manager
	buffer  []*model.RowChangedEvent
	// emittedTs means all of events which of commitTs less than or equal to emittedTs is sent to backendSink
	emittedTs   model.Ts
	redoManager redo.LogManager
}

func (t *tableSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// do nothing
	return nil
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
func (t *tableSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	logAbnormalCheckpoint := func(ckpt uint64) {
		if ckpt > resolvedTs {
			log.L().WithOptions(zap.AddCallerSkip(1)).
				Warn("checkpoint ts > resolved ts, flushed more than emitted",
					zap.Int64("tableID", t.tableID),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("checkpointTs", ckpt))
		}
	}
	i := sort.Search(len(t.buffer), func(i int) bool {
		return t.buffer[i].CommitTs > resolvedTs
	})
	if i == 0 {
		atomic.StoreUint64(&t.emittedTs, resolvedTs)
		ckpt, err := t.flushRedoLogs(ctx, resolvedTs)
		if err != nil {
			return ckpt, err
		}
		ckpt, err = t.manager.flushBackendSink(ctx)
		if err != nil {
			return ckpt, err
		}
		logAbnormalCheckpoint(ckpt)
		return ckpt, err
	}
	resolvedRows := t.buffer[:i]
	t.buffer = append(make([]*model.RowChangedEvent, 0, len(t.buffer[i:])), t.buffer[i:]...)

	err := t.manager.backendSink.EmitRowChangedEvents(ctx, resolvedRows...)
	if err != nil {
		return t.manager.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&t.emittedTs, resolvedTs)
	ckpt, err := t.flushRedoLogs(ctx, resolvedTs)
	if err != nil {
		return ckpt, err
	}
	ckpt, err = t.manager.flushBackendSink(ctx)
	if err != nil {
		return ckpt, err
	}
	logAbnormalCheckpoint(ckpt)
	return ckpt, err
}

func (t *tableSink) flushRedoLogs(ctx context.Context, resolvedTs uint64) (uint64, error) {
	if t.redoManager.Enabled() {
		err := t.redoManager.FlushLog(ctx, t.tableID, resolvedTs)
		if err != nil {
			return t.manager.getCheckpointTs(), err
		}
	}
	return 0, nil
}

// getResolvedTs returns resolved ts, which means all events before resolved ts
// have been sent to sink manager, if redo log is enabled, it also means all events
// before resolved ts have been persisted to redo log storage.
func (t *tableSink) getResolvedTs() uint64 {
	ts := atomic.LoadUint64(&t.emittedTs)
	if t.redoManager.Enabled() {
		redoResolvedTs := t.redoManager.GetMinResolvedTs()
		if redoResolvedTs < ts {
			ts = redoResolvedTs
		}
	}
	return ts
}

func (t *tableSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

// Note once the Close is called, no more events can be written to this table sink
func (t *tableSink) Close(ctx context.Context) error {
	return t.manager.destroyTableSink(ctx, t.tableID)
}

// Barrier is not used in table sink
func (t *tableSink) Barrier(ctx context.Context) error {
	return nil
}
