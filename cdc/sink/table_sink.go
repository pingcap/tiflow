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
	"github.com/pingcap/tiflow/pkg/container/queue"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type tableSink struct {
	tableID     model.TableID
	backendSink Sink
	buffer      *queue.ChunkQueue[*model.RowChangedEvent]

	metricsTableSinkTotalRows prometheus.Counter
}

var _ Sink = (*tableSink)(nil)

// NewTableSink creates a new table sink
func NewTableSink(
	s Sink, tableID model.TableID,
	totalRowsCounter prometheus.Counter,
) (*tableSink, error) {
	sink := &tableSink{
		tableID:                   tableID,
		backendSink:               s,
		metricsTableSinkTotalRows: totalRowsCounter,
	}
	sink.buffer = queue.NewChunkQueue[*model.RowChangedEvent]()

	if err := sink.AddTable(tableID); err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

func (t *tableSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	t.buffer.PushMany(rows...)
	t.metricsTableSinkTotalRows.Add(float64(len(rows)))
	return nil
}

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

// FlushRowChangedEvents flushes sorted rows to sink manager, note the resolvedTs
// is required to be no more than global resolvedTs, table barrierTs and table
// redo log watermarkTs.
func (t *tableSink) FlushRowChangedEvents(
	ctx context.Context, tableID model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	resolvedTs := resolved.Ts
	if tableID != t.tableID {
		log.Panic("inconsistent table sink",
			zap.Int64("tableID", tableID), zap.Int64("sinkTableID", t.tableID))
	}
	i := sort.Search(t.buffer.Len(), func(i int) bool {
		event := t.buffer.Peek(i)
		return event.CommitTs > resolvedTs
	})
	if i == 0 {
		return t.backendSink.FlushRowChangedEvents(ctx, t.tableID, resolved)
	}
	resolvedRows, _ := t.buffer.PopMany(i)
	err := t.backendSink.EmitRowChangedEvents(ctx, resolvedRows...)
	if err != nil {
		return model.NewResolvedTs(0), errors.Trace(err)
	}
	return t.backendSink.FlushRowChangedEvents(ctx, t.tableID, resolved)
}

func (t *tableSink) EmitCheckpointTs(_ context.Context, _ uint64, _ []*model.TableInfo) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

func (t *tableSink) AddTable(tableID model.TableID) error {
	return t.backendSink.AddTable(tableID)
}

// Close once the method is called, no more events can be written to this table sink
func (t *tableSink) Close(ctx context.Context) error {
	return t.backendSink.RemoveTable(ctx, t.tableID)
}

func (t *tableSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	return nil
}
