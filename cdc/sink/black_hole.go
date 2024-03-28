// Copyright 2020 PingCAP, Inc.
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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"go.uber.org/zap"
)

// newBlackHoleSink creates a black hole sink
func newBlackHoleSink(ctx context.Context) *blackHoleSink {
	return &blackHoleSink{
		// use `SinkTypeDB` to record metrics
		statistics: metrics.NewStatistics(ctx, "", metrics.SinkTypeDB),
	}
}

type blackHoleSink struct {
	statistics      *metrics.Statistics
	accumulated     uint64
	lastAccumulated uint64
	flushing        uint64 // 1 means flushing, 0 means not flushing
}

var _ Sink = (*blackHoleSink)(nil)

func (b *blackHoleSink) AddTable(tableID model.TableID) error {
	return nil
}

func (b *blackHoleSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		// NOTE: don't change the log, some tests depend on it.
		log.Debug("BlackHoleSink: WriteEvents", zap.Any("row", row))
	}
	rowsCount := len(rows)
	atomic.AddUint64(&b.accumulated, uint64(rowsCount))
	b.statistics.AddRowsCount(rowsCount)
	return nil
}

func (b *blackHoleSink) FlushRowChangedEvents(
	ctx context.Context, _ model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	// In black hole sink use a atomic lock to prevent
	// concurrent flush to avoid statistics error.
	if !atomic.CompareAndSwapUint64(&b.flushing, 0, 1) {
		return model.ResolvedTs{
			Mode:    resolved.Mode,
			Ts:      0,
			BatchID: resolved.BatchID,
		}, nil
	}
	log.Debug("BlockHoleSink: FlushRowChangedEvents", zap.Uint64("resolvedTs", resolved.Ts))
	err := b.statistics.RecordBatchExecution(func() (int, int64, error) {
		// TODO: add some random replication latency
		accumulated := atomic.LoadUint64(&b.accumulated)
		lastAccumulated := atomic.SwapUint64(&b.lastAccumulated, accumulated)
		batchSize := accumulated - lastAccumulated
		return int(batchSize), int64(batchSize), nil
	})
	b.statistics.PrintStatus(ctx)
	atomic.StoreUint64(&b.flushing, 0)
	return resolved, err
}

func (b *blackHoleSink) EmitCheckpointTs(ctx context.Context, ts uint64, tables []*model.TableInfo) error {
	log.Debug("BlockHoleSink: Checkpoint Event", zap.Uint64("ts", ts), zap.Any("tables", tables))
	return nil
}

func (b *blackHoleSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Debug("BlockHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

func (b *blackHoleSink) Close(ctx context.Context) error {
	return nil
}

func (b *blackHoleSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	return nil
}
