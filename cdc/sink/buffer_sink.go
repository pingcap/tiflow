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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const maxFlushBatchSize = 512

// bufferSink buffers emitted events and checkpoints and flush asynchronously.
// Note that it is a thread-safe Sink implementation.
type bufferSink struct {
	Sink
	changeFeedCheckpointTs uint64
	tableCheckpointTsMap   sync.Map
	buffer                 map[model.TableID][]*model.RowChangedEvent
	bufferMu               sync.Mutex
	flushTsChan            chan flushMsg
}

var _ Sink = (*bufferSink)(nil)

func newBufferSink(
	backendSink Sink, checkpointTs model.Ts,
) *bufferSink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:                 make(map[model.TableID][]*model.RowChangedEvent),
		changeFeedCheckpointTs: checkpointTs,
		flushTsChan:            make(chan flushMsg, maxFlushBatchSize),
	}
	return sink
}

type runState struct {
	batch [maxFlushBatchSize]flushMsg

	metricTotalRows prometheus.Counter
}

func (b *bufferSink) run(ctx context.Context, changefeedID string, errCh chan error) {
	state := runState{
		metricTotalRows: metrics.BufferSinkTotalRowsCountCounter.WithLabelValues(changefeedID),
	}
	defer func() {
		metrics.BufferSinkTotalRowsCountCounter.DeleteLabelValues(changefeedID)
	}()

	for {
		keepRun, err := b.runOnce(ctx, &state)
		if err != nil && errors.Cause(err) != context.Canceled {
			errCh <- err
			return
		}
		if !keepRun {
			return
		}
	}
}

func (b *bufferSink) runOnce(ctx context.Context, state *runState) (bool, error) {
	batchSize, batch := 0, state.batch
	push := func(event flushMsg) {
		batch[batchSize] = event
		batchSize++
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case event := <-b.flushTsChan:
		push(event)
	RecvBatch:
		for batchSize < maxFlushBatchSize {
			select {
			case event := <-b.flushTsChan:
				push(event)
			default:
				break RecvBatch
			}
		}
	}

	start := time.Now()
	b.bufferMu.Lock()
	// find all rows before resolvedTs and emit to backend sink
	for i := 0; i < batchSize; i++ {
		tableID, resolvedTs := batch[i].tableID, batch[i].resolvedTs
		rows := b.buffer[tableID]
		i := sort.Search(len(rows), func(i int) bool {
			return rows[i].CommitTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		state.metricTotalRows.Add(float64(i))

		err := b.Sink.EmitRowChangedEvents(ctx, rows[:i]...)
		if err != nil {
			b.bufferMu.Unlock()
			return false, errors.Trace(err)
		}

		// put remaining rows back to buffer
		// append to a new, fixed slice to avoid lazy GC
		b.buffer[tableID] = append(make([]*model.RowChangedEvent, 0, len(rows[i:])), rows[i:]...)
	}
	b.bufferMu.Unlock()

	for i := 0; i < batchSize; i++ {
		tableID, resolvedTs := batch[i].tableID, batch[i].resolvedTs
		checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
		if err != nil {
			return false, errors.Trace(err)
		}
		b.tableCheckpointTsMap.Store(tableID, checkpointTs)
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		log.Warn("flush row changed events too slow",
			zap.Int("batchSize", batchSize),
			zap.Duration("duration", elapsed),
			util.ZapFieldChangefeed(ctx))
	}

	return true, nil
}

// Init table sink resources
func (b *bufferSink) Init(tableID model.TableID) error {
	b.clearBufferedTableData(tableID)
	return b.Sink.Init(tableID)
}

// Barrier delete buffer
func (b *bufferSink) Barrier(ctx context.Context, tableID model.TableID) error {
	b.clearBufferedTableData(tableID)
	return b.Sink.Barrier(ctx, tableID)
}

func (b *bufferSink) clearBufferedTableData(tableID model.TableID) {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	delete(b.buffer, tableID)
}

func (b *bufferSink) TryEmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) (bool, error) {
	err := b.EmitRowChangedEvents(ctx, rows...)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *bufferSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if len(rows) == 0 {
			return nil
		}
		tableID := rows[0].Table.TableID
		b.bufferMu.Lock()
		b.buffer[tableID] = append(b.buffer[tableID], rows...)
		b.bufferMu.Unlock()
	}
	return nil
}

func (b *bufferSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return b.getTableCheckpointTs(tableID), ctx.Err()
	case b.flushTsChan <- flushMsg{
		tableID:    tableID,
		resolvedTs: resolvedTs,
	}:
	}
	return b.getTableCheckpointTs(tableID), nil
}

type flushMsg struct {
	tableID    model.TableID
	resolvedTs uint64
}

func (b *bufferSink) getTableCheckpointTs(tableID model.TableID) uint64 {
	checkPoints, ok := b.tableCheckpointTsMap.Load(tableID)
	if ok {
		return checkPoints.(uint64)
	}
	return atomic.LoadUint64(&b.changeFeedCheckpointTs)
}

// UpdateChangeFeedCheckpointTs update the changeFeedCheckpointTs every processor tick
func (b *bufferSink) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&b.changeFeedCheckpointTs, checkpointTs)
}
