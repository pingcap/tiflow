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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const maxFlushBatchSize = 512

type bufferSink struct {
	Sink
	changeFeedCheckpointTs uint64
	tableCheckpointTsMap   sync.Map
	buffer                 map[model.TableID][]*model.RowChangedEvent
	bufferMu               sync.Mutex
	flushTsChan            chan flushMsg
	drawbackChan           chan drawbackMsg
}

func newBufferSink(
	backendSink Sink, checkpointTs model.Ts, drawbackChan chan drawbackMsg,
) *bufferSink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:                 make(map[model.TableID][]*model.RowChangedEvent),
		changeFeedCheckpointTs: checkpointTs,
		flushTsChan:            make(chan flushMsg, maxFlushBatchSize),
		drawbackChan:           drawbackChan,
	}
	return sink
}

type runState struct {
	batch [maxFlushBatchSize]flushMsg

	metricFlushDuration   prometheus.Observer
	metricEmitRowDuration prometheus.Observer
	metricTotalRows       prometheus.Counter
}

func (b *bufferSink) run(ctx context.Context, errCh chan error) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	advertiseAddr := util.CaptureAddrFromCtx(ctx)
	state := runState{
		metricFlushDuration:   flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "Flush"),
		metricEmitRowDuration: flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "EmitRow"),
		metricTotalRows:       bufferSinkTotalRowsCountCounter.WithLabelValues(advertiseAddr, changefeedID),
	}
	defer func() {
		flushRowChangedDuration.DeleteLabelValues(advertiseAddr, changefeedID, "Flush")
		flushRowChangedDuration.DeleteLabelValues(advertiseAddr, changefeedID, "EmitRow")
		bufferSinkTotalRowsCountCounter.DeleteLabelValues(advertiseAddr, changefeedID)
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
	case drawback := <-b.drawbackChan:
		b.bufferMu.Lock()
		delete(b.buffer, drawback.tableID)
		b.bufferMu.Unlock()
		close(drawback.callback)
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

	b.bufferMu.Lock()
	startEmit := time.Now()
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
	state.metricEmitRowDuration.Observe(time.Since(startEmit).Seconds())

	startFlush := time.Now()
	for i := 0; i < batchSize; i++ {
		tableID, resolvedTs := batch[i].tableID, batch[i].resolvedTs
		checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
		if err != nil {
			return false, errors.Trace(err)
		}
		b.tableCheckpointTsMap.Store(tableID, checkpointTs)
	}
	now := time.Now()
	state.metricFlushDuration.Observe(now.Sub(startFlush).Seconds())
	if now.Sub(startEmit) > time.Second {
		log.Warn("flush row changed events too slow",
			zap.Int("batchSize", batchSize),
			zap.Duration("duration", now.Sub(startEmit)),
			util.ZapFieldChangefeed(ctx))
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
