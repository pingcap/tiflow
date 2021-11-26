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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

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
	ctx context.Context,
	backendSink Sink,
	errCh chan error,
	checkpointTs model.Ts,
	drawbackChan chan drawbackMsg,
) Sink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:                 make(map[model.TableID][]*model.RowChangedEvent),
		changeFeedCheckpointTs: checkpointTs,
		flushTsChan:            make(chan flushMsg, 128),
		drawbackChan:           drawbackChan,
	}
	go sink.run(ctx, errCh)
	return sink
}

func (b *bufferSink) run(ctx context.Context, errCh chan error) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	advertiseAddr := util.CaptureAddrFromCtx(ctx)
	metricFlushDuration := flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "Flush")
	metricEmitRowDuration := flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "EmitRow")
	metricBufferSize := bufferChanSizeGauge.WithLabelValues(advertiseAddr, changefeedID)
	metricTotalRows := bufferSinkTotalRowsCountCounter.WithLabelValues(advertiseAddr, changefeedID)
	defer func() {
		flushRowChangedDuration.DeleteLabelValues(advertiseAddr, changefeedID, "Flush")
		flushRowChangedDuration.DeleteLabelValues(advertiseAddr, changefeedID, "EmitRow")
		bufferChanSizeGauge.DeleteLabelValues(advertiseAddr, changefeedID)
		bufferSinkTotalRowsCountCounter.DeleteLabelValues(advertiseAddr, changefeedID)
	}()
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil && errors.Cause(err) != context.Canceled {
				errCh <- err
			}
			return
		case drawback := <-b.drawbackChan:
			b.bufferMu.Lock()
			delete(b.buffer, drawback.tableID)
			b.bufferMu.Unlock()
			close(drawback.callback)
		case flushEvent := <-b.flushTsChan:
			b.bufferMu.Lock()
			resolvedTs := flushEvent.resolvedTs
			tableID := flushEvent.tableID
			// find all rows before resolvedTs and emit to backend sink
			rows := b.buffer[tableID]
			i := sort.Search(len(rows), func(i int) bool {
				return rows[i].CommitTs > resolvedTs
			})
			metricTotalRows.Add(float64(i))

			start := time.Now()
			err := b.Sink.EmitRowChangedEvents(ctx, rows[:i]...)
			if err != nil {
				b.bufferMu.Unlock()
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			dur := time.Since(start)
			metricEmitRowDuration.Observe(dur.Seconds())

			// put remaining rows back to buffer
			// append to a new, fixed slice to avoid lazy GC
			b.buffer[tableID] = append(make([]*model.RowChangedEvent, 0, len(rows[i:])), rows[i:]...)
			b.bufferMu.Unlock()

			start = time.Now()
			checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			b.tableCheckpointTsMap.Store(tableID, checkpointTs)

			dur = time.Since(start)
			metricFlushDuration.Observe(dur.Seconds())
			if dur > 3*time.Second {
				log.Warn("flush row changed events too slow",
					zap.Duration("duration", dur), util.ZapFieldChangefeed(ctx))
			}
		case <-time.After(defaultMetricInterval):
			metricBufferSize.Set(float64(len(b.buffer)))
		}
	}
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
		return b.getCheckpointTs(tableID), ctx.Err()
	case b.flushTsChan <- flushMsg{
		tableID:    tableID,
		resolvedTs: resolvedTs,
	}:
	}
	return b.getCheckpointTs(tableID), nil
}

type flushMsg struct {
	tableID    model.TableID
	resolvedTs uint64
}

func (b *bufferSink) getCheckpointTs(tableID model.TableID) uint64 {
	checkPoints, ok := b.tableCheckpointTsMap.Load(tableID)
	if ok {
		return checkPoints.(uint64)
	}
	return b.changeFeedCheckpointTs
}
