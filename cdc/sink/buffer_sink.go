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

// bufferSink buffers emitted events and checkpoints and flush asynchronously.
// Note that it is a thread-safe Sink implementation.
type bufferSink struct {
	Sink
	checkpointTs uint64
	buffer       map[model.TableID][]*model.RowChangedEvent
	bufferMu     sync.Mutex
	flushTsChan  chan uint64
	drawbackChan chan drawbackMsg
}

var _ Sink = (*bufferSink)(nil)

func newBufferSink(
<<<<<<< HEAD
	ctx context.Context,
	backendSink Sink,
	errCh chan error,
	checkpointTs model.Ts,
	drawbackChan chan drawbackMsg,
) Sink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:       make(map[model.TableID][]*model.RowChangedEvent),
		checkpointTs: checkpointTs,
		flushTsChan:  make(chan uint64, 128),
		drawbackChan: drawbackChan,
=======
	backendSink Sink, checkpointTs model.Ts, drawbackChan chan drawbackMsg,
) *bufferSink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:                 make(map[model.TableID][]*model.RowChangedEvent),
		changeFeedCheckpointTs: checkpointTs,
		flushTsChan:            make(chan flushMsg, maxFlushBatchSize),
		drawbackChan:           drawbackChan,
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
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
<<<<<<< HEAD
		case drawback := <-b.drawbackChan:
			b.bufferMu.Lock()
			delete(b.buffer, drawback.tableID)
			b.bufferMu.Unlock()
			close(drawback.callback)
		case resolvedTs := <-b.flushTsChan:
			b.bufferMu.Lock()
			// find all rows before resolvedTs and emit to backend sink
			for tableID, rows := range b.buffer {
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
=======
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
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
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

<<<<<<< HEAD
			start := time.Now()
			checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, resolvedTs)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			atomic.StoreUint64(&b.checkpointTs, checkpointTs)
=======
		// put remaining rows back to buffer
		// append to a new, fixed slice to avoid lazy GC
		b.buffer[tableID] = append(make([]*model.RowChangedEvent, 0, len(rows[i:])), rows[i:]...)
	}
	b.bufferMu.Unlock()
	state.metricEmitRowDuration.Observe(time.Since(startEmit).Seconds())
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))

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

func (b *bufferSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return atomic.LoadUint64(&b.checkpointTs), ctx.Err()
	case b.flushTsChan <- resolvedTs:
	}
	return atomic.LoadUint64(&b.checkpointTs), nil
}
