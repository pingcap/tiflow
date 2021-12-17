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
<<<<<<< HEAD
	"math"
	"sort"
=======
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

<<<<<<< HEAD
const (
	defaultMetricInterval = time.Second * 15
)

// Manager manages table sinks, maintains the relationship between table sinks and backendSink
type Manager struct {
	backendSink  Sink
	checkpointTs model.Ts
	tableSinks   map[model.TableID]*tableSink
	tableSinksMu sync.Mutex
=======
// Manager manages table sinks, maintains the relationship between table sinks
// and backendSink.
// Manager is thread-safe.
type Manager struct {
	bufSink                *bufferSink
	tableCheckpointTsMap   sync.Map
	tableSinks             map[model.TableID]*tableSink
	tableSinksMu           sync.Mutex
	changeFeedCheckpointTs uint64
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))

	drawbackChan chan drawbackMsg

	captureAddr               string
	changefeedID              model.ChangeFeedID
	metricsTableSinkTotalRows prometheus.Counter
}

// NewManager creates a new Sink manager
func NewManager(
	ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts,
	captureAddr string, changefeedID model.ChangeFeedID,
) *Manager {
	drawbackChan := make(chan drawbackMsg, 16)
	bufSink := newBufferSink(backendSink, checkpointTs, drawbackChan)
	go bufSink.run(ctx, errCh)
	return &Manager{
<<<<<<< HEAD
		backendSink:               newBufferSink(ctx, backendSink, errCh, checkpointTs, drawbackChan),
		checkpointTs:              checkpointTs,
=======
		bufSink:                   bufSink,
		changeFeedCheckpointTs:    checkpointTs,
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
		tableSinks:                make(map[model.TableID]*tableSink),
		drawbackChan:              drawbackChan,
		captureAddr:               captureAddr,
		changefeedID:              changefeedID,
		metricsTableSinkTotalRows: tableSinkTotalRowsCountCounter.WithLabelValues(captureAddr, changefeedID),
	}
}

// CreateTableSink creates a table sink
func (m *Manager) CreateTableSink(tableID model.TableID, checkpointTs model.Ts) Sink {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	sink := &tableSink{
<<<<<<< HEAD
		tableID:   tableID,
		manager:   m,
		buffer:    make([]*model.RowChangedEvent, 0, 128),
		emittedTs: checkpointTs,
=======
		tableID:     tableID,
		manager:     m,
		buffer:      make([]*model.RowChangedEvent, 0, 128),
		redoManager: redoManager,
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
	}
	m.tableSinks[tableID] = sink
	return sink
}

// Close closes the Sink manager and backend Sink, this method can be reentrantly called
func (m *Manager) Close(ctx context.Context) error {
<<<<<<< HEAD
	tableSinkTotalRowsCountCounter.DeleteLabelValues(m.captureAddr, m.changefeedID)
	return m.backendSink.Close(ctx)
}

func (m *Manager) getMinEmittedTs() model.Ts {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if len(m.tableSinks) == 0 {
		return m.getCheckpointTs()
	}
	minTs := model.Ts(math.MaxUint64)
	for _, tableSink := range m.tableSinks {
		emittedTs := tableSink.getEmittedTs()
		if minTs > emittedTs {
			minTs = emittedTs
		}
=======
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	tableSinkTotalRowsCountCounter.DeleteLabelValues(m.captureAddr, m.changefeedID)
	if m.bufSink != nil {
		return m.bufSink.Close(ctx)
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
	}
	return nil
}

<<<<<<< HEAD
func (m *Manager) flushBackendSink(ctx context.Context) (model.Ts, error) {
	// NOTICE: Because all table sinks will try to flush backend sink,
	// which will cause a lot of lock contention and blocking in high concurrency cases.
	// So here we use flushing as a lightweight lock to improve the lock competition problem.
	//
	// Do not skip flushing for resolving #3503.
	// TODO uncomment the following return.
	// if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
	// return m.getCheckpointTs(), nil
	// }
	m.flushMu.Lock()
	defer func() {
		m.flushMu.Unlock()
		atomic.StoreInt64(&m.flushing, 0)
	}()
	minEmittedTs := m.getMinEmittedTs()
	checkpointTs, err := m.backendSink.FlushRowChangedEvents(ctx, minEmittedTs)
=======
func (m *Manager) flushBackendSink(ctx context.Context, tableID model.TableID, resolvedTs uint64) (model.Ts, error) {
	checkpointTs, err := m.bufSink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
	if err != nil {
		return m.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&m.checkpointTs, checkpointTs)
	return checkpointTs, nil
}

func (m *Manager) destroyTableSink(ctx context.Context, tableID model.TableID) error {
	m.tableSinksMu.Lock()
	delete(m.tableSinks, tableID)
	m.tableSinksMu.Unlock()
	callback := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.drawbackChan <- drawbackMsg{tableID: tableID, callback: callback}:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-callback:
	}
<<<<<<< HEAD
	return m.backendSink.Barrier(ctx)
=======
	return m.bufSink.Barrier(ctx, tableID)
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
}

func (m *Manager) getCheckpointTs() uint64 {
	return atomic.LoadUint64(&m.checkpointTs)
}

type tableSink struct {
	tableID model.TableID
	manager *Manager
	buffer  []*model.RowChangedEvent
	// emittedTs means all of events which of commitTs less than or equal to emittedTs is sent to backendSink
	emittedTs model.Ts
}

func (t *tableSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// do nothing
	return nil
}

func (t *tableSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	t.buffer = append(t.buffer, rows...)
	t.manager.metricsTableSinkTotalRows.Add(float64(len(rows)))
	return nil
}

<<<<<<< HEAD
func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

func (t *tableSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	// Log abnormal checkpoint that is large than resolved ts.
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
		ckpt, err := t.manager.flushBackendSink(ctx)
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
	ckpt, err := t.manager.flushBackendSink(ctx)
	if err != nil {
		return ckpt, err
=======
func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&m.changeFeedCheckpointTs, checkpointTs)
	if m.bufSink != nil {
		m.bufSink.UpdateChangeFeedCheckpointTs(checkpointTs)
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
	}
	logAbnormalCheckpoint(ckpt)
	return ckpt, err
}

func (t *tableSink) getEmittedTs() uint64 {
	return atomic.LoadUint64(&t.emittedTs)
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

type drawbackMsg struct {
	tableID  model.TableID
	callback chan struct{}
}

type bufferSink struct {
	Sink
	checkpointTs uint64
	buffer       map[model.TableID][]*model.RowChangedEvent
	bufferMu     sync.Mutex
	flushTsChan  chan uint64
	drawbackChan chan drawbackMsg
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
		buffer:       make(map[model.TableID][]*model.RowChangedEvent),
		checkpointTs: checkpointTs,
		flushTsChan:  make(chan uint64, 128),
		drawbackChan: drawbackChan,
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
			}
			b.bufferMu.Unlock()

			start := time.Now()
			checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, resolvedTs)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			atomic.StoreUint64(&b.checkpointTs, checkpointTs)

			dur := time.Since(start)
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

func (b *bufferSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return atomic.LoadUint64(&b.checkpointTs), ctx.Err()
	case b.flushTsChan <- resolvedTs:
	}
	return atomic.LoadUint64(&b.checkpointTs), nil
}
