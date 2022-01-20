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
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultMetricInterval = time.Second * 15
)

// Manager manages table sinks, maintains the relationship between table sinks and backendSink
type Manager struct {
	backendSink            *bufferSink
	tableCheckpointTsMap   sync.Map
	tableSinks             map[model.TableID]*tableSink
	tableSinksMu           sync.Mutex
	changeFeedCheckpointTs uint64

	flushMu  sync.Mutex
	flushing int64

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
	return &Manager{
		backendSink:               newBufferSink(ctx, backendSink, errCh, checkpointTs, drawbackChan),
		changeFeedCheckpointTs:    checkpointTs,
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
		tableID:   tableID,
		manager:   m,
		buffer:    make([]*model.RowChangedEvent, 0, 128),
		emittedTs: checkpointTs,
	}
	m.tableSinks[tableID] = sink
	return sink
}

// Close closes the Sink manager and backend Sink, this method can be reentrantly called
func (m *Manager) Close(ctx context.Context) error {
	tableSinkTotalRowsCountCounter.DeleteLabelValues(m.captureAddr, m.changefeedID)
	if m.backendSink != nil {
		log.Info("sinkManager try close bufSink",
			zap.String("changefeed", m.changefeedID))
		start := time.Now()
		if err := m.backendSink.Close(ctx); err != nil {
			log.Info("close bufSink failed",
				zap.String("changefeed", m.changefeedID),
				zap.Duration("duration", time.Since(start)))
			return err
		}
		log.Info("close bufSink success",
			zap.String("changefeed", m.changefeedID),
			zap.Duration("duration", time.Since(start)))
	}
	return nil
}

func (m *Manager) getMinEmittedTs(tableID model.TableID) model.Ts {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if len(m.tableSinks) == 0 {
		return m.getCheckpointTs(tableID)
	}
	minTs := model.Ts(math.MaxUint64)
	for _, tblSink := range m.tableSinks {
		resolvedTs := tblSink.getResolvedTs()
		if minTs > resolvedTs {
			minTs = resolvedTs
		}
	}
	return minTs
}

func (m *Manager) flushBackendSink(ctx context.Context, tableID model.TableID) (model.Ts, error) {
	// NOTICE: Because all table sinks will try to flush backend sink,
	// which will cause a lot of lock contention and blocking in high concurrency cases.
	// So here we use flushing as a lightweight lock to improve the lock competition problem.
	//
	// Do not skip flushing for resolving #3503.
	// TODO uncomment the following return.
	// if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
	// return m.getCheckpointTs(tableID), nil
	// }
	m.flushMu.Lock()
	defer func() {
		m.flushMu.Unlock()
		atomic.StoreInt64(&m.flushing, 0)
	}()
	minEmittedTs := m.getMinEmittedTs(tableID)
	checkpointTs, err := m.backendSink.FlushRowChangedEvents(ctx, tableID, minEmittedTs)
	if err != nil {
		return m.getCheckpointTs(tableID), errors.Trace(err)
	}
	m.tableCheckpointTsMap.Store(tableID, checkpointTs)
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
	return m.backendSink.Barrier(ctx, tableID)
}

func (m *Manager) getCheckpointTs(tableID model.TableID) uint64 {
	checkPoints, ok := m.tableCheckpointTsMap.Load(tableID)
	if ok {
		return checkPoints.(uint64)
	}
	// cannot find table level checkpointTs because of no table level resolvedTs flush task finished successfully,
	// for example: first time to flush resolvedTs but cannot get the flush lock, return changefeed level checkpointTs is safe
	return atomic.LoadUint64(&m.changeFeedCheckpointTs)
}

// UpdateChangeFeedCheckpointTs updates changefeed and backend sink checkpoint ts.
func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&m.changeFeedCheckpointTs, checkpointTs)
	if m.backendSink != nil {
		m.backendSink.UpdateChangeFeedCheckpointTs(checkpointTs)
	}
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

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

// FlushRowChangedEvents flushes sorted rows to sink manager, note the resolvedTs
// is required to be no more than global resolvedTs, table barrierTs and table
// redo log watermarkTs.
func (t *tableSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
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
		ckpt, err := t.manager.flushBackendSink(ctx, tableID)
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
		return t.manager.getCheckpointTs(tableID), errors.Trace(err)
	}
	atomic.StoreUint64(&t.emittedTs, resolvedTs)
	ckpt, err := t.manager.flushBackendSink(ctx, tableID)
	if err != nil {
		return ckpt, err
	}
	logAbnormalCheckpoint(ckpt)
	return ckpt, err
}

func (t *tableSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

// Note once the Close is called, no more events can be written to this table sink
func (t *tableSink) Close(ctx context.Context) error {
	return t.manager.destroyTableSink(ctx, t.tableID)
}

// getResolvedTs returns resolved ts, which means all events before resolved ts
// have been sent to sink manager
func (t *tableSink) getResolvedTs() uint64 {
	ts := atomic.LoadUint64(&t.emittedTs)
	return ts
}

// Barrier is not used in table sink
func (t *tableSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

type drawbackMsg struct {
	tableID  model.TableID
	callback chan struct{}
}

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
) *bufferSink {
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
			tableID := flushEvent.tableID
			checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, flushEvent.tableID, resolvedTs)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			b.tableCheckpointTsMap.Store(tableID, checkpointTs)

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
