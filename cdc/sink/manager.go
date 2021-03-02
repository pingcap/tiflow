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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	// TODO: buffer chan size, the accumulated data is determined by
	// the count of sorted data and unmounted data. In current benchmark a single
	// processor can reach 50k-100k QPS, and accumulated data is around
	// 200k-400k in most cases. We need a better chan cache mechanism.
	defaultBufferChanSize = 1280000
	defaultMetricInterval = time.Second * 15
)

// Manager manages table sinks, maintains the relationship between table sinks and backendSink
type Manager struct {
	backendSink  Sink
	checkpointTs model.Ts
	tableSinks   map[model.TableID]*tableSink
	tableSinksMu sync.Mutex

	flushMu sync.Mutex
}

// NewManager creates a new Sink manager
func NewManager(ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts) *Manager {
	return &Manager{
		backendSink:  newBufferSink(ctx, backendSink, errCh, checkpointTs),
		checkpointTs: checkpointTs,
		tableSinks:   make(map[model.TableID]*tableSink),
	}
}

// CreateTableSink creates a table sink
func (m *Manager) CreateTableSink(tableID model.TableID, checkpointTs model.Ts) Sink {
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	sink := &tableSink{
		tableID:   tableID,
		manager:   m,
		buffer:    make([]*model.RowChangedEvent, 0, 128),
		emittedTs: checkpointTs,
	}
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	m.tableSinks[tableID] = sink
	return sink
}

// Close closes the Sink manager and backend Sink
func (m *Manager) Close() error {
	return m.backendSink.Close()
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
	}
	return minTs
}

func (m *Manager) flushBackendSink(ctx context.Context) (model.Ts, error) {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()
	minEmittedTs := m.getMinEmittedTs()
	checkpointTs, err := m.backendSink.FlushRowChangedEvents(ctx, minEmittedTs)
	if err != nil {
		return m.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&m.checkpointTs, checkpointTs)
	return checkpointTs, nil
}

func (m *Manager) destroyTableSink(tableID model.TableID) {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	delete(m.tableSinks, tableID)
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
	return nil
}

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

func (t *tableSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	i := sort.Search(len(t.buffer), func(i int) bool {
		return t.buffer[i].CommitTs > resolvedTs
	})
	if i == 0 {
		atomic.StoreUint64(&t.emittedTs, resolvedTs)
		return t.manager.flushBackendSink(ctx)
	}
	resolvedRows := t.buffer[:i]
	t.buffer = append(make([]*model.RowChangedEvent, 0, len(t.buffer[i:])), t.buffer[i:]...)

	err := t.manager.backendSink.EmitRowChangedEvents(ctx, resolvedRows...)
	if err != nil {
		return t.manager.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&t.emittedTs, resolvedTs)
	return t.manager.flushBackendSink(ctx)
}

func (t *tableSink) getEmittedTs() uint64 {
	return atomic.LoadUint64(&t.emittedTs)
}

func (t *tableSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

func (t *tableSink) Close() error {
	t.manager.destroyTableSink(t.tableID)
	return nil
}

type bufferSink struct {
	Sink
	buffer chan struct {
		rows       []*model.RowChangedEvent
		resolvedTs model.Ts
	}
	checkpointTs uint64
}

func newBufferSink(ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts) Sink {
	sink := &bufferSink{
		Sink: backendSink,
		buffer: make(chan struct {
			rows       []*model.RowChangedEvent
			resolvedTs model.Ts
		}, defaultBufferChanSize),
		checkpointTs: checkpointTs,
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
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil && errors.Cause(err) != context.Canceled {
				errCh <- err
			}
			return
		case e := <-b.buffer:
			if e.rows == nil {
				// A resolved event received
				start := time.Now()
				checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, e.resolvedTs)
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
				continue
			}
			start := time.Now()
			err := b.Sink.EmitRowChangedEvents(ctx, e.rows...)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			dur := time.Since(start)
			metricEmitRowDuration.Observe(dur.Seconds())
		case <-time.After(defaultMetricInterval):
			metricBufferSize.Set(float64(len(b.buffer)))
		}
	}
}

func (b *bufferSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.buffer <- struct {
		rows       []*model.RowChangedEvent
		resolvedTs model.Ts
	}{rows: rows}:
	}
	return nil
}

func (b *bufferSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return atomic.LoadUint64(&b.checkpointTs), ctx.Err()
	case b.buffer <- struct {
		rows       []*model.RowChangedEvent
		resolvedTs model.Ts
	}{resolvedTs: resolvedTs, rows: nil}:
	}
	return atomic.LoadUint64(&b.checkpointTs), nil
}
