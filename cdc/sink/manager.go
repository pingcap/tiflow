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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	redo "github.com/pingcap/ticdc/cdc/redo"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Manager manages table sinks, maintains the relationship between table sinks
// and backendSink.
// Manager is thread-safe.
type Manager struct {
<<<<<<< HEAD
	backendSink  Sink
	checkpointTs model.Ts
	tableSinks   map[model.TableID]*tableSink
	tableSinksMu sync.Mutex
=======
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
func (m *Manager) CreateTableSink(tableID model.TableID, checkpointTs model.Ts, redoManager redo.LogManager) Sink {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	sink := &tableSink{
		tableID:     tableID,
		manager:     m,
		buffer:      make([]*model.RowChangedEvent, 0, 128),
		redoManager: redoManager,
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
		resolvedTs := tableSink.getResolvedTs()
		if minTs > resolvedTs {
			minTs = resolvedTs
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
}

func (m *Manager) getCheckpointTs() uint64 {
	return atomic.LoadUint64(&m.checkpointTs)
=======
	return m.bufSink.Barrier(ctx, tableID)
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

func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&m.changeFeedCheckpointTs, checkpointTs)
	if m.bufSink != nil {
		m.bufSink.UpdateChangeFeedCheckpointTs(checkpointTs)
	}
>>>>>>> b5a52cec7 (sink(ticdc): optimize buffer sink flush from O(N^2) to O(N) (#3899))
}

type drawbackMsg struct {
	tableID  model.TableID
	callback chan struct{}
}
