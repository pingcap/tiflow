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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Manager manages table sinks, maintains the relationship between table sinks
// and backendSink.
// Manager is thread-safe.
type Manager struct {
	bufSink                *bufferSink
	tableCheckpointTsMap   sync.Map
	tableSinks             map[model.TableID]*tableSink
	tableSinksMu           sync.Mutex
	changeFeedCheckpointTs uint64

	drawbackChan chan drawbackMsg

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
		bufSink:                   bufSink,
		changeFeedCheckpointTs:    checkpointTs,
		tableSinks:                make(map[model.TableID]*tableSink),
		drawbackChan:              drawbackChan,
		changefeedID:              changefeedID,
		metricsTableSinkTotalRows: tableSinkTotalRowsCountCounter.WithLabelValues(changefeedID),
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
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	tableSinkTotalRowsCountCounter.DeleteLabelValues(m.changefeedID)
	if m.bufSink != nil {
		log.Info("sinkManager try close bufSink",
			zap.String("changefeed", m.changefeedID))
		start := time.Now()
		if err := m.bufSink.Close(ctx); err != nil {
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

func (m *Manager) flushBackendSink(ctx context.Context, tableID model.TableID, resolvedTs uint64) (model.Ts, error) {
	checkpointTs, err := m.bufSink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
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

// UpdateChangeFeedCheckpointTs updates changedfeed level checkpointTs,
// this value is used in getCheckpointTs func
func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&m.changeFeedCheckpointTs, checkpointTs)
	if m.bufSink != nil {
		m.bufSink.UpdateChangeFeedCheckpointTs(checkpointTs)
	}
}

type drawbackMsg struct {
	tableID  model.TableID
	callback chan struct{}
}
