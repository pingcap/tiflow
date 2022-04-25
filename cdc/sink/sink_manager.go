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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Manager manages table sinks, maintains the relationship between table sinks
// and backendSink.
// Manager is thread-safe.
type Manager struct {
	bufSink      *bufferSink
	tableSinks   map[model.TableID]*tableSink
	tableSinksMu sync.Mutex

	changefeedID              model.ChangeFeedID
	metricsTableSinkTotalRows prometheus.Counter
}

// NewManager creates a new Sink manager
func NewManager(
	ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts,
	captureAddr string, changefeedID model.ChangeFeedID,
) *Manager {
	bufSink := newBufferSink(backendSink, checkpointTs)
	go bufSink.run(ctx, changefeedID, errCh)
	counter := metrics.TableSinkTotalRowsCountCounter.WithLabelValues(changefeedID)
	return &Manager{
		bufSink:                   bufSink,
		tableSinks:                make(map[model.TableID]*tableSink),
		changefeedID:              changefeedID,
		metricsTableSinkTotalRows: counter,
	}
}

// CreateTableSink creates a table sink
func (m *Manager) CreateTableSink(
	tableID model.TableID,
	redoManager redo.LogManager,
) (Sink, error) {
	sink := &tableSink{
		tableID:     tableID,
		manager:     m,
		buffer:      make([]*model.RowChangedEvent, 0, 128),
		redoManager: redoManager,
	}

	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	if err := sink.Init(tableID); err != nil {
		return nil, errors.Trace(err)
	}
	m.tableSinks[tableID] = sink
	return sink, nil
}

// Close closes the Sink manager and backend Sink, this method can be reentrantly called
func (m *Manager) Close(ctx context.Context) error {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	metrics.TableSinkTotalRowsCountCounter.DeleteLabelValues(m.changefeedID)
	if m.bufSink != nil {
		if err := m.bufSink.Close(ctx); err != nil && errors.Cause(err) != context.Canceled {
			log.Warn("close bufSink failed",
				zap.String("changefeed", m.changefeedID),
				zap.Error(err))
			return err
		}
	}
	return nil
}

func (m *Manager) destroyTableSink(ctx context.Context, tableID model.TableID) error {
	m.tableSinksMu.Lock()
	delete(m.tableSinks, tableID)
	m.tableSinksMu.Unlock()
	return m.bufSink.Barrier(ctx, tableID)
}

// UpdateChangeFeedCheckpointTs updates changefeed level checkpointTs,
// this value is used in getCheckpointTs func
func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	if m.bufSink != nil {
		m.bufSink.UpdateChangeFeedCheckpointTs(checkpointTs)
	}
}
