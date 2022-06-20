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

const (
	defaultMetricInterval = time.Second * 15
)

// Manager manages table sinks, maintains the relationship between table sinks and backendSink.
type Manager struct {
	backendSink            *bufferSink
	tableCheckpointTsMap   sync.Map
	tableSinks             map[model.TableID]*tableSink
	tableSinksMu           sync.Mutex
	changeFeedCheckpointTs uint64

	flushMu  sync.Mutex
	flushing int64

	captureAddr               string
	changefeedID              model.ChangeFeedID
	metricsTableSinkTotalRows prometheus.Counter
}

// NewManager creates a new Sink manager
func NewManager(
	ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts,
	captureAddr string, changefeedID model.ChangeFeedID,
) *Manager {
	return &Manager{
		backendSink:               newBufferSink(ctx, backendSink, errCh, checkpointTs),
		changeFeedCheckpointTs:    checkpointTs,
		tableSinks:                make(map[model.TableID]*tableSink),
		captureAddr:               captureAddr,
		changefeedID:              changefeedID,
		metricsTableSinkTotalRows: tableSinkTotalRowsCountCounter.WithLabelValues(captureAddr, changefeedID),
	}
}

// CreateTableSink creates a table sink
func (m *Manager) CreateTableSink(
	tableID model.TableID,
	checkpointTs model.Ts,
	redoManager redo.LogManager,
) (Sink, error) {
	sink := &tableSink{
		tableID:     tableID,
		manager:     m,
		buffer:      make([]*model.RowChangedEvent, 0, 128),
		emittedTs:   checkpointTs,
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
	tableSinkTotalRowsCountCounter.DeleteLabelValues(m.captureAddr, m.changefeedID)
	if m.backendSink != nil {
		if err := m.backendSink.Close(ctx); err != nil && errors.Cause(err) != context.Canceled {
			log.Warn("close bufSink failed",
				zap.String("changefeed", m.changefeedID),
				zap.Error(err))
			return err
		}
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

// UpdateChangeFeedCheckpointTs update the changeFeedCheckpointTs every processor tick
func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&m.changeFeedCheckpointTs, checkpointTs)
	if m.backendSink != nil {
		m.backendSink.UpdateChangeFeedCheckpointTs(checkpointTs)
	}
}
