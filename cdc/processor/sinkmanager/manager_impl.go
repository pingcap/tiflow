// Copyright 2022 PingCAP, Inc.
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

package sinkmanager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/sorter"
)

const (
	defaultGenerateTaskInterval = 100 * time.Millisecond
)

// Assert SinkManager implementation
var _ Manager = (*ManagerImpl)(nil)

// ManagerImpl is the implementation of Manager.
type ManagerImpl struct {
	changefeed model.ChangeFeedID
	// processorCtx is the context of the processor.
	processorCtx context.Context
	// progressHeap is the heap of the table progress.
	progressHeap *tablesFetchProgress
	// memQuota is used to control the total memory usage of the sink manager.
	memQuota memQuota
	// tableSinks is a map from tableID to tableSink.
	tableSinks sync.Map
	// sortEngine is used by the sink manager to fetch data.
	sortEngine sorter.EventSortEngine
	// redoManager was used to report the resolved ts of the table,
	// if redo log is enabled.
	redoManager redo.LogManager
	// lastBarrierTs is the last barrier ts.
	lastBarrierTs atomic.Uint64
	// taskTicker is used to generate fetch tasks.
	taskTicker *time.Ticker
	// workers used to pull data from source manager.
	workers []worker
	// tableTaskChan is used to send tasks to workers.
	tableTaskChan chan *tableSinkTask
	// closedChan is used to notify the manager is closed.
	closedChan chan struct{}
}

// New creates a new sink manager.
func New(redoManager redo.LogManager) Manager {
	m := &ManagerImpl{
		progressHeap:  newTableFetchProgress(),
		tableSinks:    sync.Map{},
		redoManager:   redoManager,
		tableTaskChan: make(chan *tableSinkTask),
		taskTicker:    time.NewTicker(defaultGenerateTaskInterval),
	}

	// TODO create workers and start them.

	return m
}

// generateTableSinkFetchTask generates a fetch tableSinkTask to fetch data from the source manager.
func (m *ManagerImpl) generateTableSinkFetchTask() error {
	for {
		select {
		case <-m.closedChan:
			return nil
		case <-m.taskTicker.C:
			slowestTableProgress := m.progressHeap.pop()
			tableSink, ok := m.tableSinks.Load(slowestTableProgress.tableID)
			if !ok {
				// TODO log it and continue.
				// Maybe the table sink is removed by the processor.
				// So we do **not** need add it back to the heap.
				continue
			}
			// If redo log is disabled, we use the barrier ts as the upper bound of the fetch tableSinkTask.
			// Because can not exceed the barrier ts.
			barrierTs := m.lastBarrierTs.Load()
			upperBoundPos := sorter.Position{
				StartTs:  barrierTs - 1,
				CommitTs: barrierTs,
			}
			canBeAdvance := slowestTableProgress.nextLowerBoundPos.Compare(upperBoundPos) == -1 ||
				slowestTableProgress.nextLowerBoundPos.Compare(upperBoundPos) == 0
			if !canBeAdvance || !m.memQuota.TryAcquire() {
				m.progressHeap.push(slowestTableProgress)
				continue
			}
			tableID := slowestTableProgress.tableID
			callback := func(lastWrittenPos sorter.Position) {
				p := progress{
					tableID:           tableID,
					nextLowerBoundPos: lastWrittenPos.Next(),
				}
				m.progressHeap.push(p)
			}

			t := &tableSinkTask{
				tableID:       tableID,
				lowerBound:    slowestTableProgress.nextLowerBoundPos,
				lastBarrierTs: &m.lastBarrierTs,
				tableSink:     tableSink.(*tableSinkWrapper),
				callback:      callback,
			}
			select {
			case <-m.closedChan:
				return nil
			case m.tableTaskChan <- t:
			}
		}
	}
}

// UpdateTableResolvedTs updates the resolved ts of the table.
func (m *ManagerImpl) UpdateTableResolvedTs(tableID model.TableID, ts model.Ts) {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		panic("table sink not found")
	}
	tableSink.(*tableSinkWrapper).updateCurrentSorterResolvedTs(ts)
}

// UpdateBarrierTs updates the barrier ts of all tables in the sink manager.
func (m *ManagerImpl) UpdateBarrierTs(ts model.Ts) {
	if ts <= m.lastBarrierTs.Load() {
		return
	}
	m.lastBarrierTs.Store(ts)
}

// AddTable adds a table(TableSink) to the sink manager.
func (m *ManagerImpl) AddTable(tableID model.TableID, startTs model.Ts) {
	// TODO create a new table sink and start it.
	panic("implement me")
}

// RemoveTable removes a table(TableSink) from the sink manager.
func (m *ManagerImpl) RemoveTable(tableID model.TableID) {
	// TODO stop the table sink and remove it.
	panic("implement me")
}

// GetTableStats returns the state of the table.
func (m *ManagerImpl) GetTableStats(tableID model.TableID) pipeline.Stats {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		panic("table sink not found")
	}
	checkpointTs := tableSink.(*tableSinkWrapper).getTableSinkCheckpointTs()
	m.memQuota.Release(tableID, checkpointTs)
	// TODO GC the source manager.
	return pipeline.Stats{
		CheckpointTs: checkpointTs.Ts,
		ResolvedTs:   tableSink.(*tableSinkWrapper).getCurrentSorterResolvedTs(),
		BarrierTs:    m.lastBarrierTs.Load(),
	}
}

// Close closes all workers.
func (m *ManagerImpl) Close() {
	close(m.closedChan)
	// TODO close all table sinks.
	for _, w := range m.workers {
		w.close()
	}
}
