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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
	"github.com/pingcap/tiflow/pkg/sorter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultWorkerNum            = 8
	defaultGenerateTaskInterval = 100 * time.Millisecond
)

// Assert SinkManager implementation
var _ Manager = (*ManagerImpl)(nil)

// ManagerImpl is the implementation of Manager.
type ManagerImpl struct {
	changefeedID model.ChangeFeedID
	// ctx used to control the background goroutines.
	ctx context.Context
	// cancel is used to cancel the background goroutines.
	cancel context.CancelFunc
	// progressHeap is the heap of the table progress.
	progressHeap *tableProgresses
	// memQuota is used to control the total memory usage of the sink manager.
	memQuota *memQuota
	// sinkFactory used to create table sink.
	sinkFactory *factory.SinkFactory
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
	// wg is used to wait for all the workers to exit.
	wg      sync.WaitGroup
	errChan chan error
	// Metric for table sink.
	metricsTableSinkTotalRows prometheus.Counter
}

// New creates a new sink manager.
func New(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	changefeedInfo *model.ChangeFeedInfo,
	redoManager redo.LogManager,
	sortEngine sorter.EventSortEngine,
	errChan chan error,
	metricsTableSinkTotalRows prometheus.Counter,
) (Manager, error) {
	tableSinkFactory, err := factory.New(
		ctx,
		changefeedInfo.SinkURI,
		changefeedInfo.Config,
		errChan,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(ctx)
	m := &ManagerImpl{
		changefeedID: changefeedID,
		ctx:          ctx,
		cancel:       cancel,
		progressHeap: newTableProgresses(),
		memQuota:     newMemQuota(changefeedID, changefeedInfo.Config.MemoryQuota),
		sinkFactory:  tableSinkFactory,
		sortEngine:   sortEngine,
		redoManager:  redoManager,
		taskTicker:   time.NewTicker(defaultGenerateTaskInterval),
		workers:      make([]worker, defaultWorkerNum),
		// No buffer for tableTaskChan,
		// because we want to block the table sink task if there is no worker available.
		tableTaskChan:             make(chan *tableSinkTask),
		metricsTableSinkTotalRows: metricsTableSinkTotalRows,
	}

	m.startWorkers(changefeedInfo.Config.EnableOldValue, changefeedInfo.Config.EnableOldValue)
	m.startGenerateTableTask()

	return m, nil
}

// start all the workers and report the error to the error channel.
func (m *ManagerImpl) startWorkers(splitTxn bool, enableOldValue bool) {
	for i := 0; i < defaultWorkerNum; i++ {
		w := newWorker(m.changefeedID, m.sortEngine, m.redoManager,
			m.memQuota, splitTxn, enableOldValue)
		m.workers = append(m.workers, w)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			err := w.receiveTableSinkTask(m.ctx, m.tableTaskChan)
			if err != nil {
				log.Error("Worker receive table sink task failed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Error(err))
				select {
				case m.errChan <- err:
				default:
					log.Error("Failed to send error to error channel, error channel is full",
						zap.String("namespace", m.changefeedID.Namespace),
						zap.String("changefeed", m.changefeedID.ID),
						zap.Error(err))
				}
			}
		}()
	}
}

// start generate table task and report error to the error channel.
func (m *ManagerImpl) startGenerateTableTask() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.generateTableSinkFetchTask()
		if err != nil {
			log.Error("Generate table sink fetch task failed",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Error(err))
			select {
			case m.errChan <- err:
			default:
				log.Error("Failed to send error to error channel, error channel is full",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Error(err))
			}
		}
	}()
}

// generateTableSinkFetchTask generates a fetch tableSinkTask to fetch data from the source manager.
func (m *ManagerImpl) generateTableSinkFetchTask() error {
	defer m.taskTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-m.taskTicker.C:
			slowestTableProgress := m.progressHeap.pop()
			tableID := slowestTableProgress.tableID
			tableSink, ok := m.tableSinks.Load(tableID)
			if !ok {
				log.Info("Table sink not found, probably already removed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableID))
				// Maybe the table sink is removed by the processor.(Scheduled the table to other nodes.)
				// So we do **not** need add it back to the heap.
				continue
			}
			// We use the barrier ts as the upper bound of the fetch tableSinkTask.
			// Because it can not exceed the barrier ts.
			// We also need to consider the resolved ts from sorter,
			// Because if the redo log is enabled and the table just scheduled to this node,
			// the resolved ts from sorter may be smaller than the barrier ts.
			// So we use the min value of the barrier ts and the resolved ts from sorter.
			getUpperBound := func() sorter.Position {
				barrierTs := m.lastBarrierTs.Load()
				resolvedTs := tableSink.(*tableSinkWrapper).getReceivedSorterResolvedTs()
				var upperBoundTs model.Ts
				if resolvedTs > barrierTs {
					upperBoundTs = barrierTs
				} else {
					upperBoundTs = resolvedTs
				}

				return sorter.Position{
					StartTs:  upperBoundTs - 1,
					CommitTs: upperBoundTs,
				}
			}
			checkAdvance := slowestTableProgress.nextLowerBoundPos.Compare(getUpperBound())
			if !(checkAdvance == -1 || checkAdvance == 0) || !m.memQuota.tryAcquire(defaultRequestMemSize) {
				m.progressHeap.push(slowestTableProgress)
				// Next time.
				continue
			}
			callback := func(lastWrittenPos sorter.Position) {
				p := &progress{
					tableID:           tableID,
					nextLowerBoundPos: lastWrittenPos.Next(),
				}
				m.progressHeap.push(p)
			}

			t := &tableSinkTask{
				tableID:              tableID,
				lowerBound:           slowestTableProgress.nextLowerBoundPos,
				upperBarrierTsGetter: getUpperBound,
				tableSink:            tableSink.(*tableSinkWrapper),
				callback:             callback,
			}
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			case m.tableTaskChan <- t:
			}
		}
	}
}

// UpdateReceivedSorterResolvedTs updates the received sorter resolved ts for the table.
func (m *ManagerImpl) UpdateReceivedSorterResolvedTs(tableID model.TableID, ts model.Ts) {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when updating resolved ts",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	tableSink.(*tableSinkWrapper).updateReceivedSorterResolvedTs(ts)
}

// UpdateBarrierTs updates the barrier ts of all tables in the sink manager.
func (m *ManagerImpl) UpdateBarrierTs(ts model.Ts) {
	// It is safe to do not use compare and swap here.
	// Only the processor will update the barrier ts.
	// Other goroutines will only read the barrier ts.
	// So it is safe to do not use compare and swap here, just Load and Store.
	if ts <= m.lastBarrierTs.Load() {
		return
	}
	m.lastBarrierTs.Store(ts)
}

// AddTable adds a table(TableSink) to the sink manager.
func (m *ManagerImpl) AddTable(tableID model.TableID, startTs model.Ts, targetTs model.Ts) {
	sinkWrapper := newTableSinkWrapper(
		m.changefeedID,
		tableID,
		m.sinkFactory.CreateTableSink(m.changefeedID, tableID, m.metricsTableSinkTotalRows),
		tablepb.TableStatePreparing,
		targetTs,
	)
	m.tableSinks.Store(tableID, sinkWrapper)
	initProgress := &progress{
		tableID:           tableID,
		nextLowerBoundPos: sorter.Position{StartTs: startTs - 1, CommitTs: startTs},
	}
	m.progressHeap.push(initProgress)
}

// RemoveTable removes a table(TableSink) from the sink manager.
func (m *ManagerImpl) RemoveTable(tableID model.TableID) error {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	err := tableSink.(*tableSinkWrapper).close(m.ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// NOTICE: It is safe to only remove the table sink from the map.
	// Because if we found the table sink is not in the map, we will not add it back to the heap.
	// Also, no need to GC the SorterEngine. Because the SorterEngine also removes this table.
	m.tableSinks.Delete(tableID)
	return nil
}

// GetTableStats returns the state of the table.
func (m *ManagerImpl) GetTableStats(tableID model.TableID) (pipeline.Stats, error) {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when getting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	checkpointTs := tableSink.(*tableSinkWrapper).getCheckpointTs()
	m.memQuota.release(tableID, checkpointTs)
	cleanPos := sorter.Position{
		StartTs:  checkpointTs.Ts - 1,
		CommitTs: checkpointTs.Ts,
	}
	err := m.sortEngine.CleanByTable(tableID, cleanPos)
	if err != nil {
		return pipeline.Stats{}, errors.Trace(err)
	}
	return pipeline.Stats{
		CheckpointTs: checkpointTs.Ts,
		ResolvedTs:   tableSink.(*tableSinkWrapper).getReceivedSorterResolvedTs(),
		BarrierTs:    m.lastBarrierTs.Load(),
	}, nil
}

// Close closes all workers.
func (m *ManagerImpl) Close() {
	m.cancel()
	m.wg.Wait()
}
