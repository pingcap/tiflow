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
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	sinkWorkerNum               = 8
	redoWorkerNum               = 4
	defaultGenerateTaskInterval = 100 * time.Millisecond
)

// SinkManager is the implementation of SinkManager.
type SinkManager struct {
	changefeedID model.ChangeFeedID
	// ctx used to control the background goroutines.
	ctx context.Context
	// cancel is used to cancel the background goroutines.
	cancel context.CancelFunc

	// sinkProgressHeap is the heap of the table progress for sink.
	sinkProgressHeap *tableProgresses
	// redoProgressHeap is the heap of the table progress for redo.
	redoProgressHeap *tableProgresses

	// memQuota is used to control the total memory usage of the sink manager.
	memQuota *memQuota
	// eventCache caches events fetched from sort engine.
	eventCache *redoEventCache
	// redoManager was used to report the resolved ts of the table,
	// if redo log is enabled.
	redoManager redo.LogManager
	// sortEngine is used by the sink manager to fetch data.
	sortEngine engine.SortEngine

	// sinkFactory used to create table sink.
	sinkFactory *factory.SinkFactory
	// tableSinks is a map from tableID to tableSink.
	tableSinks sync.Map
	// lastBarrierTs is the last barrier ts.
	lastBarrierTs atomic.Uint64

	// sinkWorkers used to pull data from source manager.
	sinkWorkers []sinkWorker
	// sinkTaskChan is used to send tasks to sinkWorkers.
	sinkTaskChan chan *sinkTask

	// redoWorkers used to pull data from source manager.
	redoWorkers []redoWorker
	// redoTaskChan is used to send tasks to redoWorkers.
	redoTaskChan chan *redoTask

	// wg is used to wait for all workers to exit.
	wg sync.WaitGroup

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
	sortEngine engine.SortEngine,
	mg entry.MounterGroup,
	errChan chan error,
	metricsTableSinkTotalRows prometheus.Counter,
) (*SinkManager, error) {
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
	m := &SinkManager{
		changefeedID: changefeedID,
		ctx:          ctx,
		cancel:       cancel,
		memQuota:     newMemQuota(changefeedID, changefeedInfo.Config.MemoryQuota),
		sinkFactory:  tableSinkFactory,
		sortEngine:   sortEngine,

		sinkProgressHeap: newTableProgresses(),
		sinkWorkers:      make([]sinkWorker, 0, sinkWorkerNum),
		sinkTaskChan:     make(chan *sinkTask),

		metricsTableSinkTotalRows: metricsTableSinkTotalRows,
	}

	if redoManager != nil {
		m.redoManager = redoManager
		m.redoProgressHeap = newTableProgresses()
		m.redoWorkers = make([]redoWorker, 0, redoWorkerNum)
		m.redoTaskChan = make(chan *redoTask)
		// Use at most 1/3 memory quota for redo event cache.
		m.eventCache = newRedoEventCache(changefeedInfo.Config.MemoryQuota / 3)
	}

	m.startWorkers(mg, changefeedInfo.Config.EnableOldValue, changefeedInfo.Config.EnableOldValue)
	m.startGenerateTasks()
	return m, nil
}

// start all workers and report the error to the error channel.
func (m *SinkManager) startWorkers(mg entry.MounterGroup, splitTxn bool, enableOldValue bool) {
	for i := 0; i < sinkWorkerNum; i++ {
		w := newSinkWorker(m.changefeedID, mg, m.sortEngine, m.memQuota,
			m.eventCache, splitTxn, enableOldValue)
		m.sinkWorkers = append(m.sinkWorkers, w)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			err := w.handleTasks(m.ctx, m.sinkTaskChan)
			if err != nil {
				log.Error("Worker handles sink task failed",
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

	if m.redoManager == nil {
		return
	}

	for i := 0; i < redoWorkerNum; i++ {
		w := newRedoWorker(m.changefeedID, mg, m.sortEngine, m.memQuota,
			m.redoManager, m.eventCache, splitTxn, enableOldValue)
		m.redoWorkers = append(m.redoWorkers, w)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			err := w.handleTasks(m.ctx, m.redoTaskChan)
			if err != nil {
				log.Error("Worker handles redo task failed",
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
func (m *SinkManager) startGenerateTasks() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.generateSinkTasks()
		if err != nil {
			log.Error("Generate sink tasks failed",
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

	if m.redoManager == nil {
		return
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.generateRedoTasks()
		if err != nil {
			log.Error("Generate redo tasks failed",
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

// generateSinkTasks generates tasks to fetch data from the source manager.
func (m *SinkManager) generateSinkTasks() error {
	taskTicker := time.NewTicker(defaultGenerateTaskInterval)
	defer taskTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-taskTicker.C:
			// No more tables.
			if m.sinkProgressHeap.len() == 0 {
				continue
			}
			slowestTableProgress := m.sinkProgressHeap.pop()
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
			tableState := tableSink.(*tableSinkWrapper).getState()
			if tableState < tablepb.TableStateReplicating {
				log.Panic("Tables that are not started should not appear in the progress heap",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableID))
			}
			// It means table sink is stopping or stopped.
			// We should skip it and do not push it back.
			// Because there is no case that stopping/stopped -> replicating.
			if tableState > tablepb.TableStateReplicating {
				continue
			}
			log.Info("Generate sink task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID))
			// We use the barrier ts as the upper bound of the fetch tableSinkTask.
			// Because it can not exceed the barrier ts.
			// We also need to consider the resolved ts from sorter,
			// Because if the redo log is enabled and the table just scheduled to this node,
			// the resolved ts from sorter may be smaller than the barrier ts.
			// So we use the min value of the barrier ts and the resolved ts from sorter.
			getUpperBound := func() engine.Position {
				barrierTs := m.lastBarrierTs.Load()
				resolvedTs := tableSink.(*tableSinkWrapper).getReceivedSorterResolvedTs()
				var upperBoundTs model.Ts
				if resolvedTs > barrierTs {
					upperBoundTs = barrierTs
				} else {
					upperBoundTs = resolvedTs
				}

				return engine.Position{
					StartTs:  upperBoundTs - 1,
					CommitTs: upperBoundTs,
				}
			}
			// Only generate the table sink task if lower bound less or equal the upper bound.
			checkAdvance := slowestTableProgress.nextLowerBoundPos.Compare(getUpperBound())
			if !(checkAdvance == -1 || checkAdvance == 0) || !m.memQuota.tryAcquire(requestMemSize) {
				m.sinkProgressHeap.push(slowestTableProgress)
				// Next time.
				continue
			}
			log.Debug("MemoryQuotaTracing: Acquire memory for table sink task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("memory", requestMemSize),
			)
			callback := func(lastWrittenPos engine.Position) {
				p := &progress{
					tableID:           tableID,
					nextLowerBoundPos: lastWrittenPos.Next(),
				}
				m.sinkProgressHeap.push(p)
			}

			t := &sinkTask{
				tableID:       tableID,
				lowerBound:    slowestTableProgress.nextLowerBoundPos,
				getUpperBound: getUpperBound,
				tableSink:     tableSink.(*tableSinkWrapper),
				callback:      callback,
				isCanceled: func() bool {
					return tableSink.(*tableSinkWrapper).getState() != tablepb.TableStateReplicating
				},
			}
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			case m.sinkTaskChan <- t:
			}
		}
	}
}

func (m *SinkManager) generateRedoTasks() error {
	taskTicker := time.NewTicker(defaultGenerateTaskInterval)
	defer taskTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-taskTicker.C:
			// No more tables.
			if m.redoProgressHeap.len() == 0 {
				continue
			}
			slowestTableProgress := m.redoProgressHeap.pop()
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
			// We use the table's resolved ts as the upper bound to fetch events.
			getUpperBound := func() engine.Position {
				upperBoundTs := tableSink.(*tableSinkWrapper).getReceivedSorterResolvedTs()
				return engine.Position{
					StartTs:  upperBoundTs - 1,
					CommitTs: upperBoundTs,
				}
			}
			checkAdvance := slowestTableProgress.nextLowerBoundPos.Compare(getUpperBound())
			if !(checkAdvance == -1 || checkAdvance == 0) || !m.memQuota.tryAcquire(requestMemSize) {
				m.redoProgressHeap.push(slowestTableProgress)
				// Next time.
				continue
			}
			callback := func(lastWrittenPos engine.Position) {
				p := &progress{
					tableID:           tableID,
					nextLowerBoundPos: lastWrittenPos.Next(),
				}
				m.redoProgressHeap.push(p)
			}

			t := &redoTask{
				tableID:       tableID,
				lowerBound:    slowestTableProgress.nextLowerBoundPos,
				getUpperBound: getUpperBound,
				tableSink:     tableSink.(*tableSinkWrapper),
				callback:      callback,
			}
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			case m.redoTaskChan <- t:
			}
		}
	}
}

// UpdateReceivedSorterResolvedTs updates the received sorter resolved ts for the table.
func (m *SinkManager) UpdateReceivedSorterResolvedTs(tableID model.TableID, ts model.Ts) {
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
func (m *SinkManager) UpdateBarrierTs(ts model.Ts) {
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
func (m *SinkManager) AddTable(tableID model.TableID, startTs model.Ts, targetTs model.Ts) {
	sinkWrapper := newTableSinkWrapper(
		m.changefeedID,
		tableID,
		m.sinkFactory.CreateTableSink(m.changefeedID, tableID, m.metricsTableSinkTotalRows),
		tablepb.TableStatePreparing,
		startTs,
		targetTs,
	)
	m.tableSinks.Store(tableID, sinkWrapper)
}

// StartTable sets the table(TableSink) state to replicating.
func (m *SinkManager) StartTable(tableID model.TableID, startTs model.Ts) {
	log.Info("Start table sink",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Int64("tableID", tableID))
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when starting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	tableSink.(*tableSinkWrapper).start()
	m.sinkProgressHeap.push(&progress{
		tableID:           tableID,
		nextLowerBoundPos: engine.Position{StartTs: startTs - 1, CommitTs: startTs},
	})
	if m.redoManager != nil {
		m.redoProgressHeap.push(&progress{
			tableID:           tableID,
			nextLowerBoundPos: engine.Position{StartTs: startTs - 1, CommitTs: startTs},
		})
	}
}

// AsyncStopTable sets the table(TableSink) state to stopped.
func (m *SinkManager) AsyncStopTable(tableID model.TableID) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		tableSink, ok := m.tableSinks.Load(tableID)
		if !ok {
			log.Panic("Table sink not found when removing table",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID))
		}
		err := tableSink.(*tableSinkWrapper).close(m.ctx)
		if err != nil {
			log.Warn("Failed to close table sink",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Error(err))
			m.errChan <- err
		}
		cleanedBytes := m.memQuota.clean(tableID)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("memory", cleanedBytes),
		)
	}()
}

// RemoveTable removes a table(TableSink) from the sink manager.
func (m *SinkManager) RemoveTable(tableID model.TableID) {
	// NOTICE: It is safe to only remove the table sink from the map.
	// Because if we found the table sink is closed, we will not add it back to the heap.
	// Also, no need to GC the SortEngine. Because the SortEngine also removes this table.
	m.tableSinks.Delete(tableID)
	if m.eventCache != nil {
		m.eventCache.removeTable(tableID)
	}
}

// GetAllCurrentTableIDs returns all the table IDs in the sink manager.
func (m *SinkManager) GetAllCurrentTableIDs() []model.TableID {
	var tableIDs []model.TableID
	m.tableSinks.Range(func(key, value interface{}) bool {
		tableIDs = append(tableIDs, key.(model.TableID))
		return true
	})
	return tableIDs
}

// GetTableState returns the table(TableSink) state.
func (m *SinkManager) GetTableState(tableID model.TableID) (tablepb.TableState, bool) {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Debug("Table sink not found when getting table state",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return tablepb.TableStateAbsent, false
	}
	return tableSink.(*tableSinkWrapper).getState(), true
}

// GetTableStats returns the state of the table.
func (m *SinkManager) GetTableStats(tableID model.TableID) (pipeline.Stats, error) {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when getting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	checkpointTs := tableSink.(*tableSinkWrapper).getCheckpointTs()
	m.memQuota.release(tableID, checkpointTs)
	cleanPos := engine.Position{
		StartTs:  checkpointTs.Ts - 1,
		CommitTs: checkpointTs.Ts,
	}
	err := m.sortEngine.CleanByTable(tableID, cleanPos)
	if err != nil {
		return pipeline.Stats{}, errors.Trace(err)
	}
	var resolvedTs model.Ts
	// If redo log is enabled, we have to use redo log's resolved ts to calculate processor's min resolved ts.
	if m.redoManager != nil {
		resolvedTs = m.redoManager.GetResolvedTs(tableID)
	} else {
		resolvedTs = m.sortEngine.GetResolvedTs(tableID)
	}
	return pipeline.Stats{
		CheckpointTs: checkpointTs.Ts,
		ResolvedTs:   resolvedTs,
		BarrierTs:    m.lastBarrierTs.Load(),
	}, nil
}

// Close closes all workers.
func (m *SinkManager) Close() error {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.memQuota.close()
	err := m.sinkFactory.Close()
	if err != nil {
		return errors.Trace(err)
	}
	m.tableSinks.Range(func(key, value interface{}) bool {
		err := value.(*tableSinkWrapper).close(m.ctx)
		if err != nil {
			log.Error("Close table sink failed",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", key.(model.TableID)),
				zap.Error(err))
		}
		return true
	})
	m.wg.Wait()
	return nil
}
