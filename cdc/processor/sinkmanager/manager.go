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
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	sinkWorkerNum               = 8
	redoWorkerNum               = 4
	defaultGenerateTaskInterval = 100 * time.Millisecond
	defaultEngineGCChanSize     = 128
)

type gcEvent struct {
	tableID  model.TableID
	cleanPos engine.Position
}

// TableStats of a table sink.
type TableStats struct {
	CheckpointTs model.Ts
	ResolvedTs   model.Ts
	BarrierTs    model.Ts
	// From sorter.
	ReceivedMaxCommitTs   model.Ts
	ReceivedMaxResolvedTs model.Ts
}

// SinkManager is the implementation of SinkManager.
type SinkManager struct {
	changefeedID model.ChangeFeedID
	// ctx used to control the background goroutines.
	ctx context.Context
	// cancel is used to cancel the background goroutines.
	cancel context.CancelFunc

	// up is the upstream and used to get the current pd time.
	up *upstream.Upstream

	// sinkProgressHeap is the heap of the table progress for sink.
	sinkProgressHeap *tableProgresses
	// redoProgressHeap is the heap of the table progress for redo.
	redoProgressHeap *tableProgresses

	// memQuota is used to control the total memory usage of the sink manager.
	memQuota *memQuota
	// eventCache caches events fetched from sort engine.
	eventCache *redoEventCache
	// redoManager is used to report the resolved ts of the table if redo log is enabled.
	redoManager redo.LogManager
	// sourceManager is used by the sink manager to fetch data.
	sourceManager *sourcemanager.SourceManager

	// sinkFactory used to create table sink.
	sinkFactory *factory.SinkFactory
	// tableSinks is a map from tableID to tableSink.
	tableSinks sync.Map
	// lastBarrierTs is the last barrier ts.
	lastBarrierTs atomic.Uint64

	// engineGCChan is used to GC engine when the table is advanced.
	engineGCChan chan *gcEvent

	// sinkWorkers used to pull data from source manager.
	sinkWorkers []*sinkWorker
	// sinkTaskChan is used to send tasks to sinkWorkers.
	sinkTaskChan chan *sinkTask

	// redoWorkers used to pull data from source manager.
	redoWorkers []*redoWorker
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
	up *upstream.Upstream,
	redoManager redo.LogManager,
	sourceManager *sourcemanager.SourceManager,
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
		changefeedID:  changefeedID,
		ctx:           ctx,
		cancel:        cancel,
		up:            up,
		memQuota:      newMemQuota(changefeedID, changefeedInfo.Config.MemoryQuota),
		sinkFactory:   tableSinkFactory,
		sourceManager: sourceManager,

		engineGCChan: make(chan *gcEvent, defaultEngineGCChanSize),

		sinkProgressHeap: newTableProgresses(),
		sinkWorkers:      make([]*sinkWorker, 0, sinkWorkerNum),
		sinkTaskChan:     make(chan *sinkTask),

		metricsTableSinkTotalRows: metricsTableSinkTotalRows,
	}

	if redoManager != nil && redoManager.Enabled() {
		m.redoManager = redoManager
		m.redoProgressHeap = newTableProgresses()
		m.redoWorkers = make([]*redoWorker, 0, redoWorkerNum)
		m.redoTaskChan = make(chan *redoTask)
		// Use at most 1/3 memory quota for redo event cache.
		m.eventCache = newRedoEventCache(changefeedID, changefeedInfo.Config.MemoryQuota/3)
	}

	m.startWorkers(changefeedInfo.Config.Sink.TxnAtomicity.ShouldSplitTxn(), changefeedInfo.Config.EnableOldValue)
	m.startGenerateTasks()
	m.backgroundGC()

	log.Info("Sink manager is created",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.Bool("withRedoEnabled", m.redoManager != nil))

	return m, nil
}

// start all workers and report the error to the error channel.
func (m *SinkManager) startWorkers(splitTxn bool, enableOldValue bool) {
	for i := 0; i < sinkWorkerNum; i++ {
		w := newSinkWorker(m.changefeedID, m.sourceManager, m.memQuota,
			m.eventCache, splitTxn, enableOldValue)
		m.sinkWorkers = append(m.sinkWorkers, w)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			err := w.handleTasks(m.ctx, m.sinkTaskChan)
			if err != nil && !cerrors.Is(err, context.Canceled) {
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
		w := newRedoWorker(m.changefeedID, m.sourceManager, m.memQuota,
			m.redoManager, m.eventCache, splitTxn, enableOldValue)
		m.redoWorkers = append(m.redoWorkers, w)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			err := w.handleTasks(m.ctx, m.redoTaskChan)
			if err != nil && !cerrors.Is(err, context.Canceled) {
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
		if err != nil && !cerrors.Is(err, context.Canceled) {
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
		if err != nil && !cerrors.Is(err, context.Canceled) {
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

// backgroundGC is used to clean up the old data in the sorter.
func (m *SinkManager) backgroundGC() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-m.ctx.Done():
				log.Info("Background GC is stooped because context is canceled",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID))
				return
			case gcEvent := <-m.engineGCChan:
				if err := m.sourceManager.CleanByTable(gcEvent.tableID, gcEvent.cleanPos); err != nil {
					log.Error("Failed to clean table in sort engine",
						zap.String("namespace", m.changefeedID.Namespace),
						zap.String("changefeed", m.changefeedID.ID),
						zap.Int64("tableID", gcEvent.tableID),
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
			// It means table sink is stopping or stopped.
			// We should skip it and do not push it back.
			// Because there is no case that stopping/stopped -> replicating.
			if tableState != tablepb.TableStateReplicating {
				log.Info("Table sink is not replicating, skip it",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableID),
					zap.String("tableState", tableState.String()))
				continue
			}
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
			upperBound := getUpperBound()
			// Only generate the table sink task if lower bound less or equal the upper bound.
			checkAdvance := slowestTableProgress.nextLowerBoundPos.Compare(upperBound)
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
			log.Info("Generate sink task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Any("lowerBound", slowestTableProgress.nextLowerBoundPos),
				zap.Any("currentUpperBound", upperBound),
			)
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

			log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("memory", requestMemSize))

			t := &redoTask{
				tableID:       tableID,
				lowerBound:    slowestTableProgress.nextLowerBoundPos,
				getUpperBound: getUpperBound,
				tableSink:     tableSink.(*tableSinkWrapper),
				callback: func(lastWrittenPos engine.Position) {
					p := &progress{
						tableID:           tableID,
						nextLowerBoundPos: lastWrittenPos.Next(),
					}
					m.redoProgressHeap.push(p)
				},
			}
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			case m.redoTaskChan <- t:
			}

			log.Debug("Generate redo task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Any("lowerBound", slowestTableProgress.nextLowerBoundPos),
				zap.Any("currentUpperBound", getUpperBound()))
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
func (m *SinkManager) StartTable(tableID model.TableID, startTs model.Ts) error {
	log.Info("Start table sink",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("startTs", startTs),
	)
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when starting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	backoffBaseDelayInMs := int64(100)
	totalRetryDuration := 10 * time.Second
	var replicateTs model.Ts
	err := retry.Do(m.ctx, func() error {
		phy, logic, err := m.up.PDClient.GetTS(m.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		replicateTs = oracle.ComposeTS(phy, logic)
		log.Debug("Set replicate ts",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("replicateTs", replicateTs),
		)
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithTotalRetryDuratoin(totalRetryDuration),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	if err != nil {
		return errors.Trace(err)
	}
	tableSink.(*tableSinkWrapper).start(startTs, replicateTs)
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
	return nil
}

// AsyncStopTable sets the table(TableSink) state to stopped.
func (m *SinkManager) AsyncStopTable(tableID model.TableID) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		log.Info("Async stop table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
		)
		tableSink, ok := m.tableSinks.Load(tableID)
		if !ok {
			log.Panic("Table sink not found when removing table",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableID))
		}
		tableSink.(*tableSinkWrapper).close(m.ctx)
		cleanedBytes := m.memQuota.clean(tableID)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("memory", cleanedBytes),
		)
		log.Info("Table sink closed asynchronously",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
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
func (m *SinkManager) GetTableStats(tableID model.TableID) TableStats {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when getting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	checkpointTs := tableSink.(*tableSinkWrapper).getCheckpointTs()
	m.memQuota.release(tableID, checkpointTs)
	resolvedMark := checkpointTs.ResolvedMark()
	cleanPos := engine.Position{
		StartTs:  resolvedMark - 1,
		CommitTs: resolvedMark,
	}
	gcEvent := &gcEvent{
		tableID:  tableID,
		cleanPos: cleanPos,
	}
	select {
	case m.engineGCChan <- gcEvent:
	default:
		log.Warn("Failed to send GC event to engine GC channel, engine GC channel is full",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Any("cleanPos", cleanPos))
	}
	var resolvedTs model.Ts
	// If redo log is enabled, we have to use redo log's resolved ts to calculate processor's min resolved ts.
	if m.redoManager != nil {
		resolvedTs = m.redoManager.GetResolvedTs(tableID)
	} else {
		resolvedTs = m.sourceManager.GetTableResolvedTs(tableID)
	}
	return TableStats{
		CheckpointTs:          resolvedMark,
		ResolvedTs:            resolvedTs,
		BarrierTs:             m.lastBarrierTs.Load(),
		ReceivedMaxCommitTs:   tableSink.(*tableSinkWrapper).getReceivedSorterCommitTs(),
		ReceivedMaxResolvedTs: tableSink.(*tableSinkWrapper).getReceivedSorterResolvedTs(),
	}
}

// ReceivedEvents returns the number of events received by all table sinks.
func (m *SinkManager) ReceivedEvents() int64 {
	totalReceivedEvents := int64(0)
	m.tableSinks.Range(func(_, value interface{}) bool {
		totalReceivedEvents += value.(*tableSinkWrapper).getReceivedEventCount()
		return true
	})
	return totalReceivedEvents
}

// Close closes all workers.
func (m *SinkManager) Close() error {
	log.Info("Closing sink manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID))
	start := time.Now()
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
		value.(*tableSinkWrapper).close(m.ctx)
		return true
	})
	log.Info("All table sinks closed",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
	m.wg.Wait()
	log.Info("Closed sink manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
	return nil
}
