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
	"github.com/pingcap/tiflow/cdc/processor/memquota"
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
	// engine.CleanByTable can be expensive. So it's necessary to reduce useless calls.
	cleanTableInterval  = 5 * time.Second
	cleanTableMinEvents = 128
)

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

	// used to generate task upperbounds.
	schemaStorage entry.SchemaStorage

	// sinkProgressHeap is the heap of the table progress for sink.
	sinkProgressHeap *tableProgresses
	// redoProgressHeap is the heap of the table progress for redo.
	redoProgressHeap *tableProgresses

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

	// sinkWorkers used to pull data from source manager.
	sinkWorkers []*sinkWorker
	// sinkTaskChan is used to send tasks to sinkWorkers.
	sinkTaskChan        chan *sinkTask
	sinkWorkerAvailable chan struct{}
	// sinkMemQuota is used to control the total memory usage of the table sink.
	sinkMemQuota *memquota.MemQuota

	// redoWorkers used to pull data from source manager.
	redoWorkers []*redoWorker
	// redoTaskChan is used to send tasks to redoWorkers.
	redoTaskChan        chan *redoTask
	redoWorkerAvailable chan struct{}
	// redoMemQuota is used to control the total memory usage of the redo.
	redoMemQuota *memquota.MemQuota

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
	schemaStorage entry.SchemaStorage,
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
		schemaStorage: schemaStorage,
		sinkFactory:   tableSinkFactory,
		sourceManager: sourceManager,
		errChan:       errChan,

		sinkProgressHeap:    newTableProgresses(),
		sinkWorkers:         make([]*sinkWorker, 0, sinkWorkerNum),
		sinkTaskChan:        make(chan *sinkTask),
		sinkWorkerAvailable: make(chan struct{}, 1),

		metricsTableSinkTotalRows: metricsTableSinkTotalRows,
	}

	if redoManager != nil && redoManager.Enabled() {
		m.redoManager = redoManager
		m.redoProgressHeap = newTableProgresses()
		m.redoWorkers = make([]*redoWorker, 0, redoWorkerNum)
		m.redoTaskChan = make(chan *redoTask)
		m.redoWorkerAvailable = make(chan struct{}, 1)

		// Use 3/4 memory quota as redo quota, and 1/2 again for redo cache.
		m.sinkMemQuota = memquota.NewMemQuota(changefeedID, changefeedInfo.Config.MemoryQuota/4*1, "sink")
		redoQuota := changefeedInfo.Config.MemoryQuota / 4 * 3
		m.redoMemQuota = memquota.NewMemQuota(changefeedID, redoQuota, "redo")
		m.eventCache = newRedoEventCache(changefeedID, redoQuota/2*1)
	} else {
		m.sinkMemQuota = memquota.NewMemQuota(changefeedID, changefeedInfo.Config.MemoryQuota, "sink")
		m.redoMemQuota = memquota.NewMemQuota(changefeedID, 0, "redo")
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
		w := newSinkWorker(m.changefeedID, m.sourceManager,
			m.sinkMemQuota, m.redoMemQuota,
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
				case <-m.ctx.Done():
				}
			}
		}()
	}

	if m.redoManager == nil {
		return
	}

	for i := 0; i < redoWorkerNum; i++ {
		w := newRedoWorker(m.changefeedID, m.sourceManager, m.redoMemQuota,
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
				case <-m.ctx.Done():
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
			case <-m.ctx.Done():
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
			case <-m.ctx.Done():
			}
		}
	}()
}

// backgroundGC is used to clean up the old data in the sorter.
func (m *SinkManager) backgroundGC() {
	ticker := time.NewTicker(time.Second)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				log.Info("Background GC is stooped because context is canceled",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID))
				return
			case <-ticker.C:
				tableSinks := make(map[model.TableID]*tableSinkWrapper)
				m.tableSinks.Range(func(key, value any) bool {
					tableID := key.(model.TableID)
					wrapper := value.(*tableSinkWrapper)
					tableSinks[tableID] = wrapper
					return true
				})

				for tableID, sink := range tableSinks {
					if time.Since(sink.lastCleanTime) < cleanTableInterval {
						continue
					}
					checkpointTs := sink.getCheckpointTs()
					resolvedMark := checkpointTs.ResolvedMark()
					if resolvedMark == 0 {
						continue
					}

					cleanPos := engine.Position{StartTs: resolvedMark - 1, CommitTs: resolvedMark}
					if !sink.cleanRangeEventCounts(cleanPos, cleanTableMinEvents) {
						continue
					}

					if err := m.sourceManager.CleanByTable(tableID, cleanPos); err != nil {
						log.Error("Failed to clean table in sort engine",
							zap.String("namespace", m.changefeedID.Namespace),
							zap.String("changefeed", m.changefeedID.ID),
							zap.Int64("tableID", tableID),
							zap.Error(err))
						select {
						case m.errChan <- err:
						case <-m.ctx.Done():
						}
					} else {
						log.Debug("table stale data has been cleaned",
							zap.String("namespace", m.changefeedID.Namespace),
							zap.String("changefeed", m.changefeedID.ID),
							zap.Int64("tableID", tableID),
							zap.Any("upperBound", cleanPos))
					}
					sink.lastCleanTime = time.Now()
				}
			}
		}
	}()
}

// generateSinkTasks generates tasks to fetch data from the source manager.
func (m *SinkManager) generateSinkTasks() error {
	// Task upperbound is limited by barrierTs and schemaResolvedTs.
	// But receivedSorterResolvedTs can be less than barrierTs, in which case
	// the table is just scheduled to this node.
	getUpperBound := func(tableSink *tableSinkWrapper) engine.Position {
		upperBoundTs := tableSink.getReceivedSorterResolvedTs()

		barrierTs := m.lastBarrierTs.Load()
		if upperBoundTs > barrierTs {
			upperBoundTs = barrierTs
		}

		// If a task carries events after schemaResolvedTs, mounter group threads
		// can be blocked on waiting schemaResolvedTs get advanced.
		schemaTs := m.schemaStorage.ResolvedTs()
		if upperBoundTs-1 > schemaTs {
			upperBoundTs = schemaTs + 1
		}

		return engine.Position{StartTs: upperBoundTs - 1, CommitTs: upperBoundTs}
	}

	dispatchTasks := func() error {
		tables := make([]*tableSinkWrapper, 0, sinkWorkerNum)
		progs := make([]*progress, 0, sinkWorkerNum)

		// Collect some table progresses.
		for len(tables) < sinkWorkerNum && m.sinkProgressHeap.len() > 0 {
			slowestTableProgress := m.sinkProgressHeap.pop()
			tableID := slowestTableProgress.tableID

			value, ok := m.tableSinks.Load(tableID)
			if !ok {
				log.Info("Table sink not found, probably already removed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableID))
				// Maybe the table sink is removed by the processor.(Scheduled the table to other nodes.)
				// So we do **not** need add it back to the heap.
				continue
			}
			tableSink := value.(*tableSinkWrapper)
			if tableSink.version != slowestTableProgress.version {
				// The progress maybe stale.
				continue
			}

			tableState := tableSink.getState()
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
			tables = append(tables, tableSink)
			progs = append(progs, slowestTableProgress)
		}

		i := 0
	LOOP:
		for ; i < len(tables); i++ {
			tableSink := tables[i]
			slowestTableProgress := progs[i]
			lowerBound := slowestTableProgress.nextLowerBoundPos
			upperBound := getUpperBound(tableSink)

			// The table has no available progress.
			if lowerBound.Compare(upperBound) >= 0 {
				m.sinkProgressHeap.push(slowestTableProgress)
				continue
			}

			// No available memory, skip this round directly.
			if !m.sinkMemQuota.TryAcquire(requestMemSize) {
				break LOOP
			}

			log.Debug("MemoryQuotaTracing: try acquire memory for table sink task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableSink.tableID),
				zap.Uint64("memory", requestMemSize))

			t := &sinkTask{
				tableID:       tableSink.tableID,
				lowerBound:    lowerBound,
				getUpperBound: getUpperBound,
				tableSink:     tableSink,
				callback: func(lastWrittenPos engine.Position) {
					p := &progress{
						tableID:           tableSink.tableID,
						nextLowerBoundPos: lastWrittenPos.Next(),
						version:           slowestTableProgress.version,
					}
					m.sinkProgressHeap.push(p)
					select {
					case m.sinkWorkerAvailable <- struct{}{}:
					default:
					}
				},
				isCanceled: func() bool {
					return tableSink.getState() != tablepb.TableStateReplicating
				},
			}
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			case m.sinkTaskChan <- t:
				log.Debug("Generate sink task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableSink.tableID),
					zap.Any("lowerBound", lowerBound),
					zap.Any("currentUpperBound", upperBound))
			default:
				m.sinkMemQuota.Refund(requestMemSize)
				log.Debug("MemoryQuotaTracing: refund memory for table sink task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableSink.tableID),
					zap.Uint64("memory", requestMemSize))
				break LOOP
			}
		}
		// Some progresses are not handled, return them back.
		for ; i < len(progs); i++ {
			m.sinkProgressHeap.push(progs[i])
		}
		return nil
	}

	taskTicker := time.NewTicker(defaultGenerateTaskInterval)
	defer taskTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-taskTicker.C:
			if err := dispatchTasks(); err != nil {
				return errors.Trace(err)
			}
		case <-m.sinkWorkerAvailable:
			if err := dispatchTasks(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (m *SinkManager) generateRedoTasks() error {
	// We use the table's resolved ts as the upper bound to fetch events.
	getUpperBound := func(tableSink *tableSinkWrapper) engine.Position {
		upperBoundTs := tableSink.getReceivedSorterResolvedTs()

		// If a task carries events after schemaResolvedTs, mounter group threads
		// can be blocked on waiting schemaResolvedTs get advanced.
		schemaTs := m.schemaStorage.ResolvedTs()
		if upperBoundTs-1 > schemaTs {
			upperBoundTs = schemaTs + 1
		}

		return engine.Position{StartTs: upperBoundTs - 1, CommitTs: upperBoundTs}
	}

	dispatchTasks := func() error {
		tables := make([]*tableSinkWrapper, 0, redoWorkerNum)
		progs := make([]*progress, 0, redoWorkerNum)

		for len(tables) < redoWorkerNum && m.redoProgressHeap.len() > 0 {
			slowestTableProgress := m.redoProgressHeap.pop()
			tableID := slowestTableProgress.tableID

			value, ok := m.tableSinks.Load(tableID)
			if !ok {
				log.Info("Table sink not found, probably already removed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableID))
				// Maybe the table sink is removed by the processor.(Scheduled the table to other nodes.)
				// So we do **not** need add it back to the heap.
				continue
			}
			tableSink := value.(*tableSinkWrapper)
			if tableSink.version != slowestTableProgress.version {
				// The progress maybe stale.
				continue
			}

			tableState := tableSink.getState()
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
			tables = append(tables, tableSink)
			progs = append(progs, slowestTableProgress)
		}

		i := 0
	LOOP:
		for ; i < len(tables); i++ {
			tableSink := tables[i]
			slowestTableProgress := progs[i]
			lowerBound := slowestTableProgress.nextLowerBoundPos
			upperBound := getUpperBound(tableSink)

			// The table has no available progress.
			if lowerBound.Compare(upperBound) >= 0 {
				m.redoProgressHeap.push(slowestTableProgress)
				continue
			}

			// No available memory, skip this round directly.
			if !m.redoMemQuota.TryAcquire(requestMemSize) {
				break LOOP
			}

			log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Int64("tableID", tableSink.tableID),
				zap.Uint64("memory", requestMemSize))

			t := &redoTask{
				tableID:       tableSink.tableID,
				lowerBound:    lowerBound,
				getUpperBound: getUpperBound,
				tableSink:     tableSink,
				callback: func(lastWrittenPos engine.Position) {
					p := &progress{
						tableID:           tableSink.tableID,
						nextLowerBoundPos: lastWrittenPos.Next(),
						version:           slowestTableProgress.version,
					}
					m.redoProgressHeap.push(p)
					select {
					case m.redoWorkerAvailable <- struct{}{}:
					default:
					}
				},
				isCanceled: func() bool {
					return tableSink.getState() != tablepb.TableStateReplicating
				},
			}
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			case m.redoTaskChan <- t:
				log.Debug("Generate redo task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableSink.tableID),
					zap.Any("lowerBound", lowerBound),
					zap.Any("currentUpperBound", upperBound),
					zap.Float64("lag", time.Since(oracle.GetTimeFromTS(upperBound.CommitTs)).Seconds()))
			default:
				m.redoMemQuota.Refund(requestMemSize)
				log.Debug("MemoryQuotaTracing: refund memory for redo log task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Int64("tableID", tableSink.tableID),
					zap.Uint64("memory", requestMemSize))
				break LOOP
			}
		}
		for ; i < len(progs); i++ {
			m.redoProgressHeap.push(progs[i])
		}
		return nil
	}

	taskTicker := time.NewTicker(defaultGenerateTaskInterval)
	defer taskTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-taskTicker.C:
			if err := dispatchTasks(); err != nil {
				return errors.Trace(err)
			}
		case <-m.redoWorkerAvailable:
			if err := dispatchTasks(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// UpdateReceivedSorterResolvedTs updates the received sorter resolved ts for the table.
// NOTE: it's still possible to be called during m.Close is in calling, so Close should
// take care of this.
func (m *SinkManager) UpdateReceivedSorterResolvedTs(tableID model.TableID, ts model.Ts) {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		// It's possible that the table is in removing.
		log.Debug("Table sink not found when updating resolved ts",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return
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
	_, loaded := m.tableSinks.LoadOrStore(tableID, sinkWrapper)
	if loaded {
		log.Panic("Add an exists table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
<<<<<<< HEAD
	m.sinkMemQuota.addTable(tableID)
	m.redoMemQuota.addTable(tableID)
=======
	m.sinkMemQuota.AddTable(span)
	m.redoMemQuota.AddTable(span)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179))
	log.Info("Add table sink",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("version", sinkWrapper.version))
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
		nextLowerBoundPos: engine.Position{StartTs: 0, CommitTs: startTs + 1},
		version:           tableSink.(*tableSinkWrapper).version,
	})
	if m.redoManager != nil {
		m.redoProgressHeap.push(&progress{
			tableID:           tableID,
			nextLowerBoundPos: engine.Position{StartTs: 0, CommitTs: startTs + 1},
			version:           tableSink.(*tableSinkWrapper).version,
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
<<<<<<< HEAD
		cleanedBytes := m.sinkMemQuota.clean(tableID)
		cleanedBytes += m.redoMemQuota.clean(tableID)
=======
		cleanedBytes := m.sinkMemQuota.Clean(span)
		cleanedBytes += m.redoMemQuota.Clean(span)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179))
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
	value, exists := m.tableSinks.LoadAndDelete(tableID)
	if !exists {
		log.Panic("Remove an unexist table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	sink := value.(*tableSinkWrapper)
	log.Info("Remove table sink successfully",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("checkpointTs", sink.getCheckpointTs().Ts))
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
	value, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Panic("Table sink not found when getting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	tableSink := value.(*tableSinkWrapper)

	checkpointTs := tableSink.getCheckpointTs()
<<<<<<< HEAD
	m.sinkMemQuota.release(tableID, checkpointTs)
	m.redoMemQuota.release(tableID, checkpointTs)

=======
	m.sinkMemQuota.Release(span, checkpointTs)
	m.redoMemQuota.Release(span, checkpointTs)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179))
	var resolvedTs model.Ts
	// If redo log is enabled, we have to use redo log's resolved ts to calculate processor's min resolved ts.
	if m.redoManager != nil {
		resolvedTs = m.redoManager.GetResolvedTs(tableID)
	} else {
		resolvedTs = tableSink.getReceivedSorterResolvedTs()
	}

	return TableStats{
		CheckpointTs:          checkpointTs.ResolvedMark(),
		ResolvedTs:            resolvedTs,
		BarrierTs:             m.lastBarrierTs.Load(),
		ReceivedMaxCommitTs:   tableSink.getReceivedSorterCommitTs(),
		ReceivedMaxResolvedTs: tableSink.getReceivedSorterResolvedTs(),
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
	m.sinkMemQuota.Close()
	m.redoMemQuota.Close()
	err := m.sinkFactory.Close()
	if err != nil {
		return errors.Trace(err)
	}
	m.tableSinks.Range(func(key, value interface{}) bool {
		sink := value.(*tableSinkWrapper)
		sink.close(m.ctx)
		if m.eventCache != nil {
			m.eventCache.removeTable(sink.tableID)
		}
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
