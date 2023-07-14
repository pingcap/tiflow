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
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

	changefeedInfo *model.ChangeFeedInfo

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
	// redoDMLMgr is used to report the resolved ts of the table if redo log is enabled.
	redoDMLMgr redo.DMLManager
	// sourceManager is used by the sink manager to fetch data.
	sourceManager *sourcemanager.SourceManager

	// sinkFactory used to create table sink.
	sinkFactory   *factory.SinkFactory
	sinkFactoryMu sync.Mutex

	// tableSinks is a map from tableID to tableSink.
	tableSinks sync.Map

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

	// To control lifetime of all sub-goroutines.
	managerCtx    context.Context
	managerCancel context.CancelFunc
	ready         chan struct{}

	// To control lifetime of sink and redo tasks.
	sinkEg *errgroup.Group
	redoEg *errgroup.Group

	// wg is used to wait for all workers to exit.
	wg sync.WaitGroup

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
	redoDMLMgr redo.DMLManager,
	sourceManager *sourcemanager.SourceManager,
	errChan chan error,
	warnChan chan error,
	metricsTableSinkTotalRows prometheus.Counter,
) (*SinkManager, error) {
	m := &SinkManager{
		changefeedID:   changefeedID,
		changefeedInfo: changefeedInfo,
		up:             up,
		schemaStorage:  schemaStorage,
		sourceManager:  sourceManager,

		sinkProgressHeap:    newTableProgresses(),
		sinkWorkers:         make([]*sinkWorker, 0, sinkWorkerNum),
		sinkTaskChan:        make(chan *sinkTask),
		sinkWorkerAvailable: make(chan struct{}, 1),

		metricsTableSinkTotalRows: metricsTableSinkTotalRows,
	}

	if redoDMLMgr != nil && redoDMLMgr.Enabled() {
		m.redoDMLMgr = redoDMLMgr
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

	m.ready = make(chan struct{})
	m.wg.Add(1) // So `SinkManager.Close` will also wait the subroutine.
	go func() {
		if err := m.run(ctx, warnChan); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
			case errChan <- err:
			}
		}
	}()
	<-m.ready
	return m, nil
}

func (m *SinkManager) run(ctx context.Context, warnings ...chan<- error) (err error) {
	m.managerCtx, m.managerCancel = context.WithCancel(ctx)
	defer func() {
		m.wg.Done()
		m.waitSubroutines()
		log.Info("Sink manager exists",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Error(err))
	}()

	splitTxn := m.changefeedInfo.Config.Sink.TxnAtomicity.ShouldSplitTxn()
	enableOldValue := m.changefeedInfo.Config.EnableOldValue

	gcErrors := make(chan error, 16)
	sinkFactoryErrors := make(chan error, 16)
	sinkErrors := make(chan error, 16)
	redoErrors := make(chan error, 16)

	m.backgroundGC(gcErrors)
	if m.sinkEg == nil {
		var sinkCtx context.Context
		m.sinkEg, sinkCtx = errgroup.WithContext(m.managerCtx)
		m.startSinkWorkers(sinkCtx, m.sinkEg, splitTxn, enableOldValue)
		m.sinkEg.Go(func() error { return m.generateSinkTasks(sinkCtx) })
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if err := m.sinkEg.Wait(); err != nil && !cerror.Is(err, context.Canceled) {
				log.Error("Worker handles or generates sink task failed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Error(err))
				select {
				case sinkErrors <- err:
				case <-m.managerCtx.Done():
				}
			}
		}()
	}
	if m.redoDMLMgr != nil && m.redoEg == nil {
		var redoCtx context.Context
		m.redoEg, redoCtx = errgroup.WithContext(m.managerCtx)
		m.startRedoWorkers(redoCtx, m.redoEg, splitTxn, enableOldValue)
		m.redoEg.Go(func() error { return m.generateRedoTasks(redoCtx) })
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if err := m.redoEg.Wait(); err != nil && !cerror.Is(err, context.Canceled) {
				log.Error("Worker handles or generates redo task failed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Error(err))
				select {
				case redoErrors <- err:
				case <-m.managerCtx.Done():
				}
			}
		}()
	}

	close(m.ready)
	log.Info("Sink manager is created",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Bool("withRedoEnabled", m.redoDMLMgr != nil))

	// SinkManager will restart some internal modules if necessasry.
	for {
		if err := m.initSinkFactory(sinkFactoryErrors); err != nil {
			select {
			case <-m.managerCtx.Done():
			case sinkFactoryErrors <- err:
			}
		}

		select {
		case <-m.managerCtx.Done():
			return errors.Trace(m.managerCtx.Err())
		case err = <-gcErrors:
			return errors.Trace(err)
		case err = <-sinkErrors:
			return errors.Trace(err)
		case err = <-redoErrors:
			return errors.Trace(err)
		case err = <-sinkFactoryErrors:
			log.Warn("Sink manager backend sink fails",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Error(err))
			m.clearSinkFactory()
			sinkFactoryErrors = make(chan error, 16)
		}

		if !cerror.IsChangefeedUnRetryableError(err) && errors.Cause(err) != context.Canceled {
			select {
			case <-m.managerCtx.Done():
			case warnings[0] <- err:
			}
		} else {
			return errors.Trace(err)
		}
		// Use a 5 second backoff when re-establishing internal resources.
		if err = util.Hang(m.managerCtx, 5*time.Second); err != nil {
			return errors.Trace(err)
		}
	}
}

func (m *SinkManager) initSinkFactory(errCh chan error) error {
	m.sinkFactoryMu.Lock()
	defer m.sinkFactoryMu.Unlock()
	if m.sinkFactory != nil {
		return nil
	}
	uri := m.changefeedInfo.SinkURI
	cfg := m.changefeedInfo.Config

	var err error = nil
	failpoint.Inject("SinkManagerRunError", func() {
		log.Info("failpoint SinkManagerRunError injected", zap.String("changefeed", m.changefeedID.ID))
		err = errors.New("SinkManagerRunError")
	})
	if err != nil {
		return errors.Trace(err)
	}

	if m.sinkFactory, err = factory.New(m.managerCtx, uri, cfg, errCh); err == nil {
		log.Info("Sink manager inits sink factory success",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID))
		return nil
	}
	return errors.Trace(err)
}

func (m *SinkManager) clearSinkFactory() {
	m.sinkFactoryMu.Lock()
	defer m.sinkFactoryMu.Unlock()
	if m.sinkFactory != nil {
		log.Info("Sink manager closing sink factory",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID))
		m.sinkFactory.Close()
		m.sinkFactory = nil
		log.Info("Sink manager has closed sink factory",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID))
	}
}

func (m *SinkManager) startSinkWorkers(ctx context.Context, eg *errgroup.Group, splitTxn bool, enableOldValue bool) {
	for i := 0; i < sinkWorkerNum; i++ {
		w := newSinkWorker(m.changefeedID, m.sourceManager,
			m.sinkMemQuota, m.redoMemQuota,
			m.eventCache, splitTxn, enableOldValue)
		m.sinkWorkers = append(m.sinkWorkers, w)
		eg.Go(func() error { return w.handleTasks(ctx, m.sinkTaskChan) })
	}
}

func (m *SinkManager) startRedoWorkers(ctx context.Context, eg *errgroup.Group, splitTxn bool, enableOldValue bool) {
	for i := 0; i < redoWorkerNum; i++ {
		w := newRedoWorker(m.changefeedID, m.sourceManager, m.redoMemQuota,
			m.redoDMLMgr, m.eventCache, splitTxn, enableOldValue)
		m.redoWorkers = append(m.redoWorkers, w)
		eg.Go(func() error { return w.handleTasks(ctx, m.redoTaskChan) })
	}
}

// backgroundGC is used to clean up the old data in the sorter.
func (m *SinkManager) backgroundGC(errors chan<- error) {
	ticker := time.NewTicker(time.Second)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-m.managerCtx.Done():
				log.Info("Background GC is stoped because context is canceled",
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
						case errors <- err:
						case <-m.managerCtx.Done():
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
func (m *SinkManager) generateSinkTasks(ctx context.Context) error {
	// Task upperbound is limited by barrierTs and schemaResolvedTs.
	// But receivedSorterResolvedTs can be less than barrierTs, in which case
	// the table is just scheduled to this node.
	getUpperBound := func(
		tableSinkUpperBoundTs model.Ts,
	) engine.Position {
		schemaTs := m.schemaStorage.ResolvedTs()
		if schemaTs != math.MaxUint64 && tableSinkUpperBoundTs > schemaTs+1 {
			// schemaTs == math.MaxUint64 means it's in tests.
			tableSinkUpperBoundTs = schemaTs + 1
		}
		return engine.Position{StartTs: tableSinkUpperBoundTs - 1, CommitTs: tableSinkUpperBoundTs}
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
			upperBound := getUpperBound(tableSink.getUpperBoundTs())
			// The table has no available progress.
			if lowerBound.Compare(upperBound) >= 0 {
				m.sinkProgressHeap.push(slowestTableProgress)
				continue
			}
			// The table hasn't been attached to a sink.
			if !tableSink.initTableSink() {
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
			case <-ctx.Done():
				return ctx.Err()
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
		case <-ctx.Done():
			return ctx.Err()
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

func (m *SinkManager) generateRedoTasks(ctx context.Context) error {
	// We use the table's resolved ts as the upper bound to fetch events.
	getUpperBound := func(tableSinkUpperBoundTs model.Ts) engine.Position {
		// If a task carries events after schemaResolvedTs, mounter group threads
		// can be blocked on waiting schemaResolvedTs get advanced.
		schemaTs := m.schemaStorage.ResolvedTs()
		if tableSinkUpperBoundTs > schemaTs+1 {
			tableSinkUpperBoundTs = schemaTs + 1
		}

		return engine.Position{StartTs: tableSinkUpperBoundTs - 1, CommitTs: tableSinkUpperBoundTs}
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
			upperBound := getUpperBound(tableSink.getReceivedSorterResolvedTs())

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
			case <-ctx.Done():
				return ctx.Err()
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
		case <-ctx.Done():
			return ctx.Err()
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

// UpdateBarrierTs update all tableSink's barrierTs in the SinkManager
func (m *SinkManager) UpdateBarrierTs(
	globalBarrierTs model.Ts,
	tableBarrier map[model.TableID]model.Ts,
) {
	m.tableSinks.Range(func(tableID, value interface{}) bool {
		tableSink := value.(*tableSinkWrapper)
		lastBarrierTs := tableSink.barrierTs.Load()
		// It is safe to do not use compare and swap here.
		// Only the processor will update the barrier ts.
		// Other goroutines will only read the barrier ts.
		// So it is safe to do not use compare and swap here, just Load and Store.
		if tableBarrierTs, ok := tableBarrier[tableSink.tableID]; ok {
			barrierTs := tableBarrierTs
			if barrierTs > globalBarrierTs {
				barrierTs = globalBarrierTs
			}
			if barrierTs > lastBarrierTs {
				tableSink.barrierTs.Store(barrierTs)
			}
		} else {
			if globalBarrierTs > lastBarrierTs {
				tableSink.barrierTs.Store(globalBarrierTs)
			}
		}
		return true
	})
}

// AddTable adds a table(TableSink) to the sink manager.
func (m *SinkManager) AddTable(tableID model.TableID, startTs model.Ts, targetTs model.Ts) {
	sinkWrapper := newTableSinkWrapper(
		m.changefeedID,
		tableID,
		func() tablesink.TableSink {
			if m.sinkFactoryMu.TryLock() {
				defer m.sinkFactoryMu.Unlock()
				if m.sinkFactory != nil {
					return m.sinkFactory.CreateTableSink(m.changefeedID, tableID, startTs, m.metricsTableSinkTotalRows)
				}
			}
			return nil
		},
		tablepb.TableStatePreparing,
		startTs,
		targetTs,
		func(ctx context.Context) (model.Ts, error) {
			return genReplicateTs(ctx, m.up.PDClient)
		},
	)

	_, loaded := m.tableSinks.LoadOrStore(tableID, sinkWrapper)
	if loaded {
		log.Panic("Add an exists table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
	m.sinkMemQuota.AddTable(tableID)
	m.redoMemQuota.AddTable(tableID)
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

	if err := tableSink.(*tableSinkWrapper).start(m.managerCtx, startTs); err != nil {
		return err
	}

	m.sinkProgressHeap.push(&progress{
		tableID:           tableID,
		nextLowerBoundPos: engine.Position{StartTs: 0, CommitTs: startTs + 1},
		version:           tableSink.(*tableSinkWrapper).version,
	})
	if m.redoDMLMgr != nil {
		m.redoProgressHeap.push(&progress{
			tableID:           tableID,
			nextLowerBoundPos: engine.Position{StartTs: 0, CommitTs: startTs + 1},
			version:           tableSink.(*tableSinkWrapper).version,
		})
	}
	return nil
}

// AsyncStopTable sets the table(TableSink) state to stopped.
func (m *SinkManager) AsyncStopTable(tableID model.TableID) bool {
	tableSink, ok := m.tableSinks.Load(tableID)
	if !ok {
		// Just warn, because the table sink may be removed by another goroutine.
		// This logic is the same as this function's caller.
		log.Warn("Table sink not found when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}
	if tableSink.(*tableSinkWrapper).asyncClose() {
		cleanedBytes := m.sinkMemQuota.RemoveTable(tableID)
		cleanedBytes += m.redoMemQuota.RemoveTable(tableID)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("memory", cleanedBytes))
		return true
	}
	return false
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

// GetAllTableCount returns the number of tables in the sink manager.
func (m *SinkManager) GetAllTableCount() (count int) {
	m.tableSinks.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return
}

// GetTableState returns the table(TableSink) state.
func (m *SinkManager) GetTableState(tableID model.TableID) (tablepb.TableState, bool) {
	wrapper, ok := m.tableSinks.Load(tableID)
	if !ok {
		log.Debug("Table sink not found when getting table state",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return tablepb.TableStateAbsent, false
	}

	// NOTE(qupeng): I'm not sure whether `SinkManager.AsyncStopTable` will be called
	// again or not if it returns false. So we must retry `tableSink.asyncClose` here
	// if necessary. It's better to remove the dirty logic in the future.
	tableSink := wrapper.(*tableSinkWrapper)
	if tableSink.getState() == tablepb.TableStateStopping && tableSink.asyncClose() {
		cleanedBytes := m.sinkMemQuota.RemoveTable(tableID)
		cleanedBytes += m.redoMemQuota.RemoveTable(tableID)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("memory", cleanedBytes))
	}
	return tableSink.getState(), true
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
	m.sinkMemQuota.Release(tableID, checkpointTs)
	m.redoMemQuota.Release(tableID, checkpointTs)

	var resolvedTs model.Ts
	// If redo log is enabled, we have to use redo log's resolved ts to calculate processor's min resolved ts.
	if m.redoDMLMgr != nil {
		resolvedTs = m.redoDMLMgr.GetResolvedTs(tableID)
	} else {
		resolvedTs = tableSink.getReceivedSorterResolvedTs()
	}

	return TableStats{
		CheckpointTs:          checkpointTs.ResolvedMark(),
		ResolvedTs:            resolvedTs,
		BarrierTs:             tableSink.barrierTs.Load(),
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

// wait all sub-routines associated with `m.wg` returned.
func (m *SinkManager) waitSubroutines() {
	m.managerCancel()
	// Sink workers and redo workers can be blocked on MemQuota.BlockAcquire,
	// which doesn't watch m.managerCtx. So we must close these 2 MemQuotas
	// before wait them.
	m.sinkMemQuota.Close()
	m.redoMemQuota.Close()
	m.wg.Wait()
}

// Close closes the manager. Must be called after `Run` returned.
func (m *SinkManager) Close() {
	log.Info("Closing sink manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID))
	start := time.Now()
	m.waitSubroutines()
	m.clearSinkFactory()

	log.Info("Closed sink manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
}
