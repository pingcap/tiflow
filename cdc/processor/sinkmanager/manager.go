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
	"github.com/pingcap/tiflow/pkg/spanz"
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
	tableSinks spanz.SyncMap
	// lastBarrierTs is the last barrier ts.
	lastBarrierTs atomic.Uint64

	// sinkWorkers used to pull data from source manager.
	sinkWorkers []*sinkWorker
	// sinkTaskChan is used to send tasks to sinkWorkers.
	sinkTaskChan        chan *sinkTask
	sinkWorkerAvailable chan struct{}

	// redoWorkers used to pull data from source manager.
	redoWorkers []*redoWorker
	// redoTaskChan is used to send tasks to redoWorkers.
	redoTaskChan        chan *redoTask
	redoWorkerAvailable chan struct{}

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
		// Use 3/4 memory quota as redo event cache. A large value is helpful to cache hit ratio.
		m.eventCache = newRedoEventCache(changefeedID, changefeedInfo.Config.MemoryQuota/4*3)
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
				case <-m.ctx.Done():
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
				tableSinks := spanz.NewHashMap[*tableSinkWrapper]()
				m.tableSinks.Range(func(key tablepb.Span, value any) bool {
					wrapper := value.(*tableSinkWrapper)
					tableSinks.ReplaceOrInsert(key, wrapper)
					return true
				})

				tableSinks.Range(func(span tablepb.Span, sink *tableSinkWrapper) bool {
					if time.Since(sink.lastCleanTime) < cleanTableInterval {
						return true
					}
					checkpointTs := sink.getCheckpointTs()
					resolvedMark := checkpointTs.ResolvedMark()
					if resolvedMark == 0 {
						return true
					}

					cleanPos := engine.Position{StartTs: resolvedMark - 1, CommitTs: resolvedMark}
					if !sink.cleanRangeEventCounts(cleanPos, cleanTableMinEvents) {
						return true
					}

					if err := m.sourceManager.CleanByTable(span, cleanPos); err != nil {
						log.Error("Failed to clean table in sort engine",
							zap.String("namespace", m.changefeedID.Namespace),
							zap.String("changefeed", m.changefeedID.ID),
							zap.Stringer("span", &span),
							zap.Error(err))
						select {
						case m.errChan <- err:
						case <-m.ctx.Done():
						}
					} else {
						log.Debug("table stale data has been cleaned",
							zap.String("namespace", m.changefeedID.Namespace),
							zap.String("changefeed", m.changefeedID.ID),
							zap.Stringer("span", &span),
							zap.Any("upperBound", cleanPos))
					}
					sink.lastCleanTime = time.Now()
					return true
				})
			}
		}
	}()
}

// generateSinkTasks generates tasks to fetch data from the source manager.
func (m *SinkManager) generateSinkTasks() error {
	// We use the barrier ts as the upper bound of the fetch tableSinkTask.
	// Because it can not exceed the barrier ts.
	// We also need to consider the resolved ts from sorter,
	// Because if the redo log is enabled and the table just scheduled to this node,
	// the resolved ts from sorter may be smaller than the barrier ts.
	// So we use the min value of the barrier ts and the resolved ts from sorter.
	getUpperBound := func(tableSink *tableSinkWrapper) engine.Position {
		barrierTs := m.lastBarrierTs.Load()
		resolvedTs := tableSink.getReceivedSorterResolvedTs()
		var upperBoundTs model.Ts
		if resolvedTs > barrierTs {
			upperBoundTs = barrierTs
		} else {
			upperBoundTs = resolvedTs
		}
		return engine.Position{StartTs: upperBoundTs - 1, CommitTs: upperBoundTs}
	}

	dispatchTasks := func() error {
		tables := make([]*tableSinkWrapper, 0, sinkWorkerNum)
		progs := make([]*progress, 0, sinkWorkerNum)

		// Collect some table progresses.
		for len(tables) < sinkWorkerNum && m.sinkProgressHeap.len() > 0 {
			slowestTableProgress := m.sinkProgressHeap.pop()
			span := slowestTableProgress.span

			value, ok := m.tableSinks.Load(span)
			if !ok {
				log.Info("Table sink not found, probably already removed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Stringer("span", &span))
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
					zap.Stringer("span", &span),
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
			if !m.memQuota.tryAcquire(requestMemSize) {
				break LOOP
			}

			log.Debug("MemoryQuotaTracing: try acquire memory for table sink task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Stringer("span", &tableSink.span),
				zap.Uint64("memory", requestMemSize))

			t := &sinkTask{
				span:          tableSink.span,
				lowerBound:    lowerBound,
				getUpperBound: getUpperBound,
				tableSink:     tableSink,
				callback: func(lastWrittenPos engine.Position) {
					p := &progress{
						span:              tableSink.span,
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
					zap.Stringer("span", &tableSink.span),
					zap.Any("lowerBound", lowerBound),
					zap.Any("currentUpperBound", upperBound))
			default:
				m.memQuota.refund(requestMemSize)
				log.Debug("MemoryQuotaTracing: refund memory for table sink task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Stringer("span", &tableSink.span),
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
		return engine.Position{StartTs: upperBoundTs - 1, CommitTs: upperBoundTs}
	}

	dispatchTasks := func() error {
		tables := make([]*tableSinkWrapper, 0, redoWorkerNum)
		progs := make([]*progress, 0, redoWorkerNum)

		for len(tables) < redoWorkerNum && m.redoProgressHeap.len() > 0 {
			slowestTableProgress := m.redoProgressHeap.pop()
			span := slowestTableProgress.span

			value, ok := m.tableSinks.Load(span)
			if !ok {
				log.Info("Table sink not found, probably already removed",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Stringer("span", &span))
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
					zap.Stringer("span", &span),
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
			if !m.memQuota.tryAcquire(requestMemSize) {
				break LOOP
			}

			log.Debug("MemoryQuotaTracing: try acquire memory for redo log task",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Stringer("span", &tableSink.span),
				zap.Uint64("memory", requestMemSize))

			t := &redoTask{
				span:          tableSink.span,
				lowerBound:    lowerBound,
				getUpperBound: getUpperBound,
				tableSink:     tableSink,
				callback: func(lastWrittenPos engine.Position) {
					p := &progress{
						span:              tableSink.span,
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
					zap.Stringer("span", &tableSink.span),
					zap.Any("lowerBound", lowerBound),
					zap.Any("currentUpperBound", upperBound))
			default:
				m.memQuota.refund(requestMemSize)
				log.Debug("MemoryQuotaTracing: refund memory for redo log task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Stringer("span", &tableSink.span),
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
func (m *SinkManager) UpdateReceivedSorterResolvedTs(span tablepb.Span, ts model.Ts) {
	tableSink, ok := m.tableSinks.Load(span)
	if !ok {
		// It's possible that the table is in removing.
		log.Debug("Table sink not found when updating resolved ts",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
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
func (m *SinkManager) AddTable(span tablepb.Span, startTs model.Ts, targetTs model.Ts) {
	sinkWrapper := newTableSinkWrapper(
		m.changefeedID,
		span,
		m.sinkFactory.CreateTableSink(m.changefeedID, span, m.metricsTableSinkTotalRows),
		tablepb.TableStatePreparing,
		startTs,
		targetTs,
	)
	_, loaded := m.tableSinks.LoadOrStore(span, sinkWrapper)
	if loaded {
		log.Panic("Add an exists table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
		return
	}
	m.memQuota.addTable(span)
	log.Info("Add table sink",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Stringer("span", &span),
		zap.Uint64("startTs", startTs),
		zap.Uint64("version", sinkWrapper.version))
}

// StartTable sets the table(TableSink) state to replicating.
func (m *SinkManager) StartTable(span tablepb.Span, startTs model.Ts) error {
	log.Info("Start table sink",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Stringer("span", &span),
		zap.Uint64("startTs", startTs),
	)
	tableSink, ok := m.tableSinks.Load(span)
	if !ok {
		log.Panic("Table sink not found when starting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
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
			zap.Stringer("span", &span),
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
		span:              span,
		nextLowerBoundPos: engine.Position{StartTs: 0, CommitTs: startTs + 1},
		version:           tableSink.(*tableSinkWrapper).version,
	})
	if m.redoManager != nil {
		m.redoProgressHeap.push(&progress{
			span:              span,
			nextLowerBoundPos: engine.Position{StartTs: 0, CommitTs: startTs + 1},
			version:           tableSink.(*tableSinkWrapper).version,
		})
	}
	return nil
}

// AsyncStopTable sets the table(TableSink) state to stopped.
func (m *SinkManager) AsyncStopTable(span tablepb.Span) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		log.Info("Async stop table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span),
		)
		tableSink, ok := m.tableSinks.Load(span)
		if !ok {
			log.Panic("Table sink not found when removing table",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Stringer("span", &span))
		}
		tableSink.(*tableSinkWrapper).close(m.ctx)
		cleanedBytes := m.memQuota.clean(span)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("memory", cleanedBytes),
		)
		log.Info("Table sink closed asynchronously",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span),
		)
	}()
}

// RemoveTable removes a table(TableSink) from the sink manager.
func (m *SinkManager) RemoveTable(span tablepb.Span) {
	// NOTICE: It is safe to only remove the table sink from the map.
	// Because if we found the table sink is closed, we will not add it back to the heap.
	// Also, no need to GC the SortEngine. Because the SortEngine also removes this table.
	value, exists := m.tableSinks.LoadAndDelete(span)
	if !exists {
		log.Panic("Remove an unexist table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
	}
	sink := value.(*tableSinkWrapper)
	log.Info("Remove table sink successfully",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Stringer("span", &span),
		zap.Uint64("checkpointTs", sink.getCheckpointTs().Ts))
	if m.eventCache != nil {
		m.eventCache.removeTable(span)
	}
}

// GetAllCurrentTableSpans returns all spans in the sink manager.
func (m *SinkManager) GetAllCurrentTableSpans() []tablepb.Span {
	var spans []tablepb.Span
	m.tableSinks.Range(func(key tablepb.Span, value interface{}) bool {
		spans = append(spans, key)
		return true
	})
	return spans
}

// GetTableState returns the table(TableSink) state.
func (m *SinkManager) GetTableState(span tablepb.Span) (tablepb.TableState, bool) {
	tableSink, ok := m.tableSinks.Load(span)
	if !ok {
		log.Debug("Table sink not found when getting table state",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
		return tablepb.TableStateAbsent, false
	}
	return tableSink.(*tableSinkWrapper).getState(), true
}

// GetTableStats returns the state of the table.
func (m *SinkManager) GetTableStats(span tablepb.Span) TableStats {
	value, ok := m.tableSinks.Load(span)
	if !ok {
		log.Panic("Table sink not found when getting table stats",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
	}
	tableSink := value.(*tableSinkWrapper)

	checkpointTs := tableSink.getCheckpointTs()
	m.memQuota.release(span, checkpointTs)
	var resolvedTs model.Ts
	// If redo log is enabled, we have to use redo log's resolved ts to calculate processor's min resolved ts.
	if m.redoManager != nil {
		resolvedTs = m.redoManager.GetResolvedTs(span)
	} else {
		resolvedTs = m.sourceManager.GetTableResolvedTs(span)
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
	m.tableSinks.Range(func(_ tablepb.Span, value interface{}) bool {
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
	m.tableSinks.Range(func(_ tablepb.Span, value interface{}) bool {
		sink := value.(*tableSinkWrapper)
		sink.close(m.ctx)
		if m.eventCache != nil {
			m.eventCache.removeTable(sink.span)
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
