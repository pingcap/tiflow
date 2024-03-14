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
	"fmt"
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
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	tablesinkmetrics "github.com/pingcap/tiflow/cdc/sink/metrics/tablesink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/spanz"
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
	// sorter.CleanByTable can be expensive. So it's necessary to reduce useless calls.
	cleanTableInterval  = 5 * time.Second
	cleanTableMinEvents = 128
)

// TableStats of a table sink.
type TableStats struct {
	CheckpointTs model.Ts
	ResolvedTs   model.Ts
	LastSyncedTs model.Ts
	BarrierTs    model.Ts
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

	// redoDMLMgr is used to report the resolved ts of the table if redo log is enabled.
	redoDMLMgr redo.DMLManager
	// sourceManager is used by the sink manager to fetch data.
	sourceManager *sourcemanager.SourceManager

	// sinkFactory used to create table sink.
	sinkFactory struct {
		sync.Mutex
		f *factory.SinkFactory
		// When every time we want to create a new factory, version will be increased and
		// errors will be replaced by a new channel. version is used to distinct different
		// sink factories in table sinks.
		version uint64
		errors  chan error
	}

	// tableSinks is a map from tableID to tableSink.
	tableSinks spanz.SyncMap

	// sinkWorkers used to pull data from source manager.
	sinkWorkers []*sinkWorker
	// sinkTaskChan is used to send tasks to sinkWorkers.
	sinkTaskChan        chan *sinkTask
	sinkWorkerAvailable chan struct{}
	// sinkMemQuota is used to control the total memory usage of the table sink.
	sinkMemQuota *memquota.MemQuota
	sinkRetry    *retry.ErrorRetry
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

	metricsTableSinkFlushLagDuration prometheus.Observer
}

// New creates a new sink manager.
func New(
	changefeedID model.ChangeFeedID,
	changefeedInfo *model.ChangeFeedInfo,
	up *upstream.Upstream,
	schemaStorage entry.SchemaStorage,
	redoDMLMgr redo.DMLManager,
	sourceManager *sourcemanager.SourceManager,
) *SinkManager {
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
		sinkRetry:           retry.NewInfiniteErrorRetry(),

		metricsTableSinkTotalRows: tablesinkmetrics.TotalRowsCountCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),

		metricsTableSinkFlushLagDuration: tablesinkmetrics.TableSinkFlushLagDuration.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}

	totalQuota := changefeedInfo.Config.MemoryQuota
	if redoDMLMgr != nil && redoDMLMgr.Enabled() {
		m.redoDMLMgr = redoDMLMgr
		m.redoProgressHeap = newTableProgresses()
		m.redoWorkers = make([]*redoWorker, 0, redoWorkerNum)
		m.redoTaskChan = make(chan *redoTask)
		m.redoWorkerAvailable = make(chan struct{}, 1)

		consistentMemoryUsage := changefeedInfo.Config.Consistent.MemoryUsage
		if consistentMemoryUsage == nil {
			consistentMemoryUsage = config.GetDefaultReplicaConfig().Consistent.MemoryUsage
		}

		redoQuota := totalQuota * consistentMemoryUsage.MemoryQuotaPercentage / 100
		sinkQuota := totalQuota - redoQuota
		m.sinkMemQuota = memquota.NewMemQuota(changefeedID, sinkQuota, "sink")
		m.redoMemQuota = memquota.NewMemQuota(changefeedID, redoQuota, "redo")
	} else {
		m.sinkMemQuota = memquota.NewMemQuota(changefeedID, totalQuota, "sink")
		m.redoMemQuota = memquota.NewMemQuota(changefeedID, 0, "redo")
	}

	m.ready = make(chan struct{})
	return m
}

// Run implements util.Runnable.
// When it returns, all sub-goroutines should be closed.
func (m *SinkManager) Run(ctx context.Context, warnings ...chan<- error) (err error) {
	m.managerCtx, m.managerCancel = context.WithCancel(ctx)
	m.wg.Add(1) // So `SinkManager.Close` will also wait the function.
	defer func() {
		m.wg.Done()
		m.waitSubroutines()
		log.Info("Sink manager exists",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Error(err))
	}()

	splitTxn := util.GetOrZero(m.changefeedInfo.Config.Sink.TxnAtomicity).ShouldSplitTxn()

	gcErrors := make(chan error, 16)
	sinkErrors := make(chan error, 16)
	redoErrors := make(chan error, 16)

	m.backgroundGC(gcErrors)
	if m.sinkEg == nil {
		var sinkCtx context.Context
		m.sinkEg, sinkCtx = errgroup.WithContext(m.managerCtx)
		m.startSinkWorkers(sinkCtx, m.sinkEg, splitTxn)
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
		m.startRedoWorkers(redoCtx, m.redoEg)
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
		sinkFactoryErrors, sinkFactoryVersion := m.initSinkFactory()

		select {
		case <-m.managerCtx.Done():
			return m.managerCtx.Err()
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
				zap.Uint64("factoryVersion", sinkFactoryVersion),
				zap.Error(err))
			m.clearSinkFactory()

			// To release memory quota ASAP, close all table sinks manually.
			start := time.Now()
			log.Info("Sink manager is closing all table sinks",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID))
			m.tableSinks.Range(func(span tablepb.Span, value interface{}) bool {
				value.(*tableSinkWrapper).closeTableSink()
				m.sinkMemQuota.ClearTable(span)
				return true
			})
			log.Info("Sink manager has closed all table sinks",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Duration("cost", time.Since(start)))
		}

		// If the error is retryable, we should retry to re-establish the internal resources.
		if !cerror.ShouldFailChangefeed(err) && errors.Cause(err) != context.Canceled {
			select {
			case <-m.managerCtx.Done():
			case warnings[0] <- err:
			}
		} else {
			return errors.Trace(err)
		}

		backoff, err := m.sinkRetry.GetRetryBackoff(err)
		if err != nil {
			return errors.New(fmt.Sprintf("GetRetryBackoff: %s", err.Error()))
		}

		if err = util.Hang(m.managerCtx, backoff); err != nil {
			return errors.Trace(err)
		}
	}
}

func (m *SinkManager) needsStuckCheck() bool {
	m.sinkFactory.Lock()
	defer m.sinkFactory.Unlock()
	return m.sinkFactory.f != nil && m.sinkFactory.f.Category() == factory.CategoryMQ
}

func (m *SinkManager) initSinkFactory() (chan error, uint64) {
	m.sinkFactory.Lock()
	defer m.sinkFactory.Unlock()
	uri := m.changefeedInfo.SinkURI
	cfg := m.changefeedInfo.Config

	if m.sinkFactory.f != nil {
		return m.sinkFactory.errors, m.sinkFactory.version
	}
	if m.sinkFactory.errors == nil {
		m.sinkFactory.errors = make(chan error, 16)
		m.sinkFactory.version += 1
	}

	emitError := func(err error) {
		select {
		case <-m.managerCtx.Done():
		case m.sinkFactory.errors <- err:
		}
	}

	var err error = nil
	failpoint.Inject("SinkManagerRunError", func() {
		log.Info("failpoint SinkManagerRunError injected", zap.String("changefeed", m.changefeedID.ID))
		err = errors.New("SinkManagerRunError")
	})
	if err != nil {
		emitError(err)
		return m.sinkFactory.errors, m.sinkFactory.version
	}

	m.sinkFactory.f, err = factory.New(m.managerCtx, m.changefeedID, uri, cfg, m.sinkFactory.errors, m.up.PDClock)
	if err != nil {
		emitError(err)
		return m.sinkFactory.errors, m.sinkFactory.version
	}

	log.Info("Sink manager inits sink factory success",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Uint64("factoryVersion", m.sinkFactory.version))
	return m.sinkFactory.errors, m.sinkFactory.version
}

func (m *SinkManager) clearSinkFactory() {
	m.sinkFactory.Lock()
	defer m.sinkFactory.Unlock()
	if m.sinkFactory.f != nil {
		log.Info("Sink manager closing sink factory",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Uint64("factoryVersion", m.sinkFactory.version))
		m.sinkFactory.f.Close()
		m.sinkFactory.f = nil
		log.Info("Sink manager has closed sink factory",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Uint64("factoryVersion", m.sinkFactory.version))
	}
	if m.sinkFactory.errors != nil {
		close(m.sinkFactory.errors)
		for range m.sinkFactory.errors {
		}
		m.sinkFactory.errors = nil
	}
}

func (m *SinkManager) putSinkFactoryError(err error, version uint64) (success bool) {
	m.sinkFactory.Lock()
	defer m.sinkFactory.Unlock()
	if version == m.sinkFactory.version {
		select {
		case m.sinkFactory.errors <- err:
		default:
		}
		return true
	}
	return false
}

func (m *SinkManager) startSinkWorkers(ctx context.Context, eg *errgroup.Group, splitTxn bool) {
	for i := 0; i < sinkWorkerNum; i++ {
		w := newSinkWorker(m.changefeedID, m.sourceManager,
			m.sinkMemQuota, splitTxn)
		m.sinkWorkers = append(m.sinkWorkers, w)
		eg.Go(func() error { return w.handleTasks(ctx, m.sinkTaskChan) })
	}
}

func (m *SinkManager) startRedoWorkers(ctx context.Context, eg *errgroup.Group) {
	for i := 0; i < redoWorkerNum; i++ {
		w := newRedoWorker(m.changefeedID, m.sourceManager, m.redoMemQuota,
			m.redoDMLMgr)
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
				log.Info("Background GC is stopped because context is canceled",
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

					cleanPos := sorter.Position{StartTs: resolvedMark - 1, CommitTs: resolvedMark}
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
						case errors <- err:
						case <-m.managerCtx.Done():
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

func (m *SinkManager) getUpperBound(tableSinkUpperBoundTs model.Ts) sorter.Position {
	schemaTs := m.schemaStorage.ResolvedTs()
	if schemaTs != math.MaxUint64 && tableSinkUpperBoundTs > schemaTs+1 {
		// schemaTs == math.MaxUint64 means it's in tests.
		tableSinkUpperBoundTs = schemaTs + 1
	}
	return sorter.Position{StartTs: tableSinkUpperBoundTs - 1, CommitTs: tableSinkUpperBoundTs}
}

// generateSinkTasks generates tasks to fetch data from the source manager.
func (m *SinkManager) generateSinkTasks(ctx context.Context) error {
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
			upperBound := m.getUpperBound(tableSink.getUpperBoundTs())

			if !tableSink.initTableSink() {
				// The table hasn't been attached to a sink.
				m.sinkProgressHeap.push(slowestTableProgress)
				continue
			}

			if sinkErr := tableSink.checkTableSinkHealth(); sinkErr != nil {
				switch errors.Cause(sinkErr).(type) {
				case tablesink.SinkInternalError:
					tableSink.closeAndClearTableSink()
					if restartErr := tableSink.restart(ctx); restartErr == nil {
						// Restart the table sink based on the checkpoint position.
						ckpt := tableSink.getCheckpointTs().ResolvedMark()
						lastWrittenPos := sorter.Position{StartTs: ckpt - 1, CommitTs: ckpt}
						p := &progress{
							span:              tableSink.span,
							nextLowerBoundPos: lastWrittenPos.Next(),
							version:           slowestTableProgress.version,
						}
						m.sinkProgressHeap.push(p)
						log.Info("table sink has been restarted",
							zap.String("namespace", m.changefeedID.Namespace),
							zap.String("changefeed", m.changefeedID.ID),
							zap.Stringer("span", &tableSink.span),
							zap.Any("lastWrittenPos", lastWrittenPos),
							zap.String("sinkError", sinkErr.Error()))
					} else {
						m.sinkProgressHeap.push(slowestTableProgress)
						log.Warn("table sink restart fail",
							zap.String("namespace", m.changefeedID.Namespace),
							zap.String("changefeed", m.changefeedID.ID),
							zap.Stringer("span", &tableSink.span),
							zap.String("sinkError", sinkErr.Error()),
							zap.Error(restartErr))
					}
				default:
					return sinkErr
				}
				continue
			}

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
				zap.Stringer("span", &tableSink.span),
				zap.Uint64("memory", requestMemSize))

			t := &sinkTask{
				span:          tableSink.span,
				lowerBound:    lowerBound,
				getUpperBound: m.getUpperBound,
				tableSink:     tableSink,
				callback: func(lastWrittenPos sorter.Position) {
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
			case <-ctx.Done():
				return ctx.Err()
			case m.sinkTaskChan <- t:
				log.Debug("Generate sink task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Stringer("span", &tableSink.span),
					zap.Any("lowerBound", lowerBound),
					zap.Any("currentUpperBound", upperBound))
			default:
				m.sinkMemQuota.Refund(requestMemSize)
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
			upperBound := m.getUpperBound(tableSink.getReceivedSorterResolvedTs())

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
				zap.Stringer("span", &tableSink.span),
				zap.Uint64("memory", requestMemSize))

			t := &redoTask{
				span:          tableSink.span,
				lowerBound:    lowerBound,
				getUpperBound: m.getUpperBound,
				tableSink:     tableSink,
				callback: func(lastWrittenPos sorter.Position) {
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
			case <-ctx.Done():
				return ctx.Err()
			case m.redoTaskChan <- t:
				log.Debug("Generate redo task",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.Stringer("span", &tableSink.span),
					zap.Any("lowerBound", lowerBound),
					zap.Any("currentUpperBound", upperBound),
					zap.Float64("lag", time.Since(oracle.GetTimeFromTS(upperBound.CommitTs)).Seconds()))
			default:
				m.redoMemQuota.Refund(requestMemSize)
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

// UpdateBarrierTs update all tableSink's barrierTs in the SinkManager
func (m *SinkManager) UpdateBarrierTs(globalBarrierTs model.Ts, tableBarrier map[model.TableID]model.Ts) {
	m.tableSinks.Range(func(span tablepb.Span, value interface{}) bool {
		barrierTs := globalBarrierTs
		if tableBarrierTs, ok := tableBarrier[span.TableID]; ok && tableBarrierTs < globalBarrierTs {
			barrierTs = tableBarrierTs
		}
		value.(*tableSinkWrapper).updateBarrierTs(barrierTs)
		return true
	})
}

// AddTable adds a table(TableSink) to the sink manager.
func (m *SinkManager) AddTable(span tablepb.Span, startTs model.Ts, targetTs model.Ts) {
	sinkWrapper := newTableSinkWrapper(
		m.changefeedID,
		span,
		func() (s tablesink.TableSink, version uint64) {
			if m.sinkFactory.TryLock() {
				defer m.sinkFactory.Unlock()
				if m.sinkFactory.f != nil {
					s = m.sinkFactory.f.CreateTableSink(m.changefeedID, span, startTs, m.up.PDClock, m.metricsTableSinkTotalRows, m.metricsTableSinkFlushLagDuration)
					version = m.sinkFactory.version
				}
			}
			return
		},
		tablepb.TableStatePreparing,
		startTs,
		targetTs,
		func(ctx context.Context) (model.Ts, error) {
			return genReplicateTs(ctx, m.up.PDClient)
		},
	)

	_, loaded := m.tableSinks.LoadOrStore(span, sinkWrapper)
	if loaded {
		log.Panic("Add an exists table sink",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
		return
	}
	m.sinkMemQuota.AddTable(span)
	m.redoMemQuota.AddTable(span)
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

	if err := tableSink.(*tableSinkWrapper).start(m.managerCtx, startTs); err != nil {
		return err
	}

	m.sinkProgressHeap.push(&progress{
		span:              span,
		nextLowerBoundPos: sorter.Position{StartTs: 0, CommitTs: startTs + 1},
		version:           tableSink.(*tableSinkWrapper).version,
	})
	if m.redoDMLMgr != nil {
		m.redoProgressHeap.push(&progress{
			span:              span,
			nextLowerBoundPos: sorter.Position{StartTs: 0, CommitTs: startTs + 1},
			version:           tableSink.(*tableSinkWrapper).version,
		})
	}
	return nil
}

// AsyncStopTable sets the table(TableSink) state to stopped.
func (m *SinkManager) AsyncStopTable(span tablepb.Span) bool {
	tableSink, ok := m.tableSinks.Load(span)
	if !ok {
		// Just warn, because the table sink may be removed by another goroutine.
		// This logic is the same as this function's caller.
		log.Warn("Table sink not found when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
	}
	if tableSink.(*tableSinkWrapper).asyncStop() {
		cleanedBytes := m.sinkMemQuota.RemoveTable(span)
		cleanedBytes += m.redoMemQuota.RemoveTable(span)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("memory", cleanedBytes))
		return true
	}
	return false
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
	checkpointTs := value.(*tableSinkWrapper).getCheckpointTs()
	log.Info("Remove table sink successfully",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Stringer("span", &span),
		zap.Uint64("checkpointTs", checkpointTs.Ts))
}

// GetAllCurrentTableSpans returns all spans in the sinkManager.
func (m *SinkManager) GetAllCurrentTableSpans() []tablepb.Span {
	var spans []tablepb.Span
	m.tableSinks.Range(func(key tablepb.Span, value interface{}) bool {
		spans = append(spans, key)
		return true
	})
	return spans
}

// GetAllCurrentTableSpansCount returns the table spans count in the sinkManager.
func (m *SinkManager) GetAllCurrentTableSpansCount() int {
	res := 0
	m.tableSinks.Range(func(key tablepb.Span, value interface{}) bool {
		res++
		return true
	})
	return res
}

// GetTableState returns the table(TableSink) state.
func (m *SinkManager) GetTableState(span tablepb.Span) (tablepb.TableState, bool) {
	wrapper, ok := m.tableSinks.Load(span)
	if !ok {
		log.Debug("Table sink not found when getting table state",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
		return tablepb.TableStateAbsent, false
	}

	// NOTE(qupeng): I'm not sure whether `SinkManager.AsyncStopTable` will be called
	// again or not if it returns false. So we must retry `tableSink.asyncClose` here
	// if necessary. It's better to remove the dirty logic in the future.
	tableSink := wrapper.(*tableSinkWrapper)
	if tableSink.getState() == tablepb.TableStateStopping && tableSink.asyncStop() {
		cleanedBytes := m.sinkMemQuota.RemoveTable(span)
		cleanedBytes += m.redoMemQuota.RemoveTable(span)
		log.Debug("MemoryQuotaTracing: Clean up memory quota for table sink task when removing table",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("memory", cleanedBytes))
	}
	return tableSink.getState(), true
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
	lastSyncedTs := tableSink.getLastSyncedTs()
	m.sinkMemQuota.Release(span, checkpointTs)
	m.redoMemQuota.Release(span, checkpointTs)

	advanceTimeoutInSec := util.GetOrZero(m.changefeedInfo.Config.Sink.AdvanceTimeoutInSec)
	if advanceTimeoutInSec <= 0 {
		advanceTimeoutInSec = config.DefaultAdvanceTimeoutInSec
	}
	stuckCheck := time.Duration(advanceTimeoutInSec) * time.Second

	if m.needsStuckCheck() {
		isStuck, sinkVersion := tableSink.sinkMaybeStuck(stuckCheck)
		if isStuck && m.putSinkFactoryError(errors.New("table sink stuck"), sinkVersion) {
			log.Warn("Table checkpoint is stuck too long, will restart the sink backend",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Stringer("span", &span),
				zap.Any("checkpointTs", checkpointTs),
				zap.Float64("stuckCheck", stuckCheck.Seconds()),
				zap.Uint64("factoryVersion", sinkVersion))
		}
	}

	var resolvedTs model.Ts
	// If redo log is enabled, we have to use redo log's resolved ts to calculate processor's min resolved ts.
	if m.redoDMLMgr != nil {
		resolvedTs = m.redoDMLMgr.GetResolvedTs(span)
	} else {
		resolvedTs = tableSink.getReceivedSorterResolvedTs()
	}

	sinkUpperBound := tableSink.getUpperBoundTs()
	if sinkUpperBound < checkpointTs.ResolvedMark() {
		log.Panic("sinkManager: sink upperbound should not less than checkpoint ts",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("upperbound", sinkUpperBound),
			zap.Any("checkpointTs", checkpointTs))
	}
	return TableStats{
		CheckpointTs: checkpointTs.ResolvedMark(),
		ResolvedTs:   resolvedTs,
		LastSyncedTs: lastSyncedTs,
		BarrierTs:    tableSink.barrierTs.Load(),
	}
}

// WaitForReady implements pkg/util.Runnable.
func (m *SinkManager) WaitForReady(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-m.ready:
	}
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
	// NOTE: It's unnecceary to close table sinks before clear sink factory.
	m.clearSinkFactory()

	log.Info("Closed sink manager",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("cost", time.Since(start)))
}
