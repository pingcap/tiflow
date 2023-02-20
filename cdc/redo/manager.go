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

package redo

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const redoFlushWarnDuration = time.Second * 20

// Redo Manager GC interval. It can be changed in tests.
var defaultGCIntervalInMs = 5000 // 5 seconds

// LogManager defines an interface that is used to manage redo log
type LogManager interface {
	// Enabled returns whether the log manager is enabled
	Enabled() bool

	// The following APIs are called from processor only.
	AddTable(tableID model.TableID, startTs uint64)
	RemoveTable(tableID model.TableID)
	GetResolvedTs(tableID model.TableID) model.Ts
	// Min resolvedTs for all tables. If there is no tables, return math.MaxInt64.
	GetMinResolvedTs() uint64
	EmitRowChangedEvents(ctx context.Context, tableID model.TableID, rows ...*model.RowChangedEvent) error
	UpdateResolvedTs(ctx context.Context, tableID model.TableID, resolvedTs uint64) error
	// Update checkpoint so that it can GC stale files.
	UpdateCheckpointTs(checkpointTs model.Ts)

	// The following APIs are called from owner only.
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error
	UpdateMeta(checkpointTs, resolvedTs model.Ts)
	GetFlushedMeta(checkpointTs, resolvedTs *model.Ts)

	// Cleanup removes all redo logs. Only called from owner.
	Cleanup(ctx context.Context) error
}

type cacheEvents struct {
	tableID    model.TableID
	rows       []*model.RowChangedEvent
	resolvedTs model.Ts
	eventType  model.MessageType
}

type statefulRts struct {
	flushed   model.Ts
	unflushed model.Ts
}

func (s *statefulRts) getFlushed() model.Ts {
	return atomic.LoadUint64(&s.flushed)
}

func (s *statefulRts) getUnflushed() model.Ts {
	return atomic.LoadUint64(&s.unflushed)
}

func (s *statefulRts) setFlushed(flushed model.Ts) {
	atomic.StoreUint64(&s.flushed, flushed)
}

func (s *statefulRts) checkAndSetUnflushed(unflushed model.Ts) {
	for {
		old := atomic.LoadUint64(&s.unflushed)
		if old > unflushed {
			return
		}
		if atomic.CompareAndSwapUint64(&s.unflushed, old, unflushed) {
			break
		}
	}
}

// ManagerImpl manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements LogManager interface.
type ManagerImpl struct {
	changeFeedID model.ChangeFeedID
	enabled      bool

	opts *ManagerOptions

	// rtsMap stores flushed and unflushed resolved timestamps for all tables.
	// it's just like map[tableID]*statefulRts.
	// For a given statefulRts, unflushed is updated in routine bgUpdateLog,
	// and flushed is updated in flushLog.
	rtsMap sync.Map

	// A fast way to get min flushed timestamp in rtsMap. It can be updated
	// in redo-flush goroutine and AddTable/RemoveTable.
	minResolvedTs uint64

	// Updated by `UpdateResolvedTs`. It's required by some internal places,
	// such as GC.
	checkpointTs uint64

	metaCheckpointTs statefulRts
	metaResolvedTs   statefulRts

	rwlock    sync.RWMutex
	writer    writer.RedoLogWriter
	logBuffer *chann.Chann[cacheEvents]
	closed    int32

	flushing      int64
	lastFlushTime time.Time

	metricWriteLogDuration    prometheus.Observer
	metricFlushLogDuration    prometheus.Observer
	metricTotalRowsCount      prometheus.Counter
	metricRedoWorkerBusyRatio prometheus.Counter
}

// NewManager creates a new Manager
func NewManager(ctx context.Context, cfg *config.ConsistentConfig, opts *ManagerOptions) (*ManagerImpl, error) {
	// return a disabled Manager if no consistent config or normal consistent level
	if cfg == nil || !redo.IsConsistentEnabled(cfg.Level) {
		return &ManagerImpl{enabled: false}, nil
	}

	writer, err := writer.NewRedoLogWriter(ctx, cfg, opts.FileTypeConfig)
	if err != nil {
		return nil, err
	}

	m := &ManagerImpl{
		changeFeedID:  contextutil.ChangefeedIDFromCtx(ctx),
		enabled:       true,
		opts:          opts,
		rtsMap:        sync.Map{},
		minResolvedTs: math.MaxInt64,
		writer:        writer,
	}

	if m.opts.EmitMeta {
		checkpointTs, resolvedTs := m.writer.GetMeta()
		m.metaCheckpointTs.flushed = checkpointTs
		m.metaCheckpointTs.unflushed = checkpointTs
		m.metaResolvedTs.flushed = resolvedTs
		m.metaResolvedTs.unflushed = resolvedTs
	}

	m.metricWriteLogDuration = common.RedoWriteLogDurationHistogram.
		WithLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	m.metricFlushLogDuration = common.RedoFlushLogDurationHistogram.
		WithLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	m.metricTotalRowsCount = common.RedoTotalRowsCountGauge.
		WithLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	m.metricRedoWorkerBusyRatio = common.RedoWorkerBusyRatio.
		WithLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)

	// TODO: better to wait background goroutines after the context is canceled.
	if m.opts.EnableBgRunner {
		m.logBuffer = chann.New[cacheEvents]()
		go m.bgUpdateLog(ctx, cfg.FlushIntervalInMs, opts.ErrCh)
	}
	if m.opts.EnableGCRunner {
		go m.bgGC(ctx)
	}

	return m, nil
}

// NewDisabledManager returns a disabled log manger instance, used in test only
func NewDisabledManager() *ManagerImpl {
	return &ManagerImpl{enabled: false}
}

// NewMockManager returns a mock redo manager instance, used in test only
func NewMockManager(ctx context.Context) (*ManagerImpl, error) {
	cfg := &config.ConsistentConfig{
		Level:             string(redo.ConsistentLevelEventual),
		Storage:           "blackhole://",
		FlushIntervalInMs: config.DefaultFlushIntervalInMs,
	}

	errCh := make(chan error, 1)
	logMgr, err := NewManager(ctx, cfg, newMockManagerOptions(errCh))
	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			log.Panic("log manager error: ", zap.Error(err))
		}
	}()

	return logMgr, err
}

// Enabled returns whether this log manager is enabled
func (m *ManagerImpl) Enabled() bool {
	return m.enabled
}

// EmitRowChangedEvents sends row changed events to a log buffer, the log buffer
// will be consumed by a background goroutine, which converts row changed events
// to redo logs and sends to log writer.
// TODO: After buffer sink in sink node is removed, there is no batch mechanism
// before sending row changed events to redo manager, the original log buffer
// design may have performance issue.
func (m *ManagerImpl) EmitRowChangedEvents(
	ctx context.Context,
	tableID model.TableID,
	rows ...*model.RowChangedEvent,
) error {
	return m.withLock(func(m *ManagerImpl) error {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case m.logBuffer.In() <- cacheEvents{
			tableID:   tableID,
			rows:      rows,
			eventType: model.MessageTypeRow,
		}:
		}
		return nil
	})
}

// UpdateResolvedTs asynchronously updates resolved ts of a single table.
func (m *ManagerImpl) UpdateResolvedTs(
	ctx context.Context,
	tableID model.TableID,
	resolvedTs uint64,
) error {
	return m.withLock(func(m *ManagerImpl) error {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case m.logBuffer.In() <- cacheEvents{
			tableID:    tableID,
			resolvedTs: resolvedTs,
			eventType:  model.MessageTypeResolved,
		}:
		}
		return nil
	})
}

// UpdateCheckpointTs updates checkpoint to the manager.
func (m *ManagerImpl) UpdateCheckpointTs(ckpt model.Ts) {
	for {
		curr := atomic.LoadUint64(&m.checkpointTs)
		if ckpt <= curr || atomic.CompareAndSwapUint64(&m.checkpointTs, curr, ckpt) {
			break
		}
	}
}

// EmitDDLEvent sends DDL event to redo log writer
func (m *ManagerImpl) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return m.withLock(func(m *ManagerImpl) error {
		return m.writer.SendDDL(ctx, common.DDLToRedo(ddl))
	})
}

// GetResolvedTs returns the resolved ts of a table
func (m *ManagerImpl) GetResolvedTs(tableID model.TableID) model.Ts {
	if value, ok := m.rtsMap.Load(tableID); ok {
		return value.(*statefulRts).getFlushed()
	}
	panic("GetResolvedTs is called on an invalid table")
}

// GetMinResolvedTs returns the minimum resolved ts of all tables in this redo log manager
func (m *ManagerImpl) GetMinResolvedTs() model.Ts {
	return atomic.LoadUint64(&m.minResolvedTs)
}

// UpdateMeta updates meta.
func (m *ManagerImpl) UpdateMeta(checkpointTs, resolvedTs model.Ts) {
	m.metaResolvedTs.checkAndSetUnflushed(resolvedTs)
	m.metaCheckpointTs.checkAndSetUnflushed(checkpointTs)
}

// GetFlushedMeta gets flushed meta.
func (m *ManagerImpl) GetFlushedMeta(checkpointTs, resolvedTs *model.Ts) {
	*checkpointTs = m.metaCheckpointTs.getFlushed()
	*resolvedTs = m.metaResolvedTs.getFlushed()
}

// AddTable adds a new table in redo log manager
func (m *ManagerImpl) AddTable(tableID model.TableID, startTs uint64) {
	_, loaded := m.rtsMap.LoadOrStore(tableID, &statefulRts{flushed: startTs, unflushed: startTs})
	if loaded {
		log.Warn("add duplicated table in redo log manager", zap.Int64("tableID", tableID))
		return
	}

	for {
		currMin := m.GetMinResolvedTs()
		if currMin < startTs || atomic.CompareAndSwapUint64(&m.minResolvedTs, currMin, startTs) {
			break
		}
	}
}

// RemoveTable removes a table from redo log manager
func (m *ManagerImpl) RemoveTable(tableID model.TableID) {
	var v interface{}
	var ok bool
	if v, ok = m.rtsMap.LoadAndDelete(tableID); !ok {
		log.Warn("remove a table not maintained in redo log manager", zap.Int64("tableID", tableID))
		return
	}

	removedTs := v.(*statefulRts).getFlushed()
	for {
		currMin := m.GetMinResolvedTs()
		if currMin > removedTs {
			break
		}
		newMin := uint64(math.MaxInt64)
		m.rtsMap.Range(func(key interface{}, value interface{}) bool {
			rts := value.(*statefulRts)
			flushed := rts.getFlushed()
			if flushed < newMin {
				newMin = flushed
			}
			return true
		})
		if atomic.CompareAndSwapUint64(&m.minResolvedTs, currMin, newMin) {
			break
		}
	}
}

// Cleanup removes all redo logs of this manager, it is called when changefeed is removed
// only owner should call this method.
func (m *ManagerImpl) Cleanup(ctx context.Context) error {
	common.RedoWriteLogDurationHistogram.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoFlushLogDurationHistogram.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoTotalRowsCountGauge.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoWorkerBusyRatio.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	return m.withLock(func(m *ManagerImpl) error { return m.writer.DeleteAllLogs(ctx) })
}

func (m *ManagerImpl) prepareForFlush() (tableRtsMap map[model.TableID]model.Ts, minResolvedTs model.Ts) {
	if !m.opts.EmitRowEvents {
		return
	}

	tableRtsMap = make(map[model.TableID]model.Ts)
	minResolvedTs = math.MaxUint64
	m.rtsMap.Range(func(key interface{}, value interface{}) bool {
		tableID := key.(model.TableID)
		rts := value.(*statefulRts)
		unflushed := rts.getUnflushed()
		flushed := rts.getFlushed()
		if unflushed > flushed {
			flushed = unflushed
		}
		tableRtsMap[tableID] = flushed
		if flushed < minResolvedTs {
			minResolvedTs = flushed
		}
		return true
	})

	if minResolvedTs == math.MaxUint64 {
		minResolvedTs = 0
	}
	return
}

func (m *ManagerImpl) postFlush(tableRtsMap map[model.TableID]model.Ts, minResolvedTs model.Ts) {
	if !m.opts.EmitRowEvents {
		return
	}
	if minResolvedTs != 0 {
		// m.minResolvedTs is only updated in flushLog, so no other one can change it.
		atomic.StoreUint64(&m.minResolvedTs, minResolvedTs)
	}
	for tableID, flushed := range tableRtsMap {
		if value, loaded := m.rtsMap.Load(tableID); loaded {
			value.(*statefulRts).setFlushed(flushed)
		}
	}
}

func (m *ManagerImpl) prepareForFlushMeta() (metaCheckpoint, metaResolved model.Ts) {
	if !m.opts.EmitMeta {
		return
	}
	metaCheckpoint = m.metaCheckpointTs.getUnflushed()
	metaResolved = m.metaResolvedTs.getUnflushed()
	return
}

func (m *ManagerImpl) postFlushMeta(metaCheckpoint, metaResolved model.Ts) {
	if !m.opts.EmitMeta {
		return
	}
	m.metaResolvedTs.setFlushed(metaResolved)
	m.metaCheckpointTs.setFlushed(metaCheckpoint)
}

func (m *ManagerImpl) flushLog(
	ctx context.Context, handleErr func(err error), workTimeSlice *time.Duration,
) {
	start := time.Now()
	defer func() {
		*workTimeSlice += time.Since(start)
	}()
	if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
		log.Debug("Fail to update flush flag, " +
			"the previous flush operation hasn't finished yet")
		if time.Since(m.lastFlushTime) > redoFlushWarnDuration {
			log.Warn("flushLog blocking too long, the redo manager may be stuck",
				zap.Duration("duration", time.Since(m.lastFlushTime)),
				zap.Any("changfeed", m.changeFeedID))
		}
		return
	}

	m.lastFlushTime = time.Now()
	go func() {
		defer atomic.StoreInt64(&m.flushing, 0)

		tableRtsMap, minResolvedTs := m.prepareForFlush()
		metaCheckpoint, metaResolved := m.prepareForFlushMeta()
		err := m.withLock(func(m *ManagerImpl) error {
			return m.writer.FlushLog(ctx, metaCheckpoint, metaResolved)
		})
		m.metricFlushLogDuration.Observe(time.Since(m.lastFlushTime).Seconds())
		if err != nil {
			handleErr(err)
			return
		}
		m.postFlush(tableRtsMap, minResolvedTs)
		m.postFlushMeta(metaCheckpoint, metaResolved)
	}()

	return
}

func (m *ManagerImpl) onResolvedTsMsg(tableID model.TableID, resolvedTs model.Ts) {
	value, loaded := m.rtsMap.Load(tableID)
	if !loaded {
		panic("onResolvedTsMsg is called for an invalid table")
	}
	value.(*statefulRts).checkAndSetUnflushed(resolvedTs)
}

func (m *ManagerImpl) bgUpdateLog(
	ctx context.Context, flushIntervalInMs int64, errCh chan<- error,
) {
	// logErrCh is used to retrieve errors from log flushing goroutines.
	// if the channel is full, it's better to block subsequent flushing goroutines.
	logErrCh := make(chan error, 1)
	handleErr := func(err error) { logErrCh <- err }

	log.Info("redo manager bgUpdateLog is running",
		zap.String("namespace", m.changeFeedID.Namespace),
		zap.String("changefeed", m.changeFeedID.ID),
		zap.Int64("flushIntervalInMs", flushIntervalInMs))

	ticker := time.NewTicker(time.Duration(flushIntervalInMs) * time.Millisecond)
	defer func() {
		ticker.Stop()

		m.rwlock.Lock()
		defer m.rwlock.Unlock()
		atomic.StoreInt32(&m.closed, 1)

		m.logBuffer.Close()
		for range m.logBuffer.Out() {
		}
		if err := m.writer.Close(); err != nil {
			log.Error("redo manager fails to close writer",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID),
				zap.Error(err))
		}

		log.Info("redo manager bgUpdateLog exits",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID))
	}()

	var err error
	overseerTicker := time.NewTicker(time.Second * 5)
	defer overseerTicker.Stop()
	var workTimeSlice time.Duration
	startToWork := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case err = <-logErrCh:
		case now := <-overseerTicker.C:
			busyRatio := int(workTimeSlice.Seconds() / now.Sub(startToWork).Seconds() * 1000)
			m.metricRedoWorkerBusyRatio.Add(float64(busyRatio))
			startToWork = now
			workTimeSlice = 0
		case <-ticker.C:
			// interpolate tick message to flush writer if needed
			m.flushLog(ctx, handleErr, &workTimeSlice)
		case cache, ok := <-m.logBuffer.Out():
			if !ok {
				return // channel closed
			}
			switch cache.eventType {
			case model.MessageTypeRow:
				start := time.Now()
				logs := make([]*model.RedoRowChangedEvent, 0, len(cache.rows))
				for _, row := range cache.rows {
					logs = append(logs, common.RowToRedo(row))
				}
				err = m.writer.WriteLog(ctx, cache.tableID, logs)
				m.metricWriteLogDuration.Observe(time.Since(start).Seconds())
				m.metricTotalRowsCount.Add(float64(len(logs)))
			case model.MessageTypeResolved:
				m.onResolvedTsMsg(cache.tableID, cache.resolvedTs)
			default:
				log.Debug("handle unknown event type")
			}
		}
		if err != nil {
			log.Warn("redo manager writer meets write or flush fail",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID),
				zap.Error(err))
			break
		}
	}

	// NOTE: the goroutine should never exit until the error is put into errCh successfully
	// or the context is canceled.
	select {
	case <-ctx.Done():
	case errCh <- err:
	}
}

func (m *ManagerImpl) bgGC(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(defaultGCIntervalInMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("redo manager GC exits as context cancelled",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID))
			return
		case <-ticker.C:
			ckpt := atomic.LoadUint64(&m.checkpointTs)
			if ckpt == 0 {
				continue
			}
			log.Info("redo manager GC is triggered",
				zap.Uint64("checkpointTs", ckpt),
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID))
			err := m.writer.GC(ctx, ckpt)
			if err != nil {
				log.Warn("redo manager log GC fail",
					zap.String("namespace", m.changeFeedID.Namespace),
					zap.String("changefeed", m.changeFeedID.ID), zap.Error(err))
			}
		}
	}
}

func (m *ManagerImpl) withLock(action func(m *ManagerImpl) error) error {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()
	if atomic.LoadInt32(&m.closed) != 0 {
		return cerror.ErrRedoWriterStopped.GenWithStack("redo manager is stopped")
	}
	return action(m)
}
