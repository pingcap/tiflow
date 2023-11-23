// Copyright 2023 PingCAP, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/cdc/redo/writer/factory"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	_ DDLManager = (*ddlManager)(nil)
	_ DMLManager = (*dmlManager)(nil)
)

type redoManager interface {
	// Enabled returns whether the manager is enabled
	Enabled() bool
	Run(ctx context.Context) error
}

// DDLManager defines an interface that is used to manage ddl logs in owner.
type DDLManager interface {
	redoManager
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error
	UpdateResolvedTs(ctx context.Context, resolvedTs uint64) error
	GetResolvedTs() model.Ts
}

// NewDisabledDDLManager creates a disabled ddl Manager.
func NewDisabledDDLManager() *ddlManager {
	return &ddlManager{
		logManager: &logManager{enabled: false},
	}
}

// NewDDLManager creates a new ddl Manager.
func NewDDLManager(
	changefeedID model.ChangeFeedID,
	cfg *config.ConsistentConfig, ddlStartTs model.Ts,
) *ddlManager {
	m := newLogManager(changefeedID, cfg, redo.RedoDDLLogFileType)
	tableID := int64(0)
	m.AddTable(tableID, ddlStartTs)
	return &ddlManager{
		logManager: m,
		// The current fakeTableID is meaningless, find a meaningful id in the future.
		fakeTableID: tableID,
	}
}

type ddlManager struct {
	*logManager
	fakeTableID model.TableID
}

func (m *ddlManager) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return m.logManager.emitRedoEvents(ctx, m.fakeTableID, nil, ddl)
}

func (m *ddlManager) UpdateResolvedTs(ctx context.Context, resolvedTs uint64) error {
	return m.logManager.UpdateResolvedTs(ctx, m.fakeTableID, resolvedTs)
}

func (m *ddlManager) GetResolvedTs() model.Ts {
	return m.logManager.GetResolvedTs(m.fakeTableID)
}

// DMLManager defines an interface that is used to manage dml logs in processor.
type DMLManager interface {
	redoManager
	AddTable(tableID model.TableID, startTs uint64)
	StartTable(tableID model.TableID, startTs uint64)
	RemoveTable(tableID model.TableID)
	UpdateResolvedTs(ctx context.Context, tableID model.TableID, resolvedTs uint64) error
	GetResolvedTs(tableID model.TableID) model.Ts
	EmitRowChangedEvents(
		ctx context.Context,
		tableID model.TableID,
		releaseRowsMemory func(),
		rows ...*model.RowChangedEvent,
	) error
}

// NewDMLManager creates a new dml Manager.
func NewDMLManager(
	changefeedID model.ChangeFeedID, cfg *config.ConsistentConfig,
) *dmlManager {
	return &dmlManager{
		logManager: newLogManager(changefeedID, cfg, redo.RedoRowLogFileType),
	}
}

// NewDisabledDMLManager creates a disabled dml Manager.
func NewDisabledDMLManager() *dmlManager {
	return &dmlManager{
		logManager: &logManager{enabled: false},
	}
}

type dmlManager struct {
	*logManager
}

// EmitRowChangedEvents emits row changed events to the redo log.
func (m *dmlManager) EmitRowChangedEvents(
	ctx context.Context,
	tableID model.TableID,
	releaseRowsMemory func(),
	rows ...*model.RowChangedEvent,
) error {
	var events []writer.RedoEvent
	for _, row := range rows {
		events = append(events, row)
	}
	return m.logManager.emitRedoEvents(ctx, tableID, releaseRowsMemory, events...)
}

type cacheEvents struct {
	tableID         model.TableID
	events          []writer.RedoEvent
	resolvedTs      model.Ts
	isResolvedEvent bool

	// releaseMemory is used to track memory usage of the events.
	releaseMemory func()
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

func (s *statefulRts) checkAndSetUnflushed(unflushed model.Ts) (changed bool) {
	for {
		old := atomic.LoadUint64(&s.unflushed)
		if old > unflushed {
			return false
		}
		if atomic.CompareAndSwapUint64(&s.unflushed, old, unflushed) {
			break
		}
	}
	return true
}

func (s *statefulRts) checkAndSetFlushed(flushed model.Ts) (changed bool) {
	for {
		old := atomic.LoadUint64(&s.flushed)
		if old > flushed {
			return false
		}
		if atomic.CompareAndSwapUint64(&s.flushed, old, flushed) {
			break
		}
	}
	return true
}

// logManager manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements DDLManager and DMLManager interface.
type logManager struct {
	enabled bool
	cfg     *writer.LogWriterConfig
	writer  writer.RedoLogWriter

	rwlock sync.RWMutex
	// TODO: remove logBuffer and use writer directly after file logWriter is deprecated.
	logBuffer *chann.DrainableChann[cacheEvents]
	closed    int32

	// rtsMap stores flushed and unflushed resolved timestamps for all tables.
	// it's just like map[tableID]*statefulRts.
	// For a given statefulRts, unflushed is updated in routine bgUpdateLog,
	// and flushed is updated in flushLog.
	rtsMap sync.Map

	flushing         int64
	lastFlushTime    time.Time
	releaseMemoryCbs []func()

	metricWriteLogDuration    prometheus.Observer
	metricFlushLogDuration    prometheus.Observer
	metricTotalRowsCount      prometheus.Counter
	metricRedoWorkerBusyRatio prometheus.Counter
}

func newLogManager(
	changefeedID model.ChangeFeedID, cfg *config.ConsistentConfig, logType string,
) *logManager {
	// return a disabled Manager if no consistent config or normal consistent level
	if cfg == nil || !redo.IsConsistentEnabled(cfg.Level) {
		return &logManager{enabled: false}
	}

	return &logManager{
		enabled: true,
		cfg: &writer.LogWriterConfig{
			ConsistentConfig:  *cfg,
			LogType:           logType,
			CaptureID:         config.GetGlobalServerConfig().AdvertiseAddr,
			ChangeFeedID:      changefeedID,
			MaxLogSizeInBytes: cfg.MaxLogSize * redo.Megabyte,
		},
		logBuffer: chann.NewDrainableChann[cacheEvents](),
		metricWriteLogDuration: common.RedoWriteLogDurationHistogram.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricFlushLogDuration: common.RedoFlushLogDurationHistogram.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricTotalRowsCount: common.RedoTotalRowsCountGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricRedoWorkerBusyRatio: common.RedoWorkerBusyRatio.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
}

// Run implements pkg/util.Runnable.
func (m *logManager) Run(ctx context.Context) error {
	failpoint.Inject("ChangefeedNewRedoManagerError", func() {
		failpoint.Return(errors.New("changefeed new redo manager injected error"))
	})
	if !m.Enabled() {
		return nil
	}

	defer m.close()
	start := time.Now()
	w, err := factory.NewRedoLogWriter(ctx, m.cfg)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", m.cfg.ChangeFeedID.ID),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return err
	}
	m.writer = w
	return m.bgUpdateLog(ctx, m.getFlushDuration())
}

func (m *logManager) getFlushDuration() time.Duration {
	flushIntervalInMs := m.cfg.FlushIntervalInMs
	defaultFlushIntervalInMs := redo.DefaultFlushIntervalInMs
	if m.cfg.LogType == redo.RedoDDLLogFileType {
		flushIntervalInMs = m.cfg.MetaFlushIntervalInMs
		defaultFlushIntervalInMs = redo.DefaultMetaFlushIntervalInMs
	}
	if flushIntervalInMs < redo.MinFlushIntervalInMs {
		log.Warn("redo flush interval is too small, use default value",
			zap.Stringer("namespace", m.cfg.ChangeFeedID),
			zap.Int("default", defaultFlushIntervalInMs),
			zap.String("logType", m.cfg.LogType),
			zap.Int64("interval", flushIntervalInMs))
		flushIntervalInMs = int64(defaultFlushIntervalInMs)
	}
	return time.Duration(flushIntervalInMs) * time.Millisecond
}

// Enabled returns whether this log manager is enabled
func (m *logManager) Enabled() bool {
	return m.enabled
}

// emitRedoEvents sends row changed events to a log buffer, the log buffer
// will be consumed by a background goroutine, which converts row changed events
// to redo logs and sends to log writer.
func (m *logManager) emitRedoEvents(
	ctx context.Context,
	tableID model.TableID,
	releaseRowsMemory func(),
	events ...writer.RedoEvent,
) error {
	return m.withLock(func(m *logManager) error {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case m.logBuffer.In() <- cacheEvents{
			tableID:         tableID,
			events:          events,
			releaseMemory:   releaseRowsMemory,
			isResolvedEvent: false,
		}:
		}
		return nil
	})
}

// StartTable starts a table, which means the table is ready to emit redo events.
// Note that this function should only be called once when adding a new table to processor.
func (m *logManager) StartTable(tableID model.TableID, resolvedTs uint64) {
	// advance unflushed resolved ts
	m.onResolvedTsMsg(tableID, resolvedTs)

	// advance flushed resolved ts
	if value, loaded := m.rtsMap.Load(tableID); loaded {
		value.(*statefulRts).checkAndSetFlushed(resolvedTs)
	}
}

// UpdateResolvedTs asynchronously updates resolved ts of a single table.
func (m *logManager) UpdateResolvedTs(
	ctx context.Context,
	tableID model.TableID,
	resolvedTs uint64,
) error {
	return m.withLock(func(m *logManager) error {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case m.logBuffer.In() <- cacheEvents{
			tableID:         tableID,
			resolvedTs:      resolvedTs,
			isResolvedEvent: true,
		}:
		}
		return nil
	})
}

// GetResolvedTs returns the resolved ts of a table
func (m *logManager) GetResolvedTs(tableID model.TableID) model.Ts {
	if value, ok := m.rtsMap.Load(tableID); ok {
		return value.(*statefulRts).getFlushed()
	}
	panic("GetResolvedTs is called on an invalid table")
}

// AddTable adds a new table in redo log manager
func (m *logManager) AddTable(tableID model.TableID, startTs uint64) {
	_, loaded := m.rtsMap.LoadOrStore(tableID, &statefulRts{flushed: startTs, unflushed: startTs})
	if loaded {
		log.Warn("add duplicated table in redo log manager",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", m.cfg.ChangeFeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
}

// RemoveTable removes a table from redo log manager
func (m *logManager) RemoveTable(tableID model.TableID) {
	if _, ok := m.rtsMap.LoadAndDelete(tableID); !ok {
		log.Warn("remove a table not maintained in redo log manager",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", m.cfg.ChangeFeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
}

func (m *logManager) prepareForFlush() (tableRtsMap map[model.TableID]model.Ts) {
	tableRtsMap = make(map[model.TableID]model.Ts)
	m.rtsMap.Range(func(key interface{}, value interface{}) bool {
		tableID := key.(model.TableID)
		rts := value.(*statefulRts)
		unflushed := rts.getUnflushed()
		flushed := rts.getFlushed()
		if unflushed > flushed {
			flushed = unflushed
		}
		tableRtsMap[tableID] = flushed
		return true
	})
	return tableRtsMap
}

func (m *logManager) postFlush(tableRtsMap map[model.TableID]model.Ts) {
	for tableID, flushed := range tableRtsMap {
		if value, loaded := m.rtsMap.Load(tableID); loaded {
			changed := value.(*statefulRts).checkAndSetFlushed(flushed)
			if !changed {
				log.Debug("flush redo with regressed resolved ts",
					zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
					zap.String("changefeed", m.cfg.ChangeFeedID.ID),
					zap.Int64("tableID", tableID),
					zap.Uint64("flushed", flushed),
					zap.Uint64("current", value.(*statefulRts).getFlushed()))
			}
		}
	}
}

func (m *logManager) flushLog(
	ctx context.Context, handleErr func(err error), workTimeSlice *time.Duration,
) {
	start := time.Now()
	defer func() {
		*workTimeSlice += time.Since(start)
	}()
	if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
		log.Debug("Fail to update flush flag, "+
			"the previous flush operation hasn't finished yet",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", m.cfg.ChangeFeedID.ID))
		if time.Since(m.lastFlushTime) > redo.FlushWarnDuration {
			log.Warn("flushLog blocking too long, the redo manager may be stuck",
				zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", m.cfg.ChangeFeedID.ID),
				zap.Duration("duration", time.Since(m.lastFlushTime)))
		}
		return
	}

	m.lastFlushTime = time.Now()
	releaseMemoryCbs := m.releaseMemoryCbs
	m.releaseMemoryCbs = make([]func(), 0, 1024)
	go func() {
		defer atomic.StoreInt64(&m.flushing, 0)

		tableRtsMap := m.prepareForFlush()
		log.Debug("Flush redo log",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", m.cfg.ChangeFeedID.ID),
			zap.Any("tableRtsMap", tableRtsMap))
		err := m.withLock(func(m *logManager) error {
			return m.writer.FlushLog(ctx)
		})
		for _, releaseMemory := range releaseMemoryCbs {
			releaseMemory()
		}
		m.metricFlushLogDuration.Observe(time.Since(m.lastFlushTime).Seconds())
		if err != nil {
			handleErr(err)
			return
		}
		m.postFlush(tableRtsMap)
	}()
}

func (m *logManager) handleEvent(
	ctx context.Context, e cacheEvents, workTimeSlice *time.Duration,
) error {
	startHandleEvent := time.Now()
	defer func() {
		*workTimeSlice += time.Since(startHandleEvent)
	}()

	if e.isResolvedEvent {
		m.onResolvedTsMsg(e.tableID, e.resolvedTs)
	} else {
		if e.releaseMemory != nil {
			m.releaseMemoryCbs = append(m.releaseMemoryCbs, e.releaseMemory)
		}

		start := time.Now()
		err := m.writer.WriteEvents(ctx, e.events...)
		if err != nil {
			return errors.Trace(err)
		}
		writeLogElapse := time.Since(start)
		log.Debug("redo manager writes rows",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", m.cfg.ChangeFeedID.ID),
			zap.Int("rows", len(e.events)),
			zap.Error(err),
			zap.Duration("writeLogElapse", writeLogElapse))
		m.metricTotalRowsCount.Add(float64(len(e.events)))
		m.metricWriteLogDuration.Observe(writeLogElapse.Seconds())
	}
	return nil
}

func (m *logManager) onResolvedTsMsg(tableID model.TableID, resolvedTs model.Ts) {
	// It's possible that the table is removed while redo log is still in writing.
	if value, loaded := m.rtsMap.Load(tableID); loaded {
		value.(*statefulRts).checkAndSetUnflushed(resolvedTs)
	}
}

func (m *logManager) bgUpdateLog(ctx context.Context, flushDuration time.Duration) error {
	m.releaseMemoryCbs = make([]func(), 0, 1024)
	ticker := time.NewTicker(flushDuration)
	defer ticker.Stop()
	log.Info("redo manager bgUpdateLog is running",
		zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
		zap.String("changefeed", m.cfg.ChangeFeedID.ID),
		zap.Duration("flushIntervalInMs", flushDuration),
		zap.Int64("maxLogSize", m.cfg.MaxLogSize),
		zap.Int("encoderWorkerNum", m.cfg.EncodingWorkerNum),
		zap.Int("flushWorkerNum", m.cfg.FlushWorkerNum))

	var err error
	// logErrCh is used to retrieve errors from log flushing goroutines.
	// if the channel is full, it's better to block subsequent flushing goroutines.
	logErrCh := make(chan error, 1)
	handleErr := func(err error) { logErrCh <- err }

	overseerTicker := time.NewTicker(time.Second * 5)
	defer overseerTicker.Stop()
	var workTimeSlice time.Duration
	startToWork := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.flushLog(ctx, handleErr, &workTimeSlice)
		case event, ok := <-m.logBuffer.Out():
			if !ok {
				return nil // channel closed
			}
			err = m.handleEvent(ctx, event, &workTimeSlice)
		case now := <-overseerTicker.C:
			busyRatio := int(workTimeSlice.Seconds() / now.Sub(startToWork).Seconds() * 1000)
			m.metricRedoWorkerBusyRatio.Add(float64(busyRatio))
			startToWork = now
			workTimeSlice = 0
		case err = <-logErrCh:
		}
		if err != nil {
			log.Warn("redo manager writer meets write or flush fail",
				zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", m.cfg.ChangeFeedID.ID),
				zap.Error(err))
			return err
		}
	}
}

func (m *logManager) withLock(action func(m *logManager) error) error {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()
	if atomic.LoadInt32(&m.closed) != 0 {
		return errors.ErrRedoWriterStopped.GenWithStack("redo manager is closed")
	}
	return action(m)
}

func (m *logManager) close() {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	atomic.StoreInt32(&m.closed, 1)

	m.logBuffer.CloseAndDrain()
	if m.writer != nil {
		if err := m.writer.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo manager fails to close writer",
				zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", m.cfg.ChangeFeedID.ID),
				zap.Error(err))
		}
	}
	log.Info("redo manager closed",
		zap.String("namespace", m.cfg.ChangeFeedID.Namespace),
		zap.String("changefeed", m.cfg.ChangeFeedID.ID))
}
