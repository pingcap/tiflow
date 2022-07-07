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
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	flushIntervalInMs int64 = 2000 // 2 seconds
	flushTimeout            = time.Second * 20
)

// ConsistentLevelType is the level of redo log consistent level.
type ConsistentLevelType string

const (
	// ConsistentLevelNone no consistent guarantee.
	ConsistentLevelNone ConsistentLevelType = "none"
	// ConsistentLevelEventual eventual consistent.
	ConsistentLevelEventual ConsistentLevelType = "eventual"
)

type consistentStorage string

const (
	consistentStorageLocal     consistentStorage = "local"
	consistentStorageNFS       consistentStorage = "nfs"
	consistentStorageS3        consistentStorage = "s3"
	consistentStorageBlackhole consistentStorage = "blackhole"
)

// IsValidConsistentLevel checks whether a give consistent level is valid
func IsValidConsistentLevel(level string) bool {
	switch ConsistentLevelType(level) {
	case ConsistentLevelNone, ConsistentLevelEventual:
		return true
	default:
		return false
	}
}

// IsValidConsistentStorage checks whether a give consistent storage is valid
func IsValidConsistentStorage(storage string) bool {
	switch consistentStorage(storage) {
	case consistentStorageLocal, consistentStorageNFS,
		consistentStorageS3, consistentStorageBlackhole:
		return true
	default:
		return false
	}
}

// IsConsistentEnabled returns whether the consistent feature is enabled
func IsConsistentEnabled(level string) bool {
	return IsValidConsistentLevel(level) && ConsistentLevelType(level) != ConsistentLevelNone
}

// IsS3StorageEnabled returns whether s3 storage is enabled
func IsS3StorageEnabled(storage string) bool {
	return consistentStorage(storage) == consistentStorageS3
}

// LogManager defines an interface that is used to manage redo log
type LogManager interface {
	// Enabled returns whether the log manager is enabled
	Enabled() bool

	// The following 6 APIs are called from processor only
	AddTable(tableID model.TableID, startTs uint64)
	RemoveTable(tableID model.TableID)
	GetResolvedTs(tableID model.TableID) model.Ts
	GetMinResolvedTs() uint64
	EmitRowChangedEvents(ctx context.Context, tableID model.TableID, rows ...*model.RowChangedEvent) error
	UpdateResolvedTs(ctx context.Context, tableID model.TableID, resolvedTs uint64) error
	FlushResolvedAndCheckpointTs(ctx context.Context, resolvedTs, checkpointTs uint64) (err error)

	// EmitDDLEvent are called from owner only
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error

	// Cleanup removes all redo logs
	Cleanup(ctx context.Context) error
}

// ManagerOptions defines options for redo log manager
type ManagerOptions struct {
	// whether to run background goroutine to fetch table resolved ts
	EnableBgRunner bool
	ErrCh          chan<- error
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
			panic("statefulRts.unflushed should never regress")
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
	level        ConsistentLevelType
	storageType  consistentStorage

	// rtsMap stores flushed and unflushed resolved timestamps for all tables.
	// it's just like map[tableID]*statefulRts.
	// For a given statefulRts, unflushed is updated in routine bgUpdateLog,
	// and flushed is updated in flushLog.
	rtsMap sync.Map

	writer        writer.RedoLogWriter
	logBuffer     *chann.Chann[cacheEvents]
	minResolvedTs uint64
	flushing      int64
	lastFlushTime time.Time

	metricWriteLogDuration prometheus.Observer
	metricFlushLogDuration prometheus.Observer
}

// NewManager creates a new Manager
func NewManager(ctx context.Context, cfg *config.ConsistentConfig, opts *ManagerOptions) (*ManagerImpl, error) {
	// return a disabled Manager if no consistent config or normal consistent level
	if cfg == nil || ConsistentLevelType(cfg.Level) == ConsistentLevelNone {
		return &ManagerImpl{enabled: false}, nil
	}
	if cfg.FlushIntervalInMs > flushIntervalInMs {
		flushIntervalInMs = cfg.FlushIntervalInMs
	}

	uri, err := storage.ParseRawURL(cfg.Storage)
	if err != nil {
		return nil, err
	}

	changeFeedID := contextutil.ChangefeedIDFromCtx(ctx)
	m := &ManagerImpl{
		changeFeedID:  changeFeedID,
		enabled:       true,
		level:         ConsistentLevelType(cfg.Level),
		storageType:   consistentStorage(uri.Scheme),
		rtsMap:        sync.Map{},
		logBuffer:     chann.New[cacheEvents](),
		minResolvedTs: math.MaxInt64,
		metricWriteLogDuration: common.RedoWriteLogDurationHistogram.
			WithLabelValues(changeFeedID.Namespace, changeFeedID.ID),
		metricFlushLogDuration: common.RedoFlushLogDurationHistogram.
			WithLabelValues(changeFeedID.Namespace, changeFeedID.ID),
	}

	switch m.storageType {
	case consistentStorageBlackhole:
		m.writer = writer.NewBlackHoleWriter()
	case consistentStorageLocal, consistentStorageNFS, consistentStorageS3:
		globalConf := config.GetGlobalServerConfig()
		// We use a temporary dir to storage redo logs before flushing to other backends, such as S3
		var redoDir string
		if changeFeedID.Namespace == model.DefaultNamespace {
			redoDir = filepath.Join(globalConf.DataDir,
				config.DefaultRedoDir, changeFeedID.ID)
		} else {
			redoDir = filepath.Join(globalConf.DataDir,
				config.DefaultRedoDir,
				changeFeedID.Namespace, changeFeedID.ID)
		}
		if m.storageType == consistentStorageLocal || m.storageType == consistentStorageNFS {
			// When using local or nfs as backend, store redo logs to redoDir directly.
			redoDir = uri.Path
		}

		writerCfg := &writer.LogWriterConfig{
			Dir:               redoDir,
			CaptureID:         contextutil.CaptureAddrFromCtx(ctx),
			ChangeFeedID:      changeFeedID,
			CreateTime:        time.Now(),
			MaxLogSize:        cfg.MaxLogSize,
			FlushIntervalInMs: cfg.FlushIntervalInMs,
			S3Storage:         m.storageType == consistentStorageS3,
		}
		if writerCfg.S3Storage {
			writerCfg.S3URI = *uri
		}
		writer, err := writer.NewLogWriter(ctx, writerCfg)
		if err != nil {
			return nil, err
		}
		m.writer = writer
	default:
		return nil, cerror.ErrConsistentStorage.GenWithStackByArgs(m.storageType)
	}

	if opts.EnableBgRunner {
		go m.bgUpdateLog(ctx, opts.ErrCh)
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
		Level:   string(ConsistentLevelEventual),
		Storage: "blackhole://",
	}
	errCh := make(chan error, 1)
	opts := &ManagerOptions{
		EnableBgRunner: true,
		ErrCh:          errCh,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
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
}

// UpdateResolvedTs asynchronously updates resolved ts of a single table.
func (m *ManagerImpl) UpdateResolvedTs(
	ctx context.Context,
	tableID model.TableID,
	resolvedTs uint64,
) error {
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
}

// EmitDDLEvent sends DDL event to redo log writer
func (m *ManagerImpl) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return m.writer.SendDDL(ctx, DDLToRedo(ddl))
}

// GetResolvedTs returns the resolved ts of a table
func (m *ManagerImpl) GetResolvedTs(tableID model.TableID) model.Ts {
	if value, ok := m.rtsMap.Load(tableID); ok {
		return value.(*statefulRts).getFlushed()
	}
	return 0
}

// GetMinResolvedTs returns the minimum resolved ts of all tables in this redo log manager
func (m *ManagerImpl) GetMinResolvedTs() model.Ts {
	return atomic.LoadUint64(&m.minResolvedTs)
}

// FlushResolvedAndCheckpointTs flushes resolved-ts and checkpoint-ts to redo log writer
func (m *ManagerImpl) FlushResolvedAndCheckpointTs(ctx context.Context, resolvedTs, checkpointTs uint64) (err error) {
	err = m.writer.EmitResolvedTs(ctx, resolvedTs)
	if err != nil {
		return
	}
	err = m.writer.EmitCheckpointTs(ctx, checkpointTs)
	return
}

// AddTable adds a new table in redo log manager
func (m *ManagerImpl) AddTable(tableID model.TableID, startTs uint64) {
	_, loaded := m.rtsMap.LoadOrStore(tableID, &statefulRts{flushed: startTs, unflushed: startTs})
	if loaded {
		log.Warn("add duplicated table in redo log manager", zap.Int64("tableID", tableID))
		return
	}

	if startTs < m.GetMinResolvedTs() {
		atomic.StoreUint64(&m.minResolvedTs, startTs)
	}
}

// RemoveTable removes a table from redo log manager
func (m *ManagerImpl) RemoveTable(tableID model.TableID) {
	if _, loaded := m.rtsMap.LoadAndDelete(tableID); !loaded {
		log.Warn("remove a table not maintained in redo log manager", zap.Int64("tableID", tableID))
	}
}

// Cleanup removes all redo logs of this manager, it is called when changefeed is removed
func (m *ManagerImpl) Cleanup(ctx context.Context) error {
	m.logBuffer.Close()
	// We must finish consuming the data here,
	// otherwise it will cause the channel to not close properly.
	for range m.logBuffer.Out() {
		// Do nothing. We do not care about the data.
	}
	common.RedoWriteLogDurationHistogram.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoFlushLogDurationHistogram.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	return m.writer.DeleteAllLogs(ctx)
}

func (m *ManagerImpl) flushLog(ctx context.Context, handleErr func(err error)) {
	if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
		log.Debug("Fail to update flush flag, " +
			"the previous flush operation hasn't finished yet")
		if time.Since(m.lastFlushTime) > flushTimeout {
			log.Warn("flushLog blocking too long, the redo manager may be stuck",
				zap.Duration("duration", time.Since(m.lastFlushTime)),
				zap.Any("changfeed", m.changeFeedID))
		}
		return
	}

	m.lastFlushTime = time.Now()
	go func() {
		defer atomic.StoreInt64(&m.flushing, 0)

		var minResolvedTs uint64 = math.MaxUint64
		var maxFlushedTs uint64 = 0
		tableRtsMap := make(map[model.TableID]model.Ts)
		m.rtsMap.Range(func(key interface{}, value interface{}) bool {
			tableID := key.(model.TableID)
			rts := value.(*statefulRts)
			unflushed := rts.getUnflushed()
			flushed := rts.getFlushed()
			if unflushed > flushed {
				tableRtsMap[tableID] = unflushed
				if unflushed < minResolvedTs {
					minResolvedTs = unflushed
				}
			} else if flushed > maxFlushedTs {
				maxFlushedTs = flushed
			}
			return true
		})
		if minResolvedTs > maxFlushedTs {
			minResolvedTs = maxFlushedTs
		}

		err := m.writer.FlushLog(ctx, tableRtsMap, minResolvedTs)
		m.metricFlushLogDuration.Observe(time.Since(m.lastFlushTime).Seconds())
		if err != nil {
			handleErr(err)
			return
		}

		if minResolvedTs > m.minResolvedTs {
			atomic.StoreUint64(&m.minResolvedTs, minResolvedTs)
		}
		for tableID, flushed := range tableRtsMap {
			if value, loaded := m.rtsMap.Load(tableID); loaded {
				rts := value.(*statefulRts)
				rts.setFlushed(flushed)
			}
		}
	}()

	return
}

func (m *ManagerImpl) bgUpdateLog(ctx context.Context, errCh chan<- error) {
	logErrCh := make(chan error, 1)
	handleErr := func(err error) {
		select {
		case logErrCh <- err:
		default:
		}
	}

	ticker := time.NewTicker(time.Duration(flushIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-logErrCh:
			select {
			case errCh <- err:
			default:
				log.Error("err channel in redoManager is full", zap.Error(err))
			}
			return
		case <-ticker.C:
			// interpolate tick message to flush writer if needed
			// TODO: add log and metrics
			m.flushLog(ctx, handleErr)
		case cache, ok := <-m.logBuffer.Out():
			if !ok {
				return // channel closed
			}
			switch cache.eventType {
			case model.MessageTypeRow:
				start := time.Now()
				logs := make([]*model.RedoRowChangedEvent, 0, len(cache.rows))
				for _, row := range cache.rows {
					logs = append(logs, RowToRedo(row))
				}
				_, err := m.writer.WriteLog(ctx, cache.tableID, logs)
				if err != nil {
					handleErr(err)
					return
				}
				m.metricWriteLogDuration.Observe(time.Since(start).Seconds())
			case model.MessageTypeResolved:
				value, _ := m.rtsMap.LoadOrStore(cache.tableID, &statefulRts{flushed: 0, unflushed: 0})
				value.(*statefulRts).checkAndSetUnflushed(cache.resolvedTs)
			default:
				log.Debug("handle unknown event type")
			}
		}
	}
}
