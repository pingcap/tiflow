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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

var updateRtsInterval = time.Second

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

const (
	// supposing to replicate 10k tables, each table has one cached changce averagely.
	logBufferChanSize = 10000
	logBufferTimeout  = time.Minute * 10
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

	// The following 5 APIs are called from processor only
	EmitRowChangedEvents(ctx context.Context, tableID model.TableID, rows ...*model.RowChangedEvent) error
	FlushLog(ctx context.Context, tableID model.TableID, resolvedTs uint64) error
	AddTable(tableID model.TableID, startTs uint64)
	RemoveTable(tableID model.TableID)
	GetMinResolvedTs() uint64
<<<<<<< HEAD
=======
	EmitRowChangedEvents(ctx context.Context, tableID model.TableID, rows ...*model.RowChangedEvent) error
	UpdateResolvedTs(ctx context.Context, tableID model.TableID, resolvedTs uint64) error
	UpdateCheckpointTs(ctx context.Context, checkpointTs uint64) error
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))

	// EmitDDLEvent and FlushResolvedAndCheckpointTs are called from owner only
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error
	FlushResolvedAndCheckpointTs(ctx context.Context, resolvedTs, checkpointTs uint64) (err error)

	// Cleanup removes all redo logs
	Cleanup(ctx context.Context) error
}

// ManagerOptions defines options for redo log manager
type ManagerOptions struct {
	// whether to run background goroutine to fetch table resolved ts
	EnableBgRunner bool
	ErrCh          chan<- error
}

type cacheRows struct {
	tableID model.TableID
	rows    []*model.RowChangedEvent
	// When calling FlushLog for a table, we must ensure that all data of this
	// table has been written to underlying writer. Since the EmitRowChangedEvents
	// and FlushLog of the same table can't be executed concurrently, we can
	// insert a simple barrier data into data stream to achieve this goal.
	flushCallback chan struct{}
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
	enabled     bool
	level       ConsistentLevelType
	storageType consistentStorage

<<<<<<< HEAD
	logBuffer chan cacheRows
	writer    writer.RedoLogWriter
=======
	// rtsMap stores flushed and unflushed resolved timestamps for all tables.
	// it's just like map[tableID]*statefulRts.
	// For a given statefulRts, unflushed is updated in routine bgUpdateLog,
	// and flushed is updated in flushLog.
	rtsMap sync.Map
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))

	minResolvedTs uint64
	tableIDs      []model.TableID
	rtsMap        map[model.TableID]uint64
	rtsMapMu      sync.RWMutex

	// record whether there exists a table being flushing resolved ts
	flushing int64
}

// NewManager creates a new Manager
func NewManager(ctx context.Context, cfg *config.ConsistentConfig, opts *ManagerOptions) (*ManagerImpl, error) {
	// return a disabled Manager if no consistent config or normal consistent level
	if cfg == nil || ConsistentLevelType(cfg.Level) == ConsistentLevelNone {
		return &ManagerImpl{enabled: false}, nil
	}
	uri, err := storage.ParseRawURL(cfg.Storage)
	if err != nil {
		return nil, err
	}
	m := &ManagerImpl{
<<<<<<< HEAD
		enabled:     true,
		level:       ConsistentLevelType(cfg.Level),
		storageType: consistentStorage(uri.Scheme),
		rtsMap:      make(map[model.TableID]uint64),
		logBuffer:   make(chan cacheRows, logBufferChanSize),
=======
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
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
	}

	switch m.storageType {
	case consistentStorageBlackhole:
		m.writer = writer.NewBlackHoleWriter()
	case consistentStorageLocal, consistentStorageNFS, consistentStorageS3:
		globalConf := config.GetGlobalServerConfig()
		changeFeedID := util.ChangefeedIDFromCtx(ctx)
		// We use a temporary dir to storage redo logs before flushing to other backends, such as S3
		redoDir := filepath.Join(globalConf.DataDir, config.DefaultRedoDir, changeFeedID)
		if m.storageType == consistentStorageLocal || m.storageType == consistentStorageNFS {
			// When using local or nfs as backend, store redo logs to redoDir directly.
			redoDir = uri.Path
		}

		writerCfg := &writer.LogWriterConfig{
			Dir:               redoDir,
			CaptureID:         util.CaptureAddrFromCtx(ctx),
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
		go m.bgUpdateResolvedTs(ctx, opts.ErrCh)
		go m.bgWriteLog(ctx, opts.ErrCh)
	}
	return m, nil
}

// NewDisabledManager returns a disabled log manger instance, used in test only
func NewDisabledManager() *ManagerImpl {
	return &ManagerImpl{enabled: false}
}

// Enabled returns whether this log manager is enabled
func (m *ManagerImpl) Enabled() bool {
	return m.enabled
}

// EmitRowChangedEvents sends row changed events to a log buffer, the log buffer
// will be consumed by a background goroutine, which converts row changed events
// to redo logs and sends to log writer. Note this function is non-blocking if
// the channel is not full, otherwise if the channel is always full after timeout,
// error ErrBufferLogTimeout will be returned.
// TODO: if the API is truly non-blocking, we should return an error immediately
// when the log buffer channel is full.
// TODO: After buffer sink in sink node is removed, there is no batch mechanism
// before sending row changed events to redo manager, the original log buffer
// design may have performance issue.
func (m *ManagerImpl) EmitRowChangedEvents(
	ctx context.Context,
	tableID model.TableID,
	rows ...*model.RowChangedEvent,
) error {
	timer := time.NewTimer(logBufferTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil
	case <-timer.C:
		return cerror.ErrBufferLogTimeout.GenWithStackByArgs()
	case m.logBuffer <- cacheRows{
		tableID: tableID,
		// Because the pipeline sink doesn't hold slice memory after calling
		// EmitRowChangedEvents, we copy to a new slice to manage memory
		// in redo manager itself, which is the same behavior as sink manager.
		rows: append(make([]*model.RowChangedEvent, 0, len(rows)), rows...),
	}:
	}
	return nil
}

// FlushLog emits resolved ts of a single table
func (m *ManagerImpl) FlushLog(
	ctx context.Context,
	tableID model.TableID,
	resolvedTs uint64,
) error {
	// Use flushing as a lightweight lock to reduce log contention in log writer.
	if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
		return nil
	}
	defer atomic.StoreInt64(&m.flushing, 0)

	// Adding a barrier to data stream, to ensure all logs of this table has been
	// written to underlying writer.
	flushCallbackCh := make(chan struct{})
	m.logBuffer <- cacheRows{
		tableID:       tableID,
		flushCallback: flushCallbackCh,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-flushCallbackCh:
	}

	return m.writer.FlushLog(ctx, tableID, resolvedTs)
}

// EmitDDLEvent sends DDL event to redo log writer
func (m *ManagerImpl) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return m.writer.SendDDL(ctx, DDLToRedo(ddl))
}

<<<<<<< HEAD
=======
// GetResolvedTs returns the resolved ts of a table
func (m *ManagerImpl) GetResolvedTs(tableID model.TableID) model.Ts {
	if value, ok := m.rtsMap.Load(tableID); ok {
		return value.(*statefulRts).getFlushed()
	}
	panic("GetResolvedTs is called on an invalid table")
}

>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
// GetMinResolvedTs returns the minimum resolved ts of all tables in this redo log manager
func (m *ManagerImpl) GetMinResolvedTs() uint64 {
	return atomic.LoadUint64(&m.minResolvedTs)
}

// UpdateCheckpointTs updates checkpoint-ts to redo log writer
func (m *ManagerImpl) UpdateCheckpointTs(ctx context.Context, checkpointTs uint64) (err error) {
	err = m.writer.EmitCheckpointTs(ctx, checkpointTs)
	return
}

// AddTable adds a new table in redo log manager
func (m *ManagerImpl) AddTable(tableID model.TableID, startTs uint64) {
<<<<<<< HEAD
	m.rtsMapMu.Lock()
	defer m.rtsMapMu.Unlock()
	i := sort.Search(len(m.tableIDs), func(i int) bool {
		return m.tableIDs[i] >= tableID
	})
	if i < len(m.tableIDs) && m.tableIDs[i] == tableID {
		log.Warn("add duplicated table in redo log manager", zap.Int64("tableID", tableID))
		return
	}
	if i == len(m.tableIDs) {
		m.tableIDs = append(m.tableIDs, tableID)
	} else {
		m.tableIDs = append(m.tableIDs[:i+1], m.tableIDs[i:]...)
		m.tableIDs[i] = tableID
=======
	_, loaded := m.rtsMap.LoadOrStore(tableID, &statefulRts{flushed: startTs, unflushed: startTs})
	if loaded {
		log.Warn("add duplicated table in redo log manager", zap.Int64("tableID", tableID))
		return
	}

	if startTs < m.GetMinResolvedTs() {
		atomic.StoreUint64(&m.minResolvedTs, startTs)
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
	}
	m.rtsMap[tableID] = startTs
}

// RemoveTable removes a table from redo log manager
func (m *ManagerImpl) RemoveTable(tableID model.TableID) {
<<<<<<< HEAD
	m.rtsMapMu.Lock()
	defer m.rtsMapMu.Unlock()
	i := sort.Search(len(m.tableIDs), func(i int) bool {
		return m.tableIDs[i] >= tableID
	})
	if i < len(m.tableIDs) && m.tableIDs[i] == tableID {
		copy(m.tableIDs[i:], m.tableIDs[i+1:])
		m.tableIDs = m.tableIDs[:len(m.tableIDs)-1]
		delete(m.rtsMap, tableID)
	} else {
=======
	if _, loaded := m.rtsMap.LoadAndDelete(tableID); !loaded {
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
		log.Warn("remove a table not maintained in redo log manager", zap.Int64("tableID", tableID))
	}
}

// Cleanup removes all redo logs of this manager, it is called when changefeed is removed
func (m *ManagerImpl) Cleanup(ctx context.Context) error {
	return m.writer.DeleteAllLogs(ctx)
}

<<<<<<< HEAD
// updateTableResolvedTs reads rtsMap from redo log writer and calculate the minimum
// resolved ts of all maintaining tables.
func (m *ManagerImpl) updateTableResolvedTs(ctx context.Context) error {
	m.rtsMapMu.Lock()
	defer m.rtsMapMu.Unlock()
	rtsMap, err := m.writer.GetCurrentResolvedTs(ctx, m.tableIDs)
	if err != nil {
		return err
	}
	minResolvedTs := uint64(math.MaxUint64)
	for tableID := range m.rtsMap {
		if rts, ok := rtsMap[tableID]; ok {
			m.rtsMap[tableID] = rts
		}
		rts := m.rtsMap[tableID]
		if rts < minResolvedTs {
			minResolvedTs = rts
		}
	}
	atomic.StoreUint64(&m.minResolvedTs, minResolvedTs)
	return nil
=======
func (m *ManagerImpl) prepareForFlush() (tableRtsMap map[model.TableID]model.Ts, minResolvedTs model.Ts) {
	tableRtsMap = make(map[model.TableID]model.Ts)
	minResolvedTs = math.MaxUint64
	m.rtsMap.Range(func(key interface{}, value interface{}) bool {
		tableID := key.(model.TableID)
		rts := value.(*statefulRts)
		unflushed := rts.getUnflushed()
		flushed := rts.getFlushed()
		if unflushed > flushed {
			tableRtsMap[tableID] = unflushed
			flushed = unflushed
		}
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
	if minResolvedTs > m.minResolvedTs {
		atomic.StoreUint64(&m.minResolvedTs, minResolvedTs)
	}
	for tableID, flushed := range tableRtsMap {
		if value, loaded := m.rtsMap.Load(tableID); loaded {
			value.(*statefulRts).setFlushed(flushed)
		}
	}
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

		tableRtsMap, minResolvedTs := m.prepareForFlush()
		err := m.writer.FlushLog(ctx, tableRtsMap, minResolvedTs)
		m.metricFlushLogDuration.Observe(time.Since(m.lastFlushTime).Seconds())
		if err != nil {
			handleErr(err)
			return
		}
		m.postFlush(tableRtsMap, minResolvedTs)
	}()

	return
}

func (m *ManagerImpl) onResolvedTsMsg(tableID model.TableID, resolvedTs model.Ts) {
	value, loaded := m.rtsMap.Load(tableID)
	if !loaded {
		panic("onResolvedTsMsg is called for an invalid table")
	}
	value.(*statefulRts).checkAndSetUnflushed(resolvedTs)
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
}

func (m *ManagerImpl) bgUpdateResolvedTs(ctx context.Context, errCh chan<- error) {
	ticker := time.NewTicker(updateRtsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := m.updateTableResolvedTs(ctx)
			if err != nil {
				select {
				case errCh <- err:
				default:
					log.Error("err channel is full", zap.Error(err))
				}
				return
			}
		}
	}
}

<<<<<<< HEAD
func (m *ManagerImpl) bgWriteLog(ctx context.Context, errCh chan<- error) {
=======
	ticker := time.NewTicker(time.Duration(flushIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
	for {
		select {
		case <-ctx.Done():
			return
		case cache := <-m.logBuffer:
			if cache.flushCallback != nil {
				close(cache.flushCallback)
				continue
			}
<<<<<<< HEAD
			logs := make([]*model.RedoRowChangedEvent, 0, len(cache.rows))
			for _, row := range cache.rows {
				logs = append(logs, RowToRedo(row))
			}
			_, err := m.writer.WriteLog(ctx, cache.tableID, logs)
			if err != nil {
				select {
				case errCh <- err:
				default:
					log.Error("err channel is full", zap.Error(err))
				}
				return
=======
			return
		case <-ticker.C:
			// interpolate tick message to flush writer if needed
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
				m.onResolvedTsMsg(cache.tableID, cache.resolvedTs)
			default:
				log.Debug("handle unknown event type")
>>>>>>> a6569effe (cdc: flush meta correctly when flush log (#6207))
			}
		}
	}
}
