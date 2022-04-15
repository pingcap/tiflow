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

	// The following 6 APIs are called from processor only
	TryEmitRowChangedEvents(ctx context.Context, tableID model.TableID, rows ...*model.RowChangedEvent) (bool, error)
	EmitRowChangedEvents(ctx context.Context, tableID model.TableID, rows ...*model.RowChangedEvent) error
	FlushLog(ctx context.Context, tableID model.TableID, resolvedTs uint64) error
	AddTable(tableID model.TableID, startTs uint64)
	RemoveTable(tableID model.TableID)
	GetMinResolvedTs() uint64

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
}

// ManagerImpl manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements LogManager interface.
type ManagerImpl struct {
	enabled     bool
	level       ConsistentLevelType
	storageType consistentStorage

	logBuffer chan cacheRows
	writer    writer.RedoLogWriter

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
		enabled:     true,
		level:       ConsistentLevelType(cfg.Level),
		storageType: consistentStorage(uri.Scheme),
		rtsMap:      make(map[model.TableID]uint64),
		logBuffer:   make(chan cacheRows, logBufferChanSize),
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

// TryEmitRowChangedEvents sends row changed events to a log buffer, the log buffer
// will be consumed by a background goroutine, which converts row changed events
// to redo logs and sends to log writer. Note this function is non-blocking
func (m *ManagerImpl) TryEmitRowChangedEvents(
	ctx context.Context,
	tableID model.TableID,
	rows ...*model.RowChangedEvent,
) (bool, error) {
	timer := time.NewTimer(logBufferTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case m.logBuffer <- cacheRows{
		tableID: tableID,
		// Because the pipeline sink doesn't hold slice memory after calling
		// EmitRowChangedEvents, we copy to a new slice to manage memory
		// in redo manager itself, which is the same behavior as sink manager.
		rows: append(make([]*model.RowChangedEvent, 0, len(rows)), rows...),
	}:
		return true, nil
	default:
		return false, nil
	}
}

// EmitRowChangedEvents sends row changed events to a log buffer, the log buffer
// will be consumed by a background goroutine, which converts row changed events
// to redo logs and sends to log writer. Note this function is non-blocking if
// the channel is not full, otherwise if the channel is always full after timeout,
// error ErrBufferLogTimeout will be returned.
// TODO: if the API is truly non-blocking, we should return an error immediately
// when the log buffer channel is full.
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
	return m.writer.FlushLog(ctx, tableID, resolvedTs)
}

// EmitDDLEvent sends DDL event to redo log writer
func (m *ManagerImpl) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return m.writer.SendDDL(ctx, DDLToRedo(ddl))
}

// GetMinResolvedTs returns the minimum resolved ts of all tables in this redo log manager
func (m *ManagerImpl) GetMinResolvedTs() uint64 {
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
	}
	m.rtsMap[tableID] = startTs
}

// RemoveTable removes a table from redo log manager
func (m *ManagerImpl) RemoveTable(tableID model.TableID) {
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
		log.Warn("remove a table not maintained in redo log manager", zap.Int64("tableID", tableID))
	}
}

// Cleanup removes all redo logs of this manager, it is called when changefeed is removed
func (m *ManagerImpl) Cleanup(ctx context.Context) error {
	return m.writer.DeleteAllLogs(ctx)
}

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
	for tableID, rts := range rtsMap {
		m.rtsMap[tableID] = rts
		if rts < minResolvedTs {
			minResolvedTs = rts
		}
	}
	atomic.StoreUint64(&m.minResolvedTs, minResolvedTs)
	return nil
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

func (m *ManagerImpl) bgWriteLog(ctx context.Context, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case cache := <-m.logBuffer:
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
			}
		}
	}
}
