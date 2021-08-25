//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package writer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// RedoLogWriter ...
type RedoLogWriter interface {
	// WriteLog ...
	WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) (resolvedTs uint64, err error)
	// SendDDL ...
	SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error
	// 	FlushLog ...
	FlushLog(ctx context.Context, tableID int64, ts uint64) error
	// EmitCheckpointTs ...
	EmitCheckpointTs(ctx context.Context, ts uint64) error
	// EmitResolvedTs ...
	EmitResolvedTs(ctx context.Context, ts uint64) error
	// GetCurrentResolvedTs ...
	GetCurrentResolvedTs(ctx context.Context, tableIDs []int64) (resolvedTsList map[int64]uint64, err error)
}

var defaultGCIntervalInMs = 5000

var redoLogPool = sync.Pool{
	New: func() interface{} {
		return &model.RedoLog{}
	},
}

// LogWriterConfig is the configuration used by a Writer.
type LogWriterConfig struct {
	Dir          string
	ChangeFeedID string
	CaptureID    string
	CreateTime   time.Time
	// MaxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	MaxLogSize        int64
	FlushIntervalInMs int64
	S3Storage         bool
	// S3URI should be like SINK_URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
	S3URI *url.URL
}

// LogWriter ...
type LogWriter struct {
	cfg       *LogWriterConfig
	rowWriter fileWriter
	ddlWriter fileWriter
	storage   storage.ExternalStorage
	meta      *redo.LogMeta
	metaLock  sync.RWMutex
}

// NewLogWriter creates a LogWriter instance. need the client to guarantee only one LogWriter per changefeed
// TODO: delete log files when changefeed removed, metric
func NewLogWriter(ctx context.Context, cfg *LogWriterConfig) *LogWriter {
	if cfg == nil {
		log.Panic("LogWriterConfig can not be nil")
		return nil
	}

	rowCfg := &FileWriterConfig{
		Dir:               cfg.Dir,
		ChangeFeedID:      cfg.ChangeFeedID,
		CaptureID:         cfg.CaptureID,
		FileName:          redo.DefaultRowLogFileName,
		CreateTime:        cfg.CreateTime,
		MaxLogSize:        cfg.MaxLogSize,
		FlushIntervalInMs: cfg.FlushIntervalInMs,
	}
	ddlCfg := &FileWriterConfig{
		Dir:               cfg.Dir,
		ChangeFeedID:      cfg.ChangeFeedID,
		CaptureID:         cfg.CaptureID,
		FileName:          redo.DefaultDDLLogFileName,
		CreateTime:        cfg.CreateTime,
		MaxLogSize:        cfg.MaxLogSize,
		FlushIntervalInMs: cfg.FlushIntervalInMs,
	}
	logWriter := &LogWriter{
		rowWriter: NewWriter(ctx, rowCfg),
		ddlWriter: NewWriter(ctx, ddlCfg),
		meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
	}
	if cfg.S3Storage {
		s3storage, err := redo.InitS3storage(ctx, cfg.S3URI)
		if err != nil {
			log.Panic("initS3storage fail",
				zap.Error(err),
				zap.String("change feed", cfg.ChangeFeedID))
		}
		logWriter.storage = s3storage
	}

	go logWriter.runGC(ctx)
	return logWriter
}

func (l *LogWriter) runGC(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(defaultGCIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		if l.isStopped() {
			return
		}

		select {
		case <-ctx.Done():
			log.Info("runGC got canceled", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			err := l.gc()
			if err != nil {
				log.Error("redo log GC error", zap.Error(err))
			}
		}
	}
}

func (l *LogWriter) gc() error {
	var err error
	ts := atomic.LoadUint64(&l.meta.CheckPointTs)
	err = multierr.Append(err, l.rowWriter.GC(ts))
	err = multierr.Append(err, l.ddlWriter.GC(ts))
	return err
}

// WriteLog implement WriteLog api
func (l *LogWriter) WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return 0, cerror.ErrRedoWriterStopped
	}
	if len(rows) == 0 {
		return 0, nil
	}

	maxCommitTs := l.setMaxCommitTs(tableID, 0)
	for _, r := range rows {
		if r == nil || r.Row == nil {
			continue
		}

		rl := redoLogPool.Get().(*model.RedoLog)
		rl.Row = r
		rl.DDL = nil
		rl.Type = model.RedoLogTypeRow
		// TODO: crc check
		data, err := rl.MarshalMsg(nil)
		if err != nil {
			// TODO: just return 0 if err ?
			return maxCommitTs, cerror.WrapError(cerror.ErrMarshalFailed, err)
		}

		l.rowWriter.AdvanceTs(r.Row.CommitTs)
		_, err = l.rowWriter.Write(data)
		if err != nil {
			return maxCommitTs, err
		}
		maxCommitTs = l.setMaxCommitTs(tableID, r.Row.CommitTs)
		redoLogPool.Put(rl)
	}
	return maxCommitTs, nil
}

// SendDDL implement SendDDL api
func (l *LogWriter) SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}
	if ddl == nil || ddl.DDL == nil {
		return nil
	}

	rl := redoLogPool.Get().(*model.RedoLog)
	defer redoLogPool.Put(rl)

	rl.DDL = ddl
	rl.Row = nil
	rl.Type = model.RedoLogTypeDDL
	data, err := rl.MarshalMsg(nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	l.ddlWriter.AdvanceTs(ddl.DDL.CommitTs)
	_, err = l.ddlWriter.Write(data)
	return err
}

// FlushLog implement FlushLog api
func (l *LogWriter) FlushLog(ctx context.Context, tableID int64, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}

	if err := l.flush(); err != nil {
		return err
	}
	l.setMaxCommitTs(tableID, ts)
	return nil
}

// EmitCheckpointTs implement EmitCheckpointTs api
func (l *LogWriter) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}
	atomic.StoreUint64(&l.meta.CheckPointTs, ts)
	return l.flushLogMeta()
}

// EmitResolvedTs implement EmitResolvedTs api
func (l *LogWriter) EmitResolvedTs(ctx context.Context, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}
	atomic.StoreUint64(&l.meta.ResolvedTs, ts)

	return l.flushLogMeta()
}

// GetCurrentResolvedTs implement GetCurrentResolvedTs api
func (l *LogWriter) GetCurrentResolvedTs(ctx context.Context, tableIDs []int64) (map[int64]uint64, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	default:
	}

	if len(tableIDs) == 0 {
		return nil, nil
	}

	l.metaLock.RLock()
	defer l.metaLock.RUnlock()

	ret := map[int64]uint64{}
	for i := 0; i < len(tableIDs); i++ {
		id := tableIDs[i]
		if v, ok := l.meta.ResolvedTsList[id]; ok {
			ret[id] = v
		}
	}

	return ret, nil
}

// Close implements RedoLogWriter.Close.
func (l *LogWriter) Close() error {
	var err error
	err = multierr.Append(err, l.rowWriter.Close())
	err = multierr.Append(err, l.ddlWriter.Close())
	return err
}

func (l *LogWriter) setMaxCommitTs(tableID int64, commitTs uint64) uint64 {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	if v, ok := l.meta.ResolvedTsList[tableID]; ok {
		if v < commitTs {
			l.meta.ResolvedTsList[tableID] = commitTs
		}
	} else {
		l.meta.ResolvedTsList[tableID] = commitTs
	}

	return l.meta.ResolvedTsList[tableID]
}

// flush flushes all the buffered data to the disk.
func (l *LogWriter) flush() error {
	err1 := l.flushLogMeta()
	err2 := l.ddlWriter.Flush()
	err3 := l.rowWriter.Flush()

	err := multierr.Append(err1, err2)
	err = multierr.Append(err, err3)
	return err
}

func (l *LogWriter) isStopped() bool {
	return !l.ddlWriter.IsRunning() || !l.rowWriter.IsRunning()
}

func (l *LogWriter) getMetafileName() string {
	return fmt.Sprintf("%s_%s_%d_%s%s", l.cfg.CaptureID, l.cfg.ChangeFeedID, l.cfg.CreateTime.Unix(), redo.DefaultMetaFileName, redo.MetaEXT)
}

func (l *LogWriter) flushLogMeta() error {
	l.metaLock.Lock()

	data, err := l.meta.MarshalMsg(nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	err = os.MkdirAll(l.cfg.Dir, defaultDirMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't make dir for new redo logfile"))
	}

	tmpFileName := l.filePath() + redo.TmpEXT
	tmpFile, err := openTruncFile(tmpFileName)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	_, err = tmpFile.Write(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	err = tmpFile.Sync()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	err = os.Rename(tmpFileName, l.filePath())
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	l.metaLock.Unlock()

	if !l.cfg.S3Storage {
		return nil
	}
	return l.writeMetaToS3(context.Background())
}

func (l *LogWriter) writeMetaToS3(ctx context.Context) error {
	name := l.filePath()
	fileData, err := os.ReadFile(name)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	return cerror.WrapError(cerror.ErrS3StorageAPI, l.storage.WriteFile(ctx, name, fileData))
}

func (l *LogWriter) filePath() string {
	return filepath.Join(l.cfg.Dir, l.getMetafileName())
}
