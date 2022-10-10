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
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// RedoLogWriter defines the interfaces used to write redo log, all operations are thread-safe.
//
//go:generate mockery --name=RedoLogWriter --inpackage
type RedoLogWriter interface {
	// WriteLog writer RedoRowChangedEvent to row log file.
	WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) error

	// SendDDL writer RedoDDLEvent to ddl log file.
	SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error

	// FlushLog flushes all rows written by `WriteLog` into redo storage.
	// `checkpointTs` and `resolvedTs` will be written into redo meta file.
	// Regressions on them will be ignored.
	FlushLog(ctx context.Context, checkpointTs, resolvedTs model.Ts) error

	// GetMeta gets current meta.
	GetMeta() (checkpointTs, resolvedTs model.Ts)

	// DeleteAllLogs delete all log files related to the changefeed, called from owner only.
	DeleteAllLogs(ctx context.Context) error

	// GC cleans stale files before the given checkpoint.
	GC(ctx context.Context, checkpointTs model.Ts) error

	// Close is used to close the writer.
	Close() error
}

var redoLogPool = sync.Pool{
	New: func() interface{} {
		return &model.RedoLog{}
	},
}

// LogWriterConfig is the configuration used by a Writer.
type LogWriterConfig struct {
	Dir          string
	ChangeFeedID model.ChangeFeedID
	CaptureID    string
	CreateTime   time.Time
	// MaxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	MaxLogSize        int64
	FlushIntervalInMs int64
	S3Storage         bool
	// S3URI should be like S3URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
	S3URI url.URL

	EmitMeta      bool
	EmitRowEvents bool
	EmitDDLEvents bool
}

// LogWriter implement the RedoLogWriter interface
type LogWriter struct {
	cfg       *LogWriterConfig
	rowWriter fileWriter
	ddlWriter fileWriter
	// storage in LogWriter is used to write meta and clean up
	// the redo log files when changefeed is created or deleted.
	storage storage.ExternalStorage

	meta *common.LogMeta

	metricTotalRowsCount prometheus.Gauge
}

// NewLogWriter creates a LogWriter instance. It is guaranteed only one LogWriter per changefeed
func NewLogWriter(
	ctx context.Context, cfg *LogWriterConfig, opts ...Option,
) (logWriter *LogWriter, err error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("LogWriterConfig can not be nil"))
	}

	logWriter = &LogWriter{cfg: cfg}

	if logWriter.cfg.EmitRowEvents {
		writerCfg := &FileWriterConfig{
			Dir:          cfg.Dir,
			ChangeFeedID: cfg.ChangeFeedID,
			CaptureID:    cfg.CaptureID,
			FileType:     common.DefaultRowLogFileType,
			CreateTime:   cfg.CreateTime,
			MaxLogSize:   cfg.MaxLogSize,
			S3Storage:    cfg.S3Storage,
			S3URI:        cfg.S3URI,
		}
		if logWriter.rowWriter, err = NewWriter(ctx, writerCfg, opts...); err != nil {
			return
		}
	}

	if logWriter.cfg.EmitDDLEvents {
		writerCfg := &FileWriterConfig{
			Dir:          cfg.Dir,
			ChangeFeedID: cfg.ChangeFeedID,
			CaptureID:    cfg.CaptureID,
			FileType:     common.DefaultDDLLogFileType,
			CreateTime:   cfg.CreateTime,
			MaxLogSize:   cfg.MaxLogSize,
			S3Storage:    cfg.S3Storage,
			S3URI:        cfg.S3URI,
		}
		if logWriter.ddlWriter, err = NewWriter(ctx, writerCfg, opts...); err != nil {
			return
		}
	}

	if logWriter.cfg.EmitMeta {
		if err = logWriter.initMeta(ctx); err != nil {
			log.Warn("init redo meta fail",
				zap.String("namespace", cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", cfg.ChangeFeedID.ID),
				zap.Error(err))
			return
		}
	}

	if cfg.S3Storage {
		logWriter.storage, err = common.InitS3storage(ctx, cfg.S3URI)
		if err != nil {
			return nil, err
		}
		// since other process get the remove changefeed job async, may still write some logs after owner delete the log
		err = logWriter.preCleanUpS3(ctx)
		if err != nil {
			return nil, err
		}
	}

	logWriter.metricTotalRowsCount = common.RedoTotalRowsCountGauge.
		WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID)
	return
}

func (l *LogWriter) preCleanUpS3(ctx context.Context) error {
	ret, err := l.storage.FileExists(ctx, l.getDeletedChangefeedMarker())
	if err != nil {
		return cerror.WrapError(cerror.ErrS3StorageAPI, err)
	}
	if !ret {
		return nil
	}

	files, err := getAllFilesInS3(ctx, l)
	if err != nil {
		return err
	}

	ff := []string{}
	for _, file := range files {
		if file != l.getDeletedChangefeedMarker() {
			ff = append(ff, file)
		}
	}
	err = l.deleteFilesInS3(ctx, ff)
	if err != nil {
		return err
	}
	err = l.storage.DeleteFile(ctx, l.getDeletedChangefeedMarker())
	if !isNotExistInS3(err) {
		return cerror.WrapError(cerror.ErrS3StorageAPI, err)
	}

	return nil
}

func (l *LogWriter) initMeta(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	l.meta = &common.LogMeta{}

	data, err := os.ReadFile(l.filePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return cerror.WrapError(cerror.ErrRedoMetaInitialize, errors.Annotate(err, "read meta file fail"))
	}

	_, err = l.meta.UnmarshalMsg(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoMetaInitialize, err)
	}

	return nil
}

// GC implement GC api
func (l *LogWriter) GC(ctx context.Context, ts model.Ts) error {
	var err error
	if l.cfg.EmitRowEvents {
		err = multierr.Append(err, l.rowWriter.GC(ts))
	}
	if l.cfg.EmitDDLEvents {
		err = multierr.Append(err, l.ddlWriter.GC(ts))
	}
	return err
}

// WriteLog implement WriteLog api
func (l *LogWriter) WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	if len(rows) == 0 {
		return nil
	}

	for i, r := range rows {
		if r == nil || r.Row == nil {
			continue
		}

		rl := redoLogPool.Get().(*model.RedoLog)
		rl.RedoRow = r
		rl.RedoDDL = nil
		rl.Type = model.RedoLogTypeRow
		data, err := rl.MarshalMsg(nil)
		if err != nil {
			return cerror.WrapError(cerror.ErrMarshalFailed, err)
		}

		l.rowWriter.AdvanceTs(r.Row.CommitTs)
		_, err = l.rowWriter.Write(data)
		if err != nil {
			l.metricTotalRowsCount.Add(float64(i))
			return err
		}

		redoLogPool.Put(rl)
	}
	l.metricTotalRowsCount.Add(float64(len(rows)))
	return nil
}

// SendDDL implement SendDDL api
func (l *LogWriter) SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	if ddl == nil || ddl.DDL == nil {
		return nil
	}

	rl := redoLogPool.Get().(*model.RedoLog)
	defer redoLogPool.Put(rl)

	rl.RedoDDL = ddl
	rl.RedoRow = nil
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
func (l *LogWriter) FlushLog(ctx context.Context, checkpointTs, resolvedTs model.Ts) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}

	return l.flush(checkpointTs, resolvedTs)
}

// GetMeta implement GetMeta api
func (l *LogWriter) GetMeta() (checkpointTs, resolvedTs model.Ts) {
	return l.meta.CheckpointTs, l.meta.ResolvedTs
}

// DeleteAllLogs implement DeleteAllLogs api
// FIXME: currently only owner will call this. We need to split it into 2 functions:
//  1. cleanLocalStorage, which should be called on processors;
//  2. cleanRemoteStorage, which should be called on owner.
func (l *LogWriter) DeleteAllLogs(ctx context.Context) (err error) {
	if err = l.Close(); err != nil {
		return
	}

	localFiles, err := os.ReadDir(l.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn("read removed log dir fail", zap.Error(err))
			return nil
		}
		return cerror.WrapError(cerror.ErrRedoFileOp,
			errors.Annotatef(err, "can't read log file directory: %s", l.cfg.Dir))
	}

	fileNames := make([]string, 0, len(localFiles))
	for _, file := range localFiles {
		fileNames = append(fileNames, file.Name())
	}
	filteredFiles := common.FilterChangefeedFiles(fileNames, l.cfg.ChangeFeedID)

	if len(filteredFiles) == len(fileNames) {
		if err = os.RemoveAll(l.cfg.Dir); err != nil {
			if os.IsNotExist(err) {
				log.Warn("removed log dir fail", zap.Error(err))
				return nil
			}
			return cerror.WrapError(cerror.ErrRedoFileOp, err)
		}
	} else {
		for _, file := range filteredFiles {
			if err = os.RemoveAll(filepath.Join(l.cfg.Dir, file)); err != nil {
				if os.IsNotExist(err) {
					log.Warn("removed log dir fail", zap.Error(err))
					return nil
				}
				return cerror.WrapError(cerror.ErrRedoFileOp, err)
			}
		}
	}

	if !l.cfg.S3Storage {
		return
	}

	var remoteFiles []string
	remoteFiles, err = getAllFilesInS3(ctx, l)
	if err != nil {
		return err
	}
	filteredFiles = common.FilterChangefeedFiles(remoteFiles, l.cfg.ChangeFeedID)
	err = l.deleteFilesInS3(ctx, filteredFiles)
	if err != nil {
		return
	}

	// Write deleted mark before clean any files.
	err = l.writeDeletedMarkerToS3(ctx)
	log.Info("redo manager write deleted mark",
		zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
		zap.String("changefeed", l.cfg.ChangeFeedID.ID),
		zap.Error(err))
	return
}

func (l *LogWriter) getDeletedChangefeedMarker() string {
	if l.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
		return fmt.Sprintf("delete_%s", l.cfg.ChangeFeedID.ID)
	}
	return fmt.Sprintf("delete_%s_%s", l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID)
}

func (l *LogWriter) writeDeletedMarkerToS3(ctx context.Context) error {
	return cerror.WrapError(cerror.ErrS3StorageAPI, l.storage.WriteFile(ctx, l.getDeletedChangefeedMarker(), []byte("D")))
}

func (l *LogWriter) deleteFilesInS3(ctx context.Context, files []string) error {
	eg, eCtx := errgroup.WithContext(ctx)
	for _, f := range files {
		name := f
		eg.Go(func() error {
			err := l.storage.DeleteFile(eCtx, name)
			if err != nil {
				// if fail then retry, may end up with notExit err, ignore the error
				if !isNotExistInS3(err) {
					return cerror.WrapError(cerror.ErrS3StorageAPI, err)
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

func isNotExistInS3(err error) bool {
	if err != nil {
		if aerr, ok := errors.Cause(err).(awserr.Error); ok { // nolint:errorlint
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return true
			}
		}
	}
	return false
}

var getAllFilesInS3 = func(ctx context.Context, l *LogWriter) ([]string, error) {
	files := []string{}
	err := l.storage.WalkDir(ctx, &storage.WalkOption{}, func(path string, _ int64) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageAPI, err)
	}

	return files, nil
}

// Close implements RedoLogWriter.Close.
func (l *LogWriter) Close() (err error) {
	common.RedoTotalRowsCountGauge.
		DeleteLabelValues(l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID)

	if l.cfg.EmitRowEvents {
		err = multierr.Append(err, l.rowWriter.Close())
	}
	if l.cfg.EmitDDLEvents {
		err = multierr.Append(err, l.ddlWriter.Close())
	}
	return
}

// flush flushes all the buffered data to the disk.
func (l *LogWriter) flush(checkpointTs, resolvedTs model.Ts) (err error) {
	if l.cfg.EmitDDLEvents {
		err = multierr.Append(err, l.ddlWriter.Flush())
	}
	if l.cfg.EmitRowEvents {
		err = multierr.Append(err, l.rowWriter.Flush())
	}
	if l.cfg.EmitMeta {
		err = multierr.Append(err, l.flushLogMeta(checkpointTs, resolvedTs))
	}
	return
}

func (l *LogWriter) isStopped() bool {
	var rowStopped, ddlStopped bool
	if l.cfg.EmitRowEvents {
		rowStopped = !l.rowWriter.IsRunning()
	}
	if l.cfg.EmitDDLEvents {
		ddlStopped = !l.ddlWriter.IsRunning()
	}
	return rowStopped || ddlStopped
}

func (l *LogWriter) getMetafileName() string {
	if model.DefaultNamespace == l.cfg.ChangeFeedID.Namespace {
		return fmt.Sprintf("%s_%s_%s%s", l.cfg.CaptureID, l.cfg.ChangeFeedID.ID,
			common.DefaultMetaFileType, common.MetaEXT)
	}
	return fmt.Sprintf("%s_%s_%s_%s%s", l.cfg.CaptureID,
		l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID,
		common.DefaultMetaFileType, common.MetaEXT)
}

func (l *LogWriter) maybeUpdateMeta(checkpointTs, resolvedTs uint64) ([]byte, error) {
	// NOTE: both checkpoint and resolved can regress if a cdc instance restarts.
	hasChange := false
	if checkpointTs > l.meta.CheckpointTs {
		l.meta.CheckpointTs = checkpointTs
		hasChange = true
	} else if checkpointTs > 0 && checkpointTs != l.meta.CheckpointTs {
		log.Warn("flushLogMeta with a regressed checkpoint ts, ignore",
			zap.Uint64("currCheckpointTs", l.meta.CheckpointTs),
			zap.Uint64("recvCheckpointTs", checkpointTs),
			zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", l.cfg.ChangeFeedID.ID))
	}
	if resolvedTs > l.meta.ResolvedTs {
		l.meta.ResolvedTs = resolvedTs
		hasChange = true
	} else if resolvedTs > 0 && resolvedTs != l.meta.ResolvedTs {
		log.Warn("flushLogMeta with a regressed resolved ts, ignore",
			zap.Uint64("currResolvedTs", l.meta.ResolvedTs),
			zap.Uint64("recvResolvedTs", resolvedTs),
			zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", l.cfg.ChangeFeedID.ID))
	}

	if !hasChange {
		return nil, nil
	}

	data, err := l.meta.MarshalMsg(nil)
	if err != nil {
		err = cerror.WrapError(cerror.ErrMarshalFailed, err)
	}
	return data, err
}

func (l *LogWriter) flushLogMeta(checkpointTs, resolvedTs uint64) error {
	data, err := l.maybeUpdateMeta(checkpointTs, resolvedTs)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	err = os.MkdirAll(l.cfg.Dir, common.DefaultDirMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't make dir for new redo logfile"))
	}

	// we will create a temp metadata file and then atomically rename it.
	tmpFileName := l.filePath() + common.MetaTmpEXT
	tmpFile, err := openTruncFile(tmpFileName)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	defer tmpFile.Close()

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

	dirFile, err := os.Open(l.cfg.Dir)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	defer dirFile.Close()
	// sync the dir to guarantee the renamed file is persisted to disk.
	err = dirFile.Sync()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	if !l.cfg.S3Storage {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultS3Timeout)
	defer cancel()
	return l.writeMetaToS3(ctx)
}

func (l *LogWriter) writeMetaToS3(ctx context.Context) error {
	name := l.filePath()
	fileData, err := os.ReadFile(name)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	return cerror.WrapError(cerror.ErrS3StorageAPI, l.storage.WriteFile(ctx, l.getMetafileName(), fileData))
}

func (l *LogWriter) filePath() string {
	return filepath.Join(l.cfg.Dir, l.getMetafileName())
}

func (cfg LogWriterConfig) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%d:%d:%s:%t",
		cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID,
		cfg.CaptureID, cfg.Dir, cfg.MaxLogSize,
		cfg.FlushIntervalInMs, cfg.S3URI.String(), cfg.S3Storage)
}
