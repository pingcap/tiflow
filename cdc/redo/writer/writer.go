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
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/uuid"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// RedoLogWriter defines the interfaces used to write redo log, all operations are thread-safe.
type RedoLogWriter interface {
	// WriteLog writer RedoRowChangedEvent to row log file.
	WriteLog(ctx context.Context, rows []*model.RedoRowChangedEvent) error

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

// NewRedoLogWriter creates a new RedoLogWriter.
func NewRedoLogWriter(
	ctx context.Context,
	cfg *config.ConsistentConfig,
	fileTypeConfig redo.FileTypeConfig,
) (RedoLogWriter, error) {
	uri, err := storage.ParseRawURL(cfg.Storage)
	if err != nil {
		return nil, err
	}

	scheme := uri.Scheme
	if !redo.IsValidConsistentStorage(scheme) {
		return nil, cerror.ErrConsistentStorage.GenWithStackByArgs(scheme)
	}
	if redo.IsBlackholeStorage(scheme) {
		return NewBlackHoleWriter(), nil
	}

	lwCfg := &logWriterConfig{
		FileTypeConfig:     fileTypeConfig,
		CaptureID:          contextutil.CaptureAddrFromCtx(ctx),
		ChangeFeedID:       contextutil.ChangefeedIDFromCtx(ctx),
		URI:                *uri,
		UseExternalStorage: redo.IsExternalStorage(scheme),
		MaxLogSize:         cfg.MaxLogSize,
	}

	if lwCfg.UseExternalStorage {
		// When an external storage is used, we use redoDir as a temporary dir to store redo logs
		// before we flush them to S3.
		changeFeedID := contextutil.ChangefeedIDFromCtx(ctx)
		dataDir := config.GetGlobalServerConfig().DataDir
		if changeFeedID.Namespace == model.DefaultNamespace {
			lwCfg.Dir = filepath.Join(dataDir, config.DefaultRedoDir, changeFeedID.ID)
		} else {
			lwCfg.Dir = filepath.Join(dataDir, config.DefaultRedoDir,
				changeFeedID.Namespace, changeFeedID.ID)
		}
	} else {
		// When local storage or NFS is used, we use redoDir as the final storage path.
		lwCfg.Dir = uri.Path
	}

	return newLogWriter(ctx, lwCfg)
}

type logWriterConfig struct {
	redo.FileTypeConfig
	CaptureID    string
	ChangeFeedID model.ChangeFeedID

	URI                url.URL
	UseExternalStorage bool

	// MaxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	MaxLogSize int64
	Dir        string
}

// logWriter implement the RedoLogWriter interface
type logWriter struct {
	cfg       *logWriterConfig
	rowWriter fileWriter
	ddlWriter fileWriter
	// extStorage in LogWriter is used to write meta and clean up
	// the redo log files when changefeed is created or deleted.
	extStorage storage.ExternalStorage

	meta          *common.LogMeta
	preMetaFile   string
	uuidGenerator uuid.Generator
}

func newLogWriter(
	ctx context.Context, cfg *logWriterConfig, opts ...Option,
) (lw *logWriter, err error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("LogWriterConfig can not be nil"))
	}

	lw = &logWriter{cfg: cfg}

	writerOp := &writerOptions{}
	for _, opt := range opts {
		opt(writerOp)
	}
	if writerOp.getUUIDGenerator != nil {
		lw.uuidGenerator = writerOp.getUUIDGenerator()
	} else {
		lw.uuidGenerator = uuid.NewGenerator()
	}

	if lw.cfg.EmitRowEvents {
		writerCfg := &FileWriterConfig{
			FileType:           redo.RedoRowLogFileType,
			ChangeFeedID:       cfg.ChangeFeedID,
			CaptureID:          cfg.CaptureID,
			URI:                cfg.URI,
			UseExternalStorage: cfg.UseExternalStorage,
			MaxLogSize:         cfg.MaxLogSize,
			Dir:                cfg.Dir,
		}
		if lw.rowWriter, err = NewWriter(ctx, writerCfg, opts...); err != nil {
			return
		}
	}

	if lw.cfg.EmitDDLEvents {
		writerCfg := &FileWriterConfig{
			FileType:           redo.RedoDDLLogFileType,
			ChangeFeedID:       cfg.ChangeFeedID,
			CaptureID:          cfg.CaptureID,
			URI:                cfg.URI,
			UseExternalStorage: cfg.UseExternalStorage,
			MaxLogSize:         cfg.MaxLogSize,
			Dir:                cfg.Dir,
		}
		if lw.ddlWriter, err = NewWriter(ctx, writerCfg, opts...); err != nil {
			return
		}
	}

	if lw.cfg.EmitMeta {
		if err = lw.initMeta(ctx); err != nil {
			log.Warn("init redo meta fail",
				zap.String("namespace", cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", cfg.ChangeFeedID.ID),
				zap.Error(err))
			return
		}
	}

	if cfg.UseExternalStorage {
		lw.extStorage, err = redo.InitExternalStorage(ctx, cfg.URI)
		if err != nil {
			return nil, err
		}
		// since other process get the remove changefeed job async, may still write some logs after owner delete the log
		err = lw.preCleanUpS3(ctx)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (l *logWriter) preCleanUpS3(ctx context.Context) error {
	ret, err := l.extStorage.FileExists(ctx, l.getDeletedChangefeedMarker())
	if err != nil {
		return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
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
	err = l.extStorage.DeleteFile(ctx, l.getDeletedChangefeedMarker())
	if !isNotExistInS3(err) {
		return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}

	return nil
}

func (l *logWriter) initMeta(ctx context.Context) error {
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
func (l *logWriter) GC(ctx context.Context, ts model.Ts) error {
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
func (l *logWriter) WriteLog(ctx context.Context, rows []*model.RedoRowChangedEvent) error {
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

	for _, r := range rows {
		if r == nil || r.Row == nil {
			continue
		}

		rl := &model.RedoLog{RedoRow: r, Type: model.RedoLogTypeRow}
		data, err := rl.MarshalMsg(nil)
		if err != nil {
			return cerror.WrapError(cerror.ErrMarshalFailed, err)
		}

		l.rowWriter.AdvanceTs(r.Row.CommitTs)
		_, err = l.rowWriter.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

// SendDDL implement SendDDL api
func (l *logWriter) SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error {
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

	rl := &model.RedoLog{RedoDDL: ddl, Type: model.RedoLogTypeDDL}
	data, err := rl.MarshalMsg(nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	l.ddlWriter.AdvanceTs(ddl.DDL.CommitTs)
	_, err = l.ddlWriter.Write(data)
	return err
}

// FlushLog implement FlushLog api
func (l *logWriter) FlushLog(ctx context.Context, checkpointTs, resolvedTs model.Ts) error {
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
func (l *logWriter) GetMeta() (checkpointTs, resolvedTs model.Ts) {
	return l.meta.CheckpointTs, l.meta.ResolvedTs
}

// DeleteAllLogs implement DeleteAllLogs api
// FIXME: currently only owner will call this. We need to split it into 2 functions:
//  1. cleanLocalStorage, which should be called on processors;
//  2. cleanRemoteStorage, which should be called on owner.
func (l *logWriter) DeleteAllLogs(ctx context.Context) (err error) {
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

	if !l.cfg.UseExternalStorage {
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

func (l *logWriter) getDeletedChangefeedMarker() string {
	if l.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
		return fmt.Sprintf("delete_%s", l.cfg.ChangeFeedID.ID)
	}
	return fmt.Sprintf("delete_%s_%s", l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID)
}

func (l *logWriter) writeDeletedMarkerToS3(ctx context.Context) error {
	return cerror.WrapError(cerror.ErrExternalStorageAPI,
		l.extStorage.WriteFile(ctx, l.getDeletedChangefeedMarker(), []byte("D")))
}

func (l *logWriter) deleteFilesInS3(ctx context.Context, files []string) error {
	eg, eCtx := errgroup.WithContext(ctx)
	for _, f := range files {
		name := f
		eg.Go(func() error {
			err := l.extStorage.DeleteFile(eCtx, name)
			if err != nil {
				// if fail then retry, may end up with notExit err, ignore the error
				if !isNotExistInS3(err) {
					return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

func isNotExistInS3(err error) bool {
	// TODO: support other storage
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

var getAllFilesInS3 = func(ctx context.Context, l *logWriter) ([]string, error) {
	files := []string{}
	err := l.extStorage.WalkDir(ctx, &storage.WalkOption{}, func(path string, _ int64) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}

	return files, nil
}

// Close implements RedoLogWriter.Close.
func (l *logWriter) Close() (err error) {
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
func (l *logWriter) flush(checkpointTs, resolvedTs model.Ts) (err error) {
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

func (l *logWriter) isStopped() bool {
	var rowStopped, ddlStopped bool
	if l.cfg.EmitRowEvents {
		rowStopped = !l.rowWriter.IsRunning()
	}
	if l.cfg.EmitDDLEvents {
		ddlStopped = !l.ddlWriter.IsRunning()
	}
	return rowStopped || ddlStopped
}

func (l *logWriter) maybeUpdateMeta(checkpointTs, resolvedTs uint64) ([]byte, error) {
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

func (l *logWriter) flushLogMeta(checkpointTs, resolvedTs uint64) error {
	data, err := l.maybeUpdateMeta(checkpointTs, resolvedTs)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	if !l.cfg.UseExternalStorage {
		return l.flushMetaToLocal(data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultS3Timeout)
	defer cancel()
	return l.flushMetaToS3(ctx, data)
}

func (l *logWriter) flushMetaToLocal(data []byte) error {
	if err := os.MkdirAll(l.cfg.Dir, redo.DefaultDirMode); err != nil {
		e := errors.Annotate(err, "can't make dir for new redo logfile")
		return cerror.WrapError(cerror.ErrRedoFileOp, e)
	}

	metaFile, err := openTruncFile(l.filePath())
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	_, err = metaFile.Write(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	err = metaFile.Sync()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	if l.preMetaFile != "" {
		if err := os.Remove(l.preMetaFile); err != nil && !os.IsNotExist(err) {
			return cerror.WrapError(cerror.ErrRedoFileOp, err)
		}
	}
	l.preMetaFile = metaFile.Name()

	return metaFile.Close()
}

func (l *logWriter) flushMetaToS3(ctx context.Context, data []byte) error {
	start := time.Now()
	metaFile := l.getMetafileName()
	if err := l.extStorage.WriteFile(ctx, metaFile, data); err != nil {
		return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}

	if l.preMetaFile != "" {
		if err := l.extStorage.DeleteFile(ctx, l.preMetaFile); err != nil && !isNotExistInS3(err) {
			return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
		}
	}
	l.preMetaFile = metaFile
	log.Debug("flush meta to s3",
		zap.String("metaFile", metaFile),
		zap.Any("cost", time.Since(start).Milliseconds()))
	return nil
}

func (l *logWriter) getMetafileName() string {
	return fmt.Sprintf(redo.RedoMetaFileFormat, l.cfg.CaptureID,
		l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID,
		redo.RedoMetaFileType, l.uuidGenerator.NewString(), redo.MetaEXT)
}

func (l *logWriter) filePath() string {
	return filepath.Join(l.cfg.Dir, l.getMetafileName())
}

func (cfg logWriterConfig) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%d:%s:%t",
		cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID, cfg.CaptureID,
		cfg.Dir, cfg.MaxLogSize, cfg.URI.String(), cfg.UseExternalStorage)
}
