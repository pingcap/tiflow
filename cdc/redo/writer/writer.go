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
	"io"
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

//go:generate mockery --name=RedoLogWriter --inpackage
// RedoLogWriter defines the interfaces used to write redo log, all operations are thread-safe
type RedoLogWriter interface {
	io.Closer

	// WriteLog writer RedoRowChangedEvent to row log file
	WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) error

	// SendDDL writer RedoDDLEvent to ddl log file
	SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error

	// FlushLog sends resolved-ts from table pipeline to log writer, it is
	// essential to flush when a table doesn't have any row change event for
	// some time, and the resolved ts of this table should be moved forward.
	FlushLog(ctx context.Context, rtsMap map[model.TableID]model.Ts) error

	// EmitCheckpointTs write CheckpointTs to meta file
	EmitCheckpointTs(ctx context.Context, ts uint64) error

	// DeleteAllLogs delete all log files related to the changefeed, called from owner only when delete changefeed
	DeleteAllLogs(ctx context.Context) error
}

var defaultGCIntervalInMs = 5000

var (
	logWriters = map[model.ChangeFeedID]*LogWriter{}
	initLock   sync.Mutex
)

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
}

// LogWriter implement the RedoLogWriter interface
type LogWriter struct {
	cfg       *LogWriterConfig
	rowWriter fileWriter
	ddlWriter fileWriter
	storage   storage.ExternalStorage

	// Fields are protected by metaLock.
	meta     *common.LogMeta
	metaLock sync.RWMutex

	metricTotalRowsCount prometheus.Gauge
}

// NewLogWriter creates a LogWriter instance. It is guaranteed only one LogWriter per changefeed
func NewLogWriter(
	ctx context.Context, cfg *LogWriterConfig, opts ...Option,
) (*LogWriter, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("LogWriterConfig can not be nil"))
	}

	initLock.Lock()
	defer initLock.Unlock()

	if v, ok := logWriters[cfg.ChangeFeedID]; ok {
		// if cfg changed or already closed need create a new LogWriter
		if cfg.String() == v.cfg.String() && !v.isStopped() {
			return v, nil
		}
	}

	var err error
	var logWriter *LogWriter
	rowCfg := &FileWriterConfig{
		Dir:               cfg.Dir,
		ChangeFeedID:      cfg.ChangeFeedID,
		CaptureID:         cfg.CaptureID,
		FileType:          common.DefaultRowLogFileType,
		CreateTime:        cfg.CreateTime,
		MaxLogSize:        cfg.MaxLogSize,
		FlushIntervalInMs: cfg.FlushIntervalInMs,
		S3Storage:         cfg.S3Storage,
		S3URI:             cfg.S3URI,
	}
	ddlCfg := &FileWriterConfig{
		Dir:               cfg.Dir,
		ChangeFeedID:      cfg.ChangeFeedID,
		CaptureID:         cfg.CaptureID,
		FileType:          common.DefaultDDLLogFileType,
		CreateTime:        cfg.CreateTime,
		MaxLogSize:        cfg.MaxLogSize,
		FlushIntervalInMs: cfg.FlushIntervalInMs,
		S3Storage:         cfg.S3Storage,
		S3URI:             cfg.S3URI,
	}
	logWriter = &LogWriter{
		cfg: cfg,
	}
	logWriter.rowWriter, err = NewWriter(ctx, rowCfg, opts...)
	if err != nil {
		return nil, err
	}
	logWriter.ddlWriter, err = NewWriter(ctx, ddlCfg, opts...)
	if err != nil {
		return nil, err
	}

	// since the error will not block write log, so keep go to the next init process
	err = logWriter.initMeta(ctx)
	if err != nil {
		log.Warn("init redo meta fail",
			zap.String("namespace", cfg.ChangeFeedID.Namespace),
			zap.String("changefeed", cfg.ChangeFeedID.ID),
			zap.Error(err))
	}
	if cfg.S3Storage {
		logWriter.storage, err = common.InitS3storage(ctx, cfg.S3URI)
		if err != nil {
			return nil, err
		}
	}
	// close previous writer
	if v, ok := logWriters[cfg.ChangeFeedID]; ok {
		err = v.Close()
		if err != nil {
			return nil, err
		}
	} else {
		if cfg.S3Storage {
			// since other process get the remove changefeed job async, may still write some logs after owner delete the log
			err = logWriter.preCleanUpS3(ctx)
			if err != nil {
				return nil, err
			}
		}
	}

	logWriter.metricTotalRowsCount = common.RedoTotalRowsCountGauge.
		WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID)
	logWriters[cfg.ChangeFeedID] = logWriter
	go logWriter.runGC(ctx)
	return logWriter, nil
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

	l.meta = &common.LogMeta{ResolvedTsList: make(map[model.TableID]model.Ts)}

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

func (l *LogWriter) runGC(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(defaultGCIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		if l.isStopped() {
			return
		}

		select {
		case <-ctx.Done():
			err := l.Close()
			if err != nil {
				log.Error("runGC close fail",
					zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
					zap.String("changefeed", l.cfg.ChangeFeedID.ID), zap.Error(err))
			}
		case <-ticker.C:
			err := l.gc()
			if err != nil {
				log.Error("redo log GC fail",
					zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
					zap.String("changefeed", l.cfg.ChangeFeedID.ID), zap.Error(err))
			}
		}
	}
}

func (l *LogWriter) gc() error {
	l.metaLock.RLock()
	ts := l.meta.CheckPointTs
	l.metaLock.RUnlock()

	var err error
	err = multierr.Append(err, l.rowWriter.GC(ts))
	err = multierr.Append(err, l.ddlWriter.GC(ts))
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
		// TODO: crc check
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
func (l *LogWriter) FlushLog(ctx context.Context, rtsMap map[model.TableID]model.Ts) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}

	return l.flush(rtsMap)
}

// EmitCheckpointTs implement EmitCheckpointTs api
func (l *LogWriter) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	return l.flushLogMeta(ts, nil)
}

// DeleteAllLogs implement DeleteAllLogs api
func (l *LogWriter) DeleteAllLogs(ctx context.Context) error {
	err := l.Close()
	if err != nil {
		return err
	}

	if !l.cfg.S3Storage {
		err = os.RemoveAll(l.cfg.Dir)
		if err != nil {
			return cerror.WrapError(cerror.ErrRedoFileOp, err)
		}
		// after delete logs, rm the LogWriter since it is already closed
		l.cleanUpLogWriter()
		return nil
	}

	files, err := getAllFilesInS3(ctx, l)
	if err != nil {
		return err
	}

	err = l.deleteFilesInS3(ctx, files)
	if err != nil {
		return err
	}
	// after delete logs, rm the LogWriter since it is already closed
	l.cleanUpLogWriter()

	// write a marker to s3, since other process get the remove changefeed job async,
	// may still write some logs after owner delete the log
	return l.writeDeletedMarkerToS3(ctx)
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

func (l *LogWriter) cleanUpLogWriter() {
	initLock.Lock()
	defer initLock.Unlock()
	delete(logWriters, l.cfg.ChangeFeedID)
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
func (l *LogWriter) Close() error {
	common.RedoTotalRowsCountGauge.
		DeleteLabelValues(l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID)

	var err error
	err = multierr.Append(err, l.rowWriter.Close())
	err = multierr.Append(err, l.ddlWriter.Close())
	return err
}

// flush flushes all the buffered data to the disk.
func (l *LogWriter) flush(rtsMap map[model.TableID]model.Ts) error {
	err1 := l.ddlWriter.Flush()
	err2 := l.rowWriter.Flush()
	err3 := l.flushLogMeta(0, rtsMap)

	err := multierr.Append(err1, err2)
	err = multierr.Append(err, err3)
	return err
}

func (l *LogWriter) isStopped() bool {
	return !l.ddlWriter.IsRunning() || !l.rowWriter.IsRunning()
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

func (l *LogWriter) mergeMeta(checkPointTs uint64, rtsMap map[model.TableID]model.Ts) ([]byte, error) {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	// NOTE: both checkpoint and resolved can regress if a cdc instance restarts.
	hasChange := false
	if checkPointTs > l.meta.CheckPointTs {
		l.meta.CheckPointTs = checkPointTs
		hasChange = true
<<<<<<< HEAD
	} else if checkPointTs > 0 && checkPointTs != l.meta.CheckPointTs {
		log.Panic("flushLogMeta with a regressed checkpoint ts",
			zap.Uint64("currCheckPointTs", l.meta.CheckPointTs),
			zap.Uint64("recvCheckPointTs", checkPointTs))
	}

	for tID, ts := range rtsMap {
		if l.meta.ResolvedTsList[tID] == ts {
			continue
		}
		hasChange = true
		if l.meta.ResolvedTsList[tID] > ts {
			// Table resolved timestamp can regress if the table
			// is removed and then added back quickly.
			log.Warn("flushLogMeta with a regressed resolved ts",
				zap.Int64("tableID", tID),
				zap.Uint64("currResolvedTs", ts),
				zap.Uint64("recvResolvedTs", l.meta.ResolvedTsList[tID]))
		}
		l.meta.ResolvedTsList[tID] = ts
	}

	// If a table has been removed from the cdc instance, clear it from meta file
	// only after checkpoint has been advanced to its resolved timestamp.
	garbageTIDs := make([]model.TableID, 0)
	for tID, ts := range l.meta.ResolvedTsList {
		if _, ok := rtsMap[tID]; !ok {
			// NOTE: ts < l.meta.CheckPointTs means the table must have been
			// took over by other cdc instances.
			if ts < l.meta.CheckPointTs {
				garbageTIDs = append(garbageTIDs, tID)
			}
		}
=======
	} else if checkpointTs > 0 && checkpointTs != l.meta.CheckpointTs {
		log.Warn("flushLogMeta with a regressed checkpoint ts, ignore",
			zap.Uint64("currCheckpointTs", l.meta.CheckpointTs),
			zap.Uint64("recvCheckpointTs", checkpointTs))
>>>>>>> e351bacc5 (cdc: ignore redo log meta regress for restarting (#6320))
	}
	for _, tID := range garbageTIDs {
		delete(l.meta.ResolvedTsList, tID)
		hasChange = true
<<<<<<< HEAD
=======
	} else if resolvedTs > 0 && resolvedTs != l.meta.ResolvedTs {
		log.Warn("flushLogMeta with a regressed resolved ts, ignore",
			zap.Uint64("currCheckpointTs", l.meta.ResolvedTs),
			zap.Uint64("recvCheckpointTs", resolvedTs))
>>>>>>> e351bacc5 (cdc: ignore redo log meta regress for restarting (#6320))
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

func (l *LogWriter) flushLogMeta(checkPointTs uint64, rtsMap map[model.TableID]model.Ts) error {
	data, err := l.mergeMeta(checkPointTs, rtsMap)
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

	tmpFileName := l.filePath() + common.MetaTmpEXT
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
	err = tmpFile.Close()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	err = os.Rename(tmpFileName, l.filePath())
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
