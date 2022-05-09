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
	"io/ioutil"
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
	WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) (resolvedTs uint64, err error)

	// SendDDL EmitCheckpointTs and EmitResolvedTs are called from owner only
	// SendDDL writer RedoDDLEvent to ddl log file
	SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error

	// FlushLog sends resolved-ts from table pipeline to log writer, it is
	// essential to flush when a table doesn't have any row change event for
	// some time, and the resolved ts of this table should be moved forward.
	FlushLog(ctx context.Context, tableID int64, ts uint64) error

	// EmitCheckpointTs write CheckpointTs to meta file
	EmitCheckpointTs(ctx context.Context, ts uint64) error

	// EmitResolvedTs write ResolvedTs to meta file
	EmitResolvedTs(ctx context.Context, ts uint64) error

	// GetCurrentResolvedTs return all the ResolvedTs list for given tableIDs
	GetCurrentResolvedTs(ctx context.Context, tableIDs []int64) (resolvedTsList map[int64]uint64, err error)

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
	meta      *common.LogMeta
	metaLock  sync.RWMutex

	metricTotalRowsCount prometheus.Gauge
}

// NewLogWriter creates a LogWriter instance. It is guaranteed only one LogWriter per changefeed
func NewLogWriter(ctx context.Context, cfg *LogWriterConfig) (*LogWriter, error) {
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
	logWriter.rowWriter, err = NewWriter(ctx, rowCfg)
	if err != nil {
		return nil, err
	}
	logWriter.ddlWriter, err = NewWriter(ctx, ddlCfg)
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

	logWriter.metricTotalRowsCount = redoTotalRowsCountGauge.
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

	l.meta = &common.LogMeta{ResolvedTsList: map[int64]uint64{}}
	files, err := ioutil.ReadDir(l.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return cerror.WrapError(cerror.ErrRedoMetaInitialize, errors.Annotate(err, "can't read log file directory"))
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == common.MetaEXT {
			path := filepath.Join(l.cfg.Dir, file.Name())
			fileData, err := os.ReadFile(path)
			if err != nil {
				return cerror.WrapError(cerror.ErrRedoMetaInitialize, err)
			}

			_, err = l.meta.UnmarshalMsg(fileData)
			if err != nil {
				l.meta = &common.LogMeta{ResolvedTsList: map[int64]uint64{}}
				return cerror.WrapError(cerror.ErrRedoMetaInitialize, err)
			}
			break
		}
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
func (l *LogWriter) WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return 0, cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	if len(rows) == 0 {
		return 0, nil
	}

	maxCommitTs := l.setMaxCommitTs(tableID, 0)
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
			// TODO: just return 0 if err ?
			return maxCommitTs, cerror.WrapError(cerror.ErrMarshalFailed, err)
		}

		l.rowWriter.AdvanceTs(r.Row.CommitTs)
		_, err = l.rowWriter.Write(data)
		if err != nil {
			l.metricTotalRowsCount.Add(float64(i))
			return maxCommitTs, err
		}

		maxCommitTs = l.setMaxCommitTs(tableID, r.Row.CommitTs)
		redoLogPool.Put(rl)
	}
	l.metricTotalRowsCount.Add(float64(len(rows)))
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
func (l *LogWriter) FlushLog(ctx context.Context, tableID int64, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
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
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	return l.flushLogMeta(ts, 0)
}

// EmitResolvedTs implement EmitResolvedTs api
func (l *LogWriter) EmitResolvedTs(ctx context.Context, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped.GenWithStackByArgs()
	}

	return l.flushLogMeta(0, ts)
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

	// need to make sure all data received got saved already
	err := l.rowWriter.Flush()
	if err != nil {
		return nil, err
	}

	ret := map[int64]uint64{}
	for i := 0; i < len(tableIDs); i++ {
		id := tableIDs[i]
		if v, ok := l.meta.ResolvedTsList[id]; ok {
			ret[id] = v
		}
	}

	return ret, nil
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
	redoTotalRowsCountGauge.
		DeleteLabelValues(l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID)

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
	err1 := l.flushLogMeta(0, 0)
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
	if model.DefaultNamespace == l.cfg.ChangeFeedID.Namespace {
		return fmt.Sprintf("%s_%s_%s%s", l.cfg.CaptureID, l.cfg.ChangeFeedID.ID,
			common.DefaultMetaFileType, common.MetaEXT)
	}
	return fmt.Sprintf("%s_%s_%s_%s%s", l.cfg.CaptureID,
		l.cfg.ChangeFeedID.Namespace, l.cfg.ChangeFeedID.ID,
		common.DefaultMetaFileType, common.MetaEXT)
}

func (l *LogWriter) flushLogMeta(checkPointTs, resolvedTs uint64) error {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	if checkPointTs != 0 {
		l.meta.CheckPointTs = checkPointTs
	}
	if resolvedTs != 0 {
		l.meta.ResolvedTs = resolvedTs
	}
	data, err := l.meta.MarshalMsg(nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
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
