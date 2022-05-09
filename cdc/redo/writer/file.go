// Copyright 2021 PingCAP, Inc.
// Copyright 2015 CoreOS, Inc.
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

package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/atomic"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/cdc/redo/common"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// pageBytes is the alignment for flushing records to the backing Writer.
	// It should be a multiple of the minimum sector size so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	pageBytes = 8 * common.MinSectorSize
)

const (
	defaultFlushIntervalInMs = 1000
	defaultS3Timeout         = 3 * time.Second
)

var (
	// for easy testing, not set to const
	megabyte          int64 = 1024 * 1024
	defaultMaxLogSize       = 64 * megabyte
)

//go:generate mockery --name=fileWriter --inpackage
type fileWriter interface {
	io.WriteCloser
	flusher

	// AdvanceTs receive the commitTs in the event from caller
	AdvanceTs(commitTs uint64)
	// GC run gc to remove useless files base on the checkPointTs
	GC(checkPointTs uint64) error
	// IsRunning check the fileWriter status
	IsRunning() bool
}

type flusher interface {
	Flush() error
}

// FileWriterConfig is the configuration used by a Writer.
type FileWriterConfig struct {
	Dir          string
	ChangeFeedID model.ChangeFeedID
	CaptureID    string
	FileType     string
	CreateTime   time.Time
	// MaxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	MaxLogSize        int64
	FlushIntervalInMs int64
	S3Storage         bool
	S3URI             url.URL
}

// Option define the writerOptions
type Option func(writer *writerOptions)

type writerOptions struct {
	getLogFileName func() string
}

// WithLogFileName provide the Option for fileName
func WithLogFileName(f func() string) Option {
	return func(o *writerOptions) {
		if f != nil {
			o.getLogFileName = f
		}
	}
}

// Writer is a redo log event Writer which writes redo log events to a file.
type Writer struct {
	cfg *FileWriterConfig
	op  *writerOptions
	// maxCommitTS is the max commitTS among the events in one log file
	maxCommitTS atomic.Uint64
	// the ts used in file name
	commitTS atomic.Uint64
	// the ts send with the event
	eventCommitTS atomic.Uint64
	running       atomic.Bool
	gcRunning     atomic.Bool
	size          int64
	file          *os.File
	bw            *pioutil.PageWriter
	uint64buf     []byte
	storage       storage.ExternalStorage
	sync.RWMutex

	metricFsyncDuration    prometheus.Observer
	metricFlushAllDuration prometheus.Observer
	metricWriteBytes       prometheus.Gauge
}

// NewWriter return a file rotated writer, TODO: extract to a common rotate Writer
func NewWriter(ctx context.Context, cfg *FileWriterConfig, opts ...Option) (*Writer, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("FileWriterConfig can not be nil"))
	}

	if cfg.FlushIntervalInMs == 0 {
		cfg.FlushIntervalInMs = defaultFlushIntervalInMs
	}
	cfg.MaxLogSize *= megabyte
	if cfg.MaxLogSize == 0 {
		cfg.MaxLogSize = defaultMaxLogSize
	}
	var s3storage storage.ExternalStorage
	if cfg.S3Storage {
		var err error
		s3storage, err = common.InitS3storage(ctx, cfg.S3URI)
		if err != nil {
			return nil, err
		}
	}

	op := &writerOptions{}
	for _, opt := range opts {
		opt(op)
	}
	w := &Writer{
		cfg:       cfg,
		op:        op,
		uint64buf: make([]byte, 8),
		storage:   s3storage,

		metricFsyncDuration: redoFsyncDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricFlushAllDuration: redoFlushAllDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricWriteBytes: redoWriteBytesGauge.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
	}

	w.running.Store(true)
	go w.runFlushToDisk(ctx, cfg.FlushIntervalInMs)

	return w, nil
}

func (w *Writer) runFlushToDisk(ctx context.Context, flushIntervalInMs int64) {
	ticker := time.NewTicker(time.Duration(flushIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		if !w.IsRunning() {
			return
		}

		select {
		case <-ctx.Done():
			err := w.Close()
			if err != nil {
				log.Error("runFlushToDisk close fail",
					zap.String("namespace", w.cfg.ChangeFeedID.Namespace),
					zap.String("changefeed", w.cfg.ChangeFeedID.ID),
					zap.Error(err))
			}
		case <-ticker.C:
			err := w.Flush()
			if err != nil {
				log.Error("redo log flush fail",
					zap.String("namespace", w.cfg.ChangeFeedID.Namespace),
					zap.String("changefeed", w.cfg.ChangeFeedID.ID), zap.Error(err))
			}
		}
	}
}

// Write implement write interface
// TODO: more general api with fileName generated by caller
func (w *Writer) Write(rawData []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	writeLen := int64(len(rawData))
	if writeLen > w.cfg.MaxLogSize {
		return 0, cerror.ErrFileSizeExceed.GenWithStackByArgs(writeLen, w.cfg.MaxLogSize)
	}

	if w.file == nil {
		if err := w.openOrNew(len(rawData)); err != nil {
			return 0, err
		}
	}

	if w.size+writeLen > w.cfg.MaxLogSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	if w.maxCommitTS.Load() < w.eventCommitTS.Load() {
		w.maxCommitTS.Store(w.eventCommitTS.Load())
	}
	// ref: https://github.com/etcd-io/etcd/pull/5250
	lenField, padBytes := encodeFrameSize(len(rawData))
	if err := w.writeUint64(lenField, w.uint64buf); err != nil {
		return 0, err
	}

	if padBytes != 0 {
		rawData = append(rawData, make([]byte, padBytes)...)
	}

	n, err := w.bw.Write(rawData)
	w.metricWriteBytes.Add(float64(n))
	w.size += int64(n)
	return n, err
}

// AdvanceTs implement Advance interface
func (w *Writer) AdvanceTs(commitTs uint64) {
	w.eventCommitTS.Store(commitTs)
}

func (w *Writer) writeUint64(n uint64, buf []byte) error {
	binary.LittleEndian.PutUint64(buf, n)
	v, err := w.bw.Write(buf)
	w.metricWriteBytes.Add(float64(v))

	return err
}

// the func uses code from etcd wal/encoder.go
// ref: https://github.com/etcd-io/etcd/pull/5250
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

// Close implements fileWriter.Close.
func (w *Writer) Close() error {
	w.Lock()
	defer w.Unlock()
	// always set to false when closed, since if having err may not get fixed just by retry
	defer w.running.Store(false)

	if !w.IsRunning() {
		return nil
	}

	redoFlushAllDurationHistogram.
		DeleteLabelValues(w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID)
	redoFsyncDurationHistogram.
		DeleteLabelValues(w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID)
	redoWriteBytesGauge.
		DeleteLabelValues(w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID)

	return w.close()
}

// IsRunning implement IsRunning interface
func (w *Writer) IsRunning() bool {
	return w.running.Load()
}

func (w *Writer) isGCRunning() bool {
	return w.gcRunning.Load()
}

func (w *Writer) close() error {
	if w.file == nil {
		return nil
	}
	err := w.flushAll()
	if err != nil {
		return err
	}

	// rename the file name from commitTs.log.tmp to maxCommitTS.log if closed safely
	// after rename, the file name could be used for search, since the ts is the max ts for all events in the file.
	w.commitTS.Store(w.maxCommitTS.Load())
	err = os.Rename(w.file.Name(), w.filePath())
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	if w.cfg.S3Storage {
		ctx, cancel := context.WithTimeout(context.Background(), defaultS3Timeout)
		defer cancel()

		err = w.renameInS3(ctx, w.file.Name(), w.filePath())
		if err != nil {
			return cerror.WrapError(cerror.ErrS3StorageAPI, err)
		}
	}

	err = w.file.Close()
	w.file = nil
	return cerror.WrapError(cerror.ErrRedoFileOp, err)
}

func (w *Writer) renameInS3(ctx context.Context, oldPath, newPath string) error {
	err := w.writeToS3(ctx, newPath)
	if err != nil {
		return cerror.WrapError(cerror.ErrS3StorageAPI, err)
	}
	return cerror.WrapError(cerror.ErrS3StorageAPI, w.storage.DeleteFile(ctx, filepath.Base(oldPath)))
}

func (w *Writer) getLogFileName() string {
	if w.op != nil && w.op.getLogFileName != nil {
		return w.op.getLogFileName()
	}
	if model.DefaultNamespace == w.cfg.ChangeFeedID.Namespace {
		return fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.CaptureID,
			w.cfg.ChangeFeedID.ID,
			w.cfg.CreateTime.Unix(), w.cfg.FileType, w.commitTS.Load(), common.LogEXT)
	}
	return fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", w.cfg.CaptureID,
		w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
		w.cfg.CreateTime.Unix(), w.cfg.FileType, w.commitTS.Load(), common.LogEXT)
}

func (w *Writer) filePath() string {
	return filepath.Join(w.cfg.Dir, w.getLogFileName())
}

func openTruncFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, common.DefaultFileMode)
}

func (w *Writer) openNew() error {
	err := os.MkdirAll(w.cfg.Dir, common.DefaultDirMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotatef(err, "can't make dir: %s for new redo logfile", w.cfg.Dir))
	}

	// reset ts used in file name when new file
	w.commitTS.Store(w.eventCommitTS.Load())
	w.maxCommitTS.Store(w.eventCommitTS.Load())
	path := w.filePath() + common.TmpEXT
	f, err := openTruncFile(path)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open new redo logfile"))
	}
	w.file = f
	w.size = 0
	err = w.newPageWriter()
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) openOrNew(writeLen int) error {
	path := w.filePath()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return w.openNew()
	}
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "error getting log file info"))
	}

	if info.Size()+int64(writeLen) >= w.cfg.MaxLogSize {
		return w.rotate()
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, common.DefaultFileMode)
	if err != nil {
		// return err let the caller decide next move
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	w.file = file
	w.size = info.Size()
	err = w.newPageWriter()
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) newPageWriter() error {
	offset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	w.bw = pioutil.NewPageWriter(w.file, pageBytes, int(offset))

	return nil
}

func (w *Writer) rotate() error {
	if err := w.close(); err != nil {
		return err
	}
	return w.openNew()
}

// GC implement GC interface
func (w *Writer) GC(checkPointTs uint64) error {
	if !w.IsRunning() || w.isGCRunning() {
		return nil
	}

	w.gcRunning.Store(true)
	defer w.gcRunning.Store(false)

	remove, err := w.getShouldRemovedFiles(checkPointTs)
	if err != nil {
		return err
	}

	var errs error
	for _, f := range remove {
		err := os.Remove(filepath.Join(w.cfg.Dir, f.Name()))
		errs = multierr.Append(errs, err)
	}

	if errs != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errs)
	}

	if w.cfg.S3Storage {
		// since if fail delete in s3, do not block any path, so just log the error if any
		go func() {
			var errs error
			for _, f := range remove {
				err := w.storage.DeleteFile(context.Background(), f.Name())
				errs = multierr.Append(errs, err)
			}
			if errs != nil {
				errs = cerror.WrapError(cerror.ErrS3StorageAPI, errs)
				log.Warn("delete redo log in s3 fail", zap.Error(errs))
			}
		}()
	}

	return nil
}

// shouldRemoved remove the file which commitTs in file name (max commitTs of all event ts in the file) < checkPointTs,
// since all event ts < checkPointTs already sent to sink, the log is not needed any more for recovery
func (w *Writer) shouldRemoved(checkPointTs uint64, f os.FileInfo) (bool, error) {
	if filepath.Ext(f.Name()) != common.LogEXT {
		return false, nil
	}

	commitTs, fileType, err := common.ParseLogFileName(f.Name())
	if err != nil {
		return false, err
	}

	return commitTs < checkPointTs && fileType == w.cfg.FileType, nil
}

func (w *Writer) getShouldRemovedFiles(checkPointTs uint64) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(w.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn("check removed log dir fail", zap.Error(err))
			return []os.FileInfo{}, nil
		}
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotatef(err, "can't read log file directory: %s", w.cfg.Dir))
	}

	logFiles := []os.FileInfo{}
	for _, f := range files {
		ret, err := w.shouldRemoved(checkPointTs, f)
		if err != nil {
			log.Warn("check removed log file fail",
				zap.String("logFile", f.Name()),
				zap.Error(err))
			continue
		}

		if ret {
			logFiles = append(logFiles, f)
		}
	}

	return logFiles, nil
}

func (w *Writer) flushAll() error {
	if w.file == nil {
		return nil
	}

	start := time.Now()
	err := w.flush()
	if err != nil {
		return err
	}
	if !w.cfg.S3Storage {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultS3Timeout)
	defer cancel()

	err = w.writeToS3(ctx, w.file.Name())
	w.metricFlushAllDuration.Observe(time.Since(start).Seconds())

	return err
}

// Flush implement Flush interface
func (w *Writer) Flush() error {
	w.Lock()
	defer w.Unlock()

	return w.flushAll()
}

func (w *Writer) flush() error {
	if w.file == nil {
		return nil
	}

	n, err := w.bw.FlushN()
	w.metricWriteBytes.Add(float64(n))
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	start := time.Now()
	err = w.file.Sync()
	w.metricFsyncDuration.Observe(time.Since(start).Seconds())

	return cerror.WrapError(cerror.ErrRedoFileOp, err)
}

func (w *Writer) writeToS3(ctx context.Context, name string) error {
	fileData, err := os.ReadFile(name)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	// Key in s3: aws.String(rs.options.Prefix + name), prefix should be changefeed name
	return cerror.WrapError(cerror.ErrS3StorageAPI, w.storage.WriteFile(ctx, filepath.Base(name), fileData))
}
