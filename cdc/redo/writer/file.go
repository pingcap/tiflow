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
	"github.com/pingcap/tiflow/cdc/redo/common"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/fsutil"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/atomic"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	// pageBytes is the alignment for flushing records to the backing Writer.
	// It should be a multiple of the minimum sector size so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	pageBytes = 8 * redo.MinSectorSize
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
	FileType     string
	ChangeFeedID model.ChangeFeedID
	CaptureID    string

	URI                url.URL
	UseExternalStorage bool

	// MaxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	MaxLogSize int64
	Dir        string
}

// Option define the writerOptions
type Option func(writer *writerOptions)

type writerOptions struct {
	getLogFileName   func() string
	getUUIDGenerator func() uuid.Generator
}

// WithLogFileName provide the Option for fileName
func WithLogFileName(f func() string) Option {
	return func(o *writerOptions) {
		if f != nil {
			o.getLogFileName = f
		}
	}
}

// WithUUIDGenerator provides the Option for uuid generator
func WithUUIDGenerator(f func() uuid.Generator) Option {
	return func(o *writerOptions) {
		if f != nil {
			o.getUUIDGenerator = f
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
	// record the filepath that is being written, and has not been flushed
	ongoingFilePath string
	bw              *pioutil.PageWriter
	uint64buf       []byte
	storage         storage.ExternalStorage
	sync.RWMutex
	uuidGenerator uuid.Generator
	allocator     *fsutil.FileAllocator

	metricFsyncDuration    prometheus.Observer
	metricFlushAllDuration prometheus.Observer
	metricWriteBytes       prometheus.Gauge
}

// NewWriter return a file rotated writer, TODO: extract to a common rotate Writer
func NewWriter(ctx context.Context, cfg *FileWriterConfig, opts ...Option) (*Writer, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("FileWriterConfig can not be nil"))
	}

	cfg.MaxLogSize *= megabyte
	if cfg.MaxLogSize == 0 {
		cfg.MaxLogSize = defaultMaxLogSize
	}
	var extStorage storage.ExternalStorage
	if cfg.UseExternalStorage {
		var err error
		extStorage, err = redo.InitExternalStorage(ctx, cfg.URI)
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
		storage:   extStorage,

		metricFsyncDuration: common.RedoFsyncDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricWriteBytes: common.RedoWriteBytesGauge.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
	}
	if w.op.getUUIDGenerator != nil {
		w.uuidGenerator = w.op.getUUIDGenerator()
	} else {
		w.uuidGenerator = uuid.NewGenerator()
	}

	if len(cfg.Dir) == 0 {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.New("invalid redo dir path"))
	}

	err := os.MkdirAll(cfg.Dir, redo.DefaultDirMode)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp,
			errors.Annotatef(err, "can't make dir: %s for redo writing", cfg.Dir))
	}

	// if we use S3 as the remote storage, a file allocator can be leveraged to
	// pre-allocate files for us.
	// TODO: test whether this improvement can also be applied to NFS.
	if cfg.UseExternalStorage {
		w.allocator = fsutil.NewFileAllocator(cfg.Dir, cfg.FileType, defaultMaxLogSize)
	}

	w.running.Store(true)
	return w, nil
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
		if err := w.openNew(); err != nil {
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
	if err != nil {
		return 0, err
	}
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

	if w.allocator != nil {
		w.allocator.Close()
		w.allocator = nil
	}

	if !w.IsRunning() {
		return nil
	}

	common.RedoFlushAllDurationHistogram.
		DeleteLabelValues(w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID)
	common.RedoFsyncDurationHistogram.
		DeleteLabelValues(w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID)
	common.RedoWriteBytesGauge.
		DeleteLabelValues(w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID)

	ctx, cancel := context.WithTimeout(context.Background(), redo.CloseTimeout)
	defer cancel()
	return w.close(ctx)
}

// IsRunning implement IsRunning interface
func (w *Writer) IsRunning() bool {
	return w.running.Load()
}

func (w *Writer) isGCRunning() bool {
	return w.gcRunning.Load()
}

func (w *Writer) close(ctx context.Context) error {
	if w.file == nil {
		return nil
	}

	if err := w.flush(); err != nil {
		return err
	}

	if w.cfg.UseExternalStorage {
		off, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// offset equals to 0 means that no written happened for current file,
		// we can simply return
		if off == 0 {
			return nil
		}
		// a file created by a file allocator needs to be truncated
		// to save disk space and network bandwidth.
		if err := w.file.Truncate(off); err != nil {
			return err
		}
	}

	// rename the file name from commitTs.log.tmp to maxCommitTS.log if closed safely
	// after rename, the file name could be used for search, since the ts is the max ts for all events in the file.
	w.commitTS.Store(w.maxCommitTS.Load())
	err := os.Rename(w.file.Name(), w.filePath())
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	dirFile, err := os.Open(w.cfg.Dir)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	defer dirFile.Close()
	// sync the dir to guarantee the renamed file is persisted to disk.
	err = dirFile.Sync()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	// We only write content to S3 before closing the local file.
	// By this way, we no longer need renaming object in S3.
	if w.cfg.UseExternalStorage {
		err = w.writeToS3(ctx, w.ongoingFilePath)
		if err != nil {
			w.file.Close()
			w.file = nil
			return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
		}
	}

	err = w.file.Close()
	w.file = nil
	return cerror.WrapError(cerror.ErrRedoFileOp, err)
}

func (w *Writer) getLogFileName() string {
	if w.op != nil && w.op.getLogFileName != nil {
		return w.op.getLogFileName()
	}
	uid := w.uuidGenerator.NewString()
	if model.DefaultNamespace == w.cfg.ChangeFeedID.Namespace {
		return fmt.Sprintf(redo.RedoLogFileFormatV1,
			w.cfg.CaptureID, w.cfg.ChangeFeedID.ID, w.cfg.FileType,
			w.commitTS.Load(), uid, redo.LogEXT)
	}
	return fmt.Sprintf(redo.RedoLogFileFormatV2,
		w.cfg.CaptureID, w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
		w.cfg.FileType, w.commitTS.Load(), uid, redo.LogEXT)
}

// filePath always creates a new, unique file path, note this function is not
// thread-safe, writer needs to ensure lock is acquired when calling it.
func (w *Writer) filePath() string {
	fp := filepath.Join(w.cfg.Dir, w.getLogFileName())
	w.ongoingFilePath = fp
	return fp
}

func openTruncFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, redo.DefaultFileMode)
}

func (w *Writer) openNew() error {
	err := os.MkdirAll(w.cfg.Dir, redo.DefaultDirMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp,
			errors.Annotatef(err, "can't make dir: %s for new redo logfile", w.cfg.Dir))
	}

	// reset ts used in file name when new file
	var f *os.File
	if w.allocator == nil {
		w.commitTS.Store(w.eventCommitTS.Load())
		w.maxCommitTS.Store(w.eventCommitTS.Load())
		path := w.filePath() + redo.TmpEXT
		f, err = openTruncFile(path)
		if err != nil {
			return cerror.WrapError(cerror.ErrRedoFileOp,
				errors.Annotate(err, "can't open new redolog file"))
		}
	} else {
		// if there is a file allocator, we use the pre-created file
		// supplied by the allocator to boost performance
		f, err = w.allocator.Open()
		if err != nil {
			return cerror.WrapError(cerror.ErrRedoFileOp,
				errors.Annotate(err, "can't open new redolog file with file allocator"))
		}
	}
	w.file = f
	w.size = 0
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
	ctx, cancel := context.WithTimeout(context.Background(), redo.DefaultTimeout)
	defer cancel()
	if err := w.close(ctx); err != nil {
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

	// FIXME: it will also delete other processor's files if the redo
	// storage is a remote NFS path. Try to only clean itself's files.
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

	if w.cfg.UseExternalStorage {
		// since if fail delete in s3, do not block any path, so just log the error if any
		go func() {
			var errs error
			for _, f := range remove {
				err := w.storage.DeleteFile(context.Background(), f.Name())
				errs = multierr.Append(errs, err)
			}
			if errs != nil {
				errs = cerror.WrapError(cerror.ErrExternalStorageAPI, errs)
				log.Warn("delete redo log in s3 fail", zap.Error(errs))
			}
		}()
	}

	return nil
}

// shouldRemoved remove the file which commitTs in file name (max commitTs of all event ts in the file) < checkPointTs,
// since all event ts < checkPointTs already sent to sink, the log is not needed any more for recovery
func (w *Writer) shouldRemoved(checkPointTs uint64, f os.FileInfo) (bool, error) {
	if filepath.Ext(f.Name()) != redo.LogEXT {
		return false, nil
	}

	commitTs, fileType, err := redo.ParseLogFileName(f.Name())
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

// flushAndRotateFile flushes the file to disk and rotate it if S3 storage is used.
func (w *Writer) flushAndRotateFile() error {
	if w.file == nil {
		return nil
	}

	start := time.Now()
	err := w.flush()
	if err != nil {
		return err
	}

	if !w.cfg.UseExternalStorage {
		return nil
	}

	if w.size == 0 {
		return nil
	}

	// for s3 storage, when the file is flushed to disk, we need an immediate
	// file rotate. Otherwise, the existing file content would be repeatedly written to S3,
	// which could cause considerable network bandwidth waste.
	err = w.rotate()
	if err != nil {
		return nil
	}

	w.metricFlushAllDuration.Observe(time.Since(start).Seconds())

	return err
}

// Flush implement Flush interface
func (w *Writer) Flush() error {
	w.Lock()
	defer w.Unlock()

	return w.flushAndRotateFile()
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
	err = w.storage.WriteFile(ctx, filepath.Base(name), fileData)
	if err != nil {
		return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}

	// in case the page cache piling up triggered the OS memory reclaming which may cause
	// I/O latency spike, we mandatorily drop the page cache of the file when it is successfully
	// written to S3.
	err = fsutil.DropPageCache(name)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	return nil
}
