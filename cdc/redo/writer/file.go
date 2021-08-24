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

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/redo"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/uber-go/atomic"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	// pageBytes is the alignment for flushing records to the backing Writer.
	// It should be a multiple of the minimum sector size so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	pageBytes = 8 * redo.MinSectorSize
)

const (
	defaultDirMode = 0o755

	defaultFlushIntervalInMs = 1000
)

const (
	// stopped defines the state value of a writer which has been stopped
	stopped uint32 = 0
	// started defines the state value of a writer which is currently started
	started uint32 = 1
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

	// AdvanceTs ...
	AdvanceTs(commitTs uint64)
	// GC ...
	GC(checkPointTs uint64) error
	// IsRunning ...
	IsRunning() bool
}

type flusher interface {
	Flush() error
}

// writerConfig is the configuration used by a writer.
type writerConfig struct {
	dir          string
	changeFeedID string
	captureID    string
	fileName     string
	createTime   time.Time
	// maxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	maxLogSize        int64
	flushIntervalInMs int64
	s3Storage         bool
	s3URI             *url.URL
}

// writer is a redo log event writer which writes redo log events to a file.
type writer struct {
	cfg *writerConfig
	// maxCommitTS is the max commitTS among the events in one log file
	maxCommitTS atomic.Uint64
	// the ts used in file name
	commitTS atomic.Uint64
	// the ts send with the event
	eventCommitTS atomic.Uint64
	state         atomic.Uint32
	gcRunning     atomic.Bool
	size          int64
	file          *os.File
	bw            *pioutil.PageWriter
	uint64buf     []byte
	storage       storage.ExternalStorage
	sync.RWMutex
}

// TODO: extract to a common rotate writer
func newWriter(ctx context.Context, cfg *writerConfig) *writer {
	if cfg == nil {
		log.Panic("writerConfig can not be nil")
		return nil
	}

	if cfg.flushIntervalInMs == 0 {
		cfg.flushIntervalInMs = defaultFlushIntervalInMs
	}
	cfg.maxLogSize *= megabyte
	if cfg.maxLogSize == 0 {
		cfg.maxLogSize = defaultMaxLogSize
	}
	if cfg.s3Storage {
		if cfg.s3URI == nil {
			log.Panic("S3URI can not be nil",
				zap.String("change feed", cfg.changeFeedID))
		}
	}

	w := &writer{
		cfg:       cfg,
		uint64buf: make([]byte, 8),
	}

	if cfg.s3Storage {
		s3storage, err := redo.InitS3storage(ctx, cfg.s3URI)
		if err != nil {
			log.Panic("initS3storage fail",
				zap.Error(err),
				zap.String("change feed", cfg.changeFeedID))
		}
		w.storage = s3storage
	}

	w.state.Store(started)
	go w.runFlushToDisk(ctx, cfg.flushIntervalInMs)

	return w
}

func (w *writer) runFlushToDisk(ctx context.Context, flushIntervalInMs int64) {
	ticker := time.NewTicker(time.Duration(flushIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		if !w.IsRunning() {
			return
		}

		select {
		case <-ctx.Done():
			log.Info("runFlushToDisk got canceled", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			err := w.Flush()
			if err != nil {
				log.Error("redo log flush error", zap.Error(err))
			}
		}
	}
}

func (w *writer) Write(rawData []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	writeLen := int64(len(rawData))
	if writeLen > w.cfg.maxLogSize {
		return 0, errors.Errorf("rawData %d exceeds maximum file size %d", writeLen, w.cfg.maxLogSize)
	}

	if w.file == nil {
		if err := w.openOrNew(len(rawData)); err != nil {
			return 0, err
		}
	}

	if w.size+writeLen > w.cfg.maxLogSize {
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
	w.size += int64(n)
	return n, err
}

// AdvanceTs ...
func (w *writer) AdvanceTs(commitTs uint64) {
	w.eventCommitTS.Store(commitTs)
}

func (w *writer) writeUint64(n uint64, buf []byte) error {
	binary.LittleEndian.PutUint64(buf, n)
	_, err := w.bw.Write(buf)
	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

// Close implements Writer.Close.
func (w *writer) Close() error {
	if !w.IsRunning() {
		return nil
	}
	w.Lock()
	defer w.Unlock()

	err := w.close()
	if err != nil {
		return err
	}

	w.state.Store(stopped)
	return nil
}

func (w *writer) IsRunning() bool {
	return w.state.Load() == started
}

func (w *writer) isGCRunning() bool {
	return w.gcRunning.Load()
}

func (w *writer) close() error {
	if w.file == nil {
		return nil
	}
	err := w.flushAll()
	if err != nil {
		return err
	}

	// rename the file name from commitTs.log.tmp to maxCommitTS.log if closed safely
	w.commitTS.Store(w.maxCommitTS.Load())
	err = os.Rename(w.file.Name(), w.filePath())
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	err = w.file.Close()
	w.file = nil
	return cerror.WrapError(cerror.ErrRedoFileOp, err)
}

func (w *writer) getLogFileName() string {
	return fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileName, w.commitTS.Load(), redo.LogEXT)
}

func (w *writer) getLogFileNameFormat(ext string) string {
	return fmt.Sprintf("%s_%s_%d_%s_%s%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileName, "%d", ext)
}

func (w *writer) filePath() string {
	return filepath.Join(w.cfg.dir, w.getLogFileName())
}

func openTruncFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, redo.DefaultFileMode)
}

func (w *writer) openNew() error {
	err := os.MkdirAll(w.cfg.dir, defaultDirMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't make dir for new redo logfile"))
	}

	// reset ts used in file name when new file
	w.commitTS.Store(w.eventCommitTS.Load())
	w.maxCommitTS.Store(w.eventCommitTS.Load())
	path := w.filePath() + redo.TmpEXT
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

func (w *writer) openOrNew(writeLen int) error {
	path := w.filePath()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return w.openNew()
	}
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "error getting log file info"))
	}

	if info.Size()+int64(writeLen) >= w.cfg.maxLogSize {
		return w.rotate()
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, redo.DefaultFileMode)
	if err != nil {
		return w.openNew()
	}
	w.file = file
	w.size = info.Size()
	err = w.newPageWriter()
	if err != nil {
		return err
	}
	return nil
}

func (w *writer) newPageWriter() error {
	offset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	w.bw = pioutil.NewPageWriter(w.file, pageBytes, int(offset))

	return nil
}

func (w *writer) rotate() error {
	if err := w.close(); err != nil {
		return err
	}
	return w.openNew()
}

// GC TODO: GC in s3
func (w *writer) GC(checkPointTs uint64) error {
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
		err := os.Remove(filepath.Join(w.cfg.dir, f.Name()))
		errs = multierr.Append(errs, err)
	}

	return cerror.WrapError(cerror.ErrRedoFileOp, errs)
}

func (w *writer) parseLogFileName(name string) (uint64, error) {
	if filepath.Ext(name) != redo.LogEXT && filepath.Ext(name) != redo.TmpEXT {
		return 0, errors.New("bad log name, file name extension not match")
	}

	var commitTs uint64
	format := w.getLogFileNameFormat(filepath.Ext(name))
	_, err := fmt.Sscanf(name, format, &commitTs)
	if err != nil {
		return 0, errors.New("bad log name")
	}
	return commitTs, nil
}

func (w *writer) shouldRemoved(checkPointTs uint64, f os.FileInfo) (bool, error) {
	if filepath.Ext(f.Name()) == redo.TmpEXT {
		return false, nil
	}

	commitTs, err := w.parseLogFileName(f.Name())
	if err != nil {
		return false, err
	}
	return commitTs < checkPointTs, nil
}

func (w *writer) getShouldRemovedFiles(checkPointTs uint64) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(w.cfg.dir)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't read log file directory"))
	}

	logFiles := []os.FileInfo{}
	for _, f := range files {
		ret, err := w.shouldRemoved(checkPointTs, f)
		if err != nil {
			log.Warn("check removed log file fail",
				zap.String("log file", f.Name()),
				zap.Error(err))
			continue
		}

		if ret {
			logFiles = append(logFiles, f)
		}
	}

	return logFiles, nil
}

func (w *writer) flushAll() error {
	err := w.flush()
	if err != nil {
		return err
	}
	if !w.cfg.s3Storage {
		return nil
	}
	return w.writeToS3(context.Background())
}

func (w *writer) Flush() error {
	w.Lock()
	defer w.Unlock()

	return w.flushAll()
}

func (w *writer) flush() error {
	if w.file == nil {
		return nil
	}

	err := w.bw.Flush()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	return cerror.WrapError(cerror.ErrRedoFileOp, w.file.Sync())
}

func (w *writer) writeToS3(ctx context.Context) error {
	name := w.filePath()
	// TODO: use small file in s3, if read takes too long
	fileData, err := os.ReadFile(name)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	return cerror.WrapError(cerror.ErrRedoFileOp, w.storage.WriteFile(ctx, name, fileData))
}
