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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const (
	defaultDirMode  = 0o755
	defaultFileMode = 0o644

	defaultMetaFileName = "meta"
	//100 megabytes
	defaultMaxLogSize    = 100 * 1024 * 1024
	defaultFlushInterval = 2
)

const (
	// stopped defines the state value of a writer which has been stopped
	stopped uint32 = 0
	// started defines the state value of a writer which is currently started
	started  uint32 = 1
	megabyte        = 1024 * 1024
)

// LogWriterConfig is the configuration used by a writer.
type writerConfig struct {
	dir          string
	changeFeedID string
	startTs      uint64
	CreateTime   time.Time
	// maxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	maxLogSize         int64
	flushIntervalInSec int64
}

// writer is a redo log event writer which writes redo log events to a file.
type writer struct {
	cfg      *writerConfig
	size     int64
	commitTS *atomic.Uint64
	state    *atomic.Uint32
	file     *os.File
	storage  storage.ExternalStorage
	stopChan chan struct{}
	sync.RWMutex
}

func newWriter(ctx context.Context, cfg *writerConfig, host, path string, uri *url.URL) *writer {
	if cfg == nil {
		panic("FileWriterConfig can not be nil")
	}

	if cfg.flushIntervalInSec == 0 {
		cfg.flushIntervalInSec = defaultFlushInterval
	}
	cfg.maxLogSize *= megabyte
	if cfg.maxLogSize == 0 {
		cfg.maxLogSize = defaultMaxLogSize
	}

	w := &writer{
		cfg:      cfg,
		stopChan: make(chan struct{}),
		commitTS: atomic.NewUint64(cfg.startTs),
	}
	s3storage, err := w.initS3storage(ctx, host, path, uri)
	if err != nil {
		panic(err)
	}
	w.storage = s3storage

	w.state.Store(started)
	go w.runFlushToDisk(ctx, cfg.flushIntervalInSec)
	go w.runFlushToS3(ctx, cfg.flushIntervalInSec)
	return w
}

func (w *writer) initS3storage(ctx context.Context, host, path string, uri *url.URL) (storage.ExternalStorage, error) {
	if len(host) == 0 {
		return nil, errors.Errorf("please specify the bucket for s3 in %s", uri)
	}

	prefix := strings.Trim(path, "/")
	s3 := &backup.S3{Bucket: host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(uri, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, err)
	}

	// we should set this to true, since br set it by default in parseBackend
	s3.ForcePathStyle = true
	backend := &backup.StorageBackend{
		Backend: &backup.StorageBackend_S3{S3: s3},
	}
	s3storage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		SkipCheckPath:   true,
		HTTPClient:      nil,
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, err)
	}

	return s3storage, nil
}

func (w *writer) runFlushToDisk(ctx context.Context, flushIntervalInSec int64) {
	ticker := time.NewTicker(time.Duration(flushIntervalInSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("runFlushToDisk canceled", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			err := w.flush()
			log.Error("redo log flush error", zap.Error(err))
		case <-w.stopChan:
			log.Info("redo log writer stopped")
			return
		}
	}
}

func (w *writer) Write(rawData []byte) (int, error) {
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

	n, err := w.file.Write(rawData)
	w.size += int64(n)

	return n, err
}

// Close implements Writer.Close.
func (w *writer) Close() error {
	if !w.isRunning() {
		return nil
	}

	w.Lock()
	defer w.Unlock()

	w.state.Store(stopped)
	w.stopChan <- struct{}{}
	return w.close()
}

func (w *writer) isRunning() bool {
	return w.state.Load() == started
}

func (w *writer) close() error {
	if w.file == nil {
		return nil
	}
	err := w.flush()
	if err != nil {
		return err
	}

	err = w.file.Close()
	w.file = nil
	return err
}

func (w *writer) getLogFileName() string {
	return fmt.Sprintf("%s_%d_%d", w.cfg.changeFeedID, w.cfg.CreateTime.Unix(), w.commitTS)
}

func (w *writer) filePath() string {
	return filepath.Join(w.cfg.dir, w.getLogFileName())
}

func (w *writer) openNew() error {
	err := os.MkdirAll(w.cfg.dir, defaultDirMode)
	if err != nil {
		return errors.Errorf("can't make dir for new redo logfile: %s", err)
	}

	path := w.filePath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return errors.Errorf("can't open new redo logfile: %s", err)
	}
	w.file = f
	w.size = 0
	return nil
}

func (w *writer) openOrNew(writeLen int) error {
	path := w.filePath()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return w.openNew()
	}
	if err != nil {
		return errors.Errorf("error getting log file info: %s", err)
	}

	if info.Size()+int64(writeLen) >= w.cfg.maxLogSize {
		return w.rotate()
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, defaultFileMode)
	if err != nil {
		return w.openNew()
	}
	w.file = file
	w.size = info.Size()
	return nil
}

func (w *writer) rotate() error {
	if err := w.close(); err != nil {
		return err
	}
	return w.openNew()
}

// flush flushes all the buffered data to the disk.
func (w *writer) flush() error {
	if w.file == nil {
		return nil
	}
	return errors.Wrap(w.file.Sync(), "Sync")
}

func (w *writer) writeToS3(ctx context.Context) error {
	name := w.filePath()
	w.Lock()
	// TODO: use small file in s3, if read take too long
	fileData, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	w.Unlock()
	return w.storage.WriteFile(ctx, name, fileData)
}

func (w *writer) runFlushToS3(ctx context.Context, flushIntervalInSec int64) {
	ticker := time.NewTicker(time.Duration(flushIntervalInSec)*time.Second + time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("runFlushToS3 canceled", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			err := w.writeToS3(ctx)
			log.Error("redo log flush to s3 error", zap.Error(err))
		case <-w.stopChan:
			log.Info("redo log writer stopped")
			return
		}
	}
}
