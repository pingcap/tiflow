//  Copyright 2023 PingCAP, Inc.
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

package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type fileCache struct {
	data        []byte
	fileSize    int64
	maxCommitTs model.Ts
	// After memoryWriter become stable, this field would be used to
	// avoid traversing log files.
	minCommitTs model.Ts

	filename string
	flushed  chan struct{}
	writer   *dataWriter
}

type dataWriter struct {
	buf    *bytes.Buffer
	writer io.Writer
	closer io.Closer
}

func (w *dataWriter) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *dataWriter) Close() error {
	if w.closer != nil {
		return w.closer.Close()
	}
	return nil
}

func (f *fileCache) waitFlushed(ctx context.Context) error {
	if f.flushed != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.flushed:
		}
	}
	return nil
}

func (f *fileCache) markFlushed() {
	if f.flushed != nil {
		close(f.flushed)
	}
}

func (f *fileCache) appendData(event *polymorphicRedoEvent) error {
	_, err := f.writer.Write(event.data.Bytes())
	if err != nil {
		return err
	}
	f.fileSize += int64(event.data.Len())
	if event.commitTs > f.maxCommitTs {
		f.maxCommitTs = event.commitTs
	}
	if event.commitTs < f.minCommitTs {
		f.minCommitTs = event.commitTs
	}
	return nil
}

type fileWorkerGroup struct {
	cfg       *writer.LogWriterConfig
	op        *writer.LogWriterOptions
	workerNum int

	extStorage    storage.ExternalStorage
	uuidGenerator uuid.Generator

	pool    sync.Pool
	files   []*fileCache
	flushCh chan *fileCache

	metricWriteBytes       prometheus.Gauge
	metricFlushAllDuration prometheus.Observer
}

func newFileWorkerGroup(
	cfg *writer.LogWriterConfig, workerNum int,
	extStorage storage.ExternalStorage,
	opts ...writer.Option,
) *fileWorkerGroup {
	if workerNum <= 0 {
		workerNum = redo.DefaultFlushWorkerNum
	}

	op := &writer.LogWriterOptions{}
	for _, opt := range opts {
		opt(op)
	}

	return &fileWorkerGroup{
		cfg:           cfg,
		op:            op,
		workerNum:     workerNum,
		extStorage:    extStorage,
		uuidGenerator: uuid.NewGenerator(),
		pool: sync.Pool{
			New: func() interface{} {
				// Use pointer here to prevent static checkers from reporting errors.
				// Ref: https://github.com/dominikh/go-tools/issues/1336.
				buf := make([]byte, 0, cfg.MaxLogSizeInBytes)
				return &buf
			},
		},
		flushCh: make(chan *fileCache),
		metricWriteBytes: common.RedoWriteBytesGauge.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
	}
}

func (f *fileWorkerGroup) Run(
	ctx context.Context, inputCh <-chan *polymorphicRedoEvent,
) (err error) {
	defer func() {
		f.close()
		if err != nil && errors.Cause(err) != context.Canceled {
			log.Warn("redo file workers closed with error",
				zap.String("namespace", f.cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", f.cfg.ChangeFeedID.ID),
				zap.Error(err))
		}
	}()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return f.bgWriteLogs(egCtx, inputCh)
	})
	for i := 0; i < f.workerNum; i++ {
		eg.Go(func() error {
			return f.bgFlushFileCache(egCtx)
		})
	}
	log.Info("redo file workers started",
		zap.String("namespace", f.cfg.ChangeFeedID.Namespace),
		zap.String("changefeed", f.cfg.ChangeFeedID.ID),
		zap.Int("workerNum", f.workerNum))
	return eg.Wait()
}

func (f *fileWorkerGroup) close() {
	common.RedoFlushAllDurationHistogram.
		DeleteLabelValues(f.cfg.ChangeFeedID.Namespace, f.cfg.ChangeFeedID.ID)
	common.RedoWriteBytesGauge.
		DeleteLabelValues(f.cfg.ChangeFeedID.Namespace, f.cfg.ChangeFeedID.ID)
}

func (f *fileWorkerGroup) bgFlushFileCache(egCtx context.Context) error {
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case file := <-f.flushCh:
			start := time.Now()
			if err := file.writer.Close(); err != nil {
				return errors.Trace(err)
			}
			var err error
			if f.cfg.FlushConcurrency <= 1 {
				err = f.extStorage.WriteFile(egCtx, file.filename, file.writer.buf.Bytes())
			} else {
				err = f.multiPartUpload(egCtx, file)
			}
			f.metricFlushAllDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				return errors.Trace(err)
			}
			file.markFlushed()

			bufPtr := &file.data
			file.data = nil
			f.pool.Put(bufPtr)
		}
	}
}

func (f *fileWorkerGroup) multiPartUpload(ctx context.Context, file *fileCache) error {
	multipartWrite, err := f.extStorage.Create(ctx, file.filename, &storage.WriterOption{
		Concurrency: f.cfg.FlushConcurrency,
	})
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = multipartWrite.Write(ctx, file.writer.buf.Bytes()); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(multipartWrite.Close(ctx))
}

func (f *fileWorkerGroup) bgWriteLogs(
	egCtx context.Context, inputCh <-chan *polymorphicRedoEvent,
) (err error) {
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case event := <-inputCh:
			if event == nil {
				log.Error("inputCh of redo file worker is closed unexpectedly")
				return errors.ErrUnexpected.FastGenByArgs("inputCh of redo file worker is closed unexpectedly")
			}

			if event.data != nil {
				err = f.writeToCache(egCtx, event)
				event.reset()
				eventPool.Put(event)
			} else if event.flushCallback != nil {
				err = f.flushAll(egCtx)
				event.flushCallback()
			}

			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// newFileCache write event to a new file cache.
func (f *fileWorkerGroup) newFileCache(event *polymorphicRedoEvent) error {
	bufPtr := f.pool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0]
	var (
		wr     io.Writer
		closer io.Closer
	)
	bufferWriter := bytes.NewBuffer(buf)
	wr = bufferWriter
	if f.cfg.Compression == compression.LZ4 {
		wr = lz4.NewWriter(bufferWriter)
		closer = wr.(io.Closer)
	}
	_, err := wr.Write(event.data.Bytes())
	if err != nil {
		return errors.Trace(err)
	}

	dw := &dataWriter{
		buf:    bufferWriter,
		writer: wr,
		closer: closer,
	}
	file := &fileCache{
		data:        buf,
		fileSize:    int64(len(event.data.Bytes())),
		maxCommitTs: event.commitTs,
		minCommitTs: event.commitTs,
		flushed:     make(chan struct{}),
		writer:      dw,
	}
	f.files = append(f.files, file)
	return nil
}

func (f *fileWorkerGroup) writeToCache(
	egCtx context.Context, event *polymorphicRedoEvent,
) error {
	writeLen := int64(event.data.Len())
	if writeLen > f.cfg.MaxLogSizeInBytes {
		// TODO: maybe we need to deal with the oversized event.
		return errors.ErrFileSizeExceed.GenWithStackByArgs(writeLen, f.cfg.MaxLogSizeInBytes)
	}
	defer f.metricWriteBytes.Add(float64(writeLen))

	if len(f.files) == 0 {
		return f.newFileCache(event)
	}

	file := f.files[len(f.files)-1]
	if file.fileSize+writeLen > f.cfg.MaxLogSizeInBytes {
		file.filename = f.getLogFileName(file.maxCommitTs)
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case f.flushCh <- file:
		}

		return f.newFileCache(event)
	}

	return file.appendData(event)
}

func (f *fileWorkerGroup) flushAll(egCtx context.Context) error {
	if len(f.files) == 0 {
		return nil
	}

	file := f.files[len(f.files)-1]
	file.filename = f.getLogFileName(file.maxCommitTs)
	select {
	case <-egCtx.Done():
		return errors.Trace(egCtx.Err())
	case f.flushCh <- file:
	}

	// wait all files flushed
	for _, file := range f.files {
		err := file.waitFlushed(egCtx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	f.files = f.files[:0]
	return nil
}

func (f *fileWorkerGroup) getLogFileName(maxCommitTS model.Ts) string {
	if f.op != nil && f.op.GetLogFileName != nil {
		return f.op.GetLogFileName()
	}
	uid := f.uuidGenerator.NewString()
	if model.DefaultNamespace == f.cfg.ChangeFeedID.Namespace {
		return fmt.Sprintf(redo.RedoLogFileFormatV1,
			f.cfg.CaptureID, f.cfg.ChangeFeedID.ID, f.cfg.LogType,
			maxCommitTS, uid, redo.LogEXT)
	}
	return fmt.Sprintf(redo.RedoLogFileFormatV2,
		f.cfg.CaptureID, f.cfg.ChangeFeedID.Namespace, f.cfg.ChangeFeedID.ID,
		f.cfg.LogType, maxCommitTS, uid, redo.LogEXT)
}
