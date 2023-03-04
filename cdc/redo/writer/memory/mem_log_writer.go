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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	defaultEncodingWorkerNum      = 16
	defaultEncodingInputChanSize  = 128
	defaultEncodingOutputChanSize = 2048
	// Maximum allocated memory is flushWorkerNum*maxLogSize, which is
	// `8*64MB = 512MB` by default.
	defaultFlushWorkerNum = 8
)

var _ writer.RedoLogWriter = (*memoryLogWriter)(nil)

type memoryLogWriter struct {
	cfg           *writer.LogWriterConfig
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup

	eg     *errgroup.Group
	cancel context.CancelFunc
}

// NewLogWriter creates a new memoryLogWriter.
func NewLogWriter(
	ctx context.Context, cfg *writer.LogWriterConfig, opts ...writer.Option,
) (*memoryLogWriter, error) {
	if cfg == nil {
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid,
			errors.New("invalid LogWriterConfig"))
	}
	// "nfs" and "local" scheme are converted to "file" scheme
	if !cfg.UseExternalStorage {
		redo.FixLocalScheme(&cfg.URI)
		cfg.UseExternalStorage = redo.IsExternalStorage(cfg.URI.Scheme)
	}

	extStorage, err := redo.InitExternalStorage(ctx, cfg.URI)
	if err != nil {
		return nil, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	lwCtx, lwCancel := context.WithCancel(ctx)
	lw := &memoryLogWriter{
		cfg:    cfg,
		eg:     eg,
		cancel: lwCancel,
	}

	lw.encodeWorkers = newEncodingWorkerGroup(defaultEncodingWorkerNum)
	eg.Go(func() error {
		return lw.encodeWorkers.Run(lwCtx)
	})
	lw.fileWorkers = newFileWorkerGroup(cfg, defaultFlushWorkerNum, extStorage, opts...)
	eg.Go(func() error {
		return lw.fileWorkers.Run(lwCtx, lw.encodeWorkers.outputCh)
	})
	return lw, nil
}

// WriteEvents implements RedoLogWriter.WriteEvents
func (l *memoryLogWriter) WriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		if err := l.encodeWorkers.AddEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// FlushLog implement FlushLog api
func (l *memoryLogWriter) FlushLog(ctx context.Context) error {
	return l.encodeWorkers.FlushAll(ctx)
}

// Close implements RedoLogWriter.Close
func (l *memoryLogWriter) Close() error {
	if l.cancel != nil {
		l.cancel()
	} else {
		log.Panic("redo writer close without init")
	}
	return l.eg.Wait()
}
