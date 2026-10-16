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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ writer.RedoLogWriter = (*memoryLogWriter)(nil)

type memoryLogWriter struct {
	cfg           *writer.LogWriterConfig
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup

	eg     *errgroup.Group
	cancel context.CancelFunc

	errMu    sync.RWMutex
	firstErr error
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
		redo.FixLocalScheme(cfg.URI)
		cfg.UseExternalStorage = redo.IsExternalStorage(cfg.URI.Scheme)
	}

	extStorage, err := redo.InitExternalStorage(ctx, *cfg.URI)
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

	lw.encodeWorkers = newEncodingWorkerGroup(cfg)
	eg.Go(func() error {
		err := lw.encodeWorkers.Run(lwCtx)
		lw.recordError(err)
		return err
	})
	lw.fileWorkers = newFileWorkerGroup(cfg, cfg.FlushWorkerNum, extStorage, opts...)
	eg.Go(func() error {
		err := lw.fileWorkers.Run(lwCtx, lw.encodeWorkers.outputCh)
		lw.recordError(err)
		return err
	})
	return lw, nil
}

func (l *memoryLogWriter) recordError(err error) {
	// Keep the worker's root cause before errgroup cancellation makes sibling
	// workers return context.Canceled.
	if err == nil || errors.Cause(err) == context.Canceled {
		return
	}
	l.errMu.Lock()
	defer l.errMu.Unlock()
	if l.firstErr == nil {
		l.firstErr = err
	}
}

func (l *memoryLogWriter) preferFirstError(err error) error {
	l.errMu.RLock()
	defer l.errMu.RUnlock()
	if l.firstErr != nil {
		return l.firstErr
	}
	return err
}

// WriteEvents implements RedoLogWriter.WriteEvents
func (l *memoryLogWriter) WriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("namespace", l.cfg.ChangeFeedID.Namespace),
				zap.String("changefeed", l.cfg.ChangeFeedID.ID),
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		if err := l.encodeWorkers.AddEvent(ctx, event); err != nil {
			return l.preferFirstError(err)
		}
	}
	return l.preferFirstError(nil)
}

// FlushLog implement FlushLog api
func (l *memoryLogWriter) FlushLog(ctx context.Context) error {
	return l.preferFirstError(l.encodeWorkers.FlushAll(ctx))
}

// Close implements RedoLogWriter.Close
func (l *memoryLogWriter) Close() error {
	if l.cancel != nil {
		l.cancel()
	} else {
		log.Panic("redo writer close without init")
	}
	return l.preferFirstError(l.eg.Wait())
}
