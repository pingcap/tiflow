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

package file

import (
	"context"
	"path/filepath"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

var _ writer.RedoLogWriter = &logWriter{}

// logWriter implement the RedoLogWriter interface
type logWriter struct {
	cfg           *writer.LogWriterConfig
	backendWriter fileWriter
}

// NewLogWriter create a new logWriter.
func NewLogWriter(
	ctx context.Context, cfg *writer.LogWriterConfig, opts ...writer.Option,
) (lw *logWriter, err error) {
	if cfg == nil {
		err := errors.New("LogWriterConfig can not be nil")
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid, err)
	}

	if cfg.UseExternalStorage {
		// When an external storage is used, we use redoDir as a temporary dir to store redo logs
		// before we flush them to S3.
		changeFeedID := cfg.ChangeFeedID
		dataDir := config.GetGlobalServerConfig().DataDir
		cfg.Dir = filepath.Join(dataDir, config.DefaultRedoDir,
			changeFeedID.Namespace, changeFeedID.ID)
	} else {
		// When local storage or NFS is used, we use redoDir as the final storage path.
		cfg.Dir = cfg.URI.Path
	}

	lw = &logWriter{cfg: cfg}
	if lw.backendWriter, err = NewFileWriter(ctx, cfg, opts...); err != nil {
		return nil, err
	}
	return
}

func (l *logWriter) WriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("capture", l.cfg.CaptureID))
			continue
		}

		rl := event.ToRedoLog()
		data, err := rl.MarshalMsg(nil)
		if err != nil {
			return errors.WrapError(errors.ErrMarshalFailed, err)
		}

		l.backendWriter.AdvanceTs(rl.GetCommitTs())
		_, err = l.backendWriter.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

// FlushLog implement FlushLog api
func (l *logWriter) FlushLog(ctx context.Context) (err error) {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}

	return l.backendWriter.Flush()
}

// Close implements RedoLogWriter.Close.
func (l *logWriter) Close() (err error) {
	return l.backendWriter.Close()
}

func (l *logWriter) isStopped() bool {
	return !l.backendWriter.IsRunning()
}
