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
	"net/url"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/uuid"
)

var (
	_ RedoEvent = (*model.RowChangedEvent)(nil)
	_ RedoEvent = (*model.DDLEvent)(nil)
)

// RedoEvent is the interface for redo event.
type RedoEvent interface {
	ToRedoLog() *model.RedoLog
}

// RedoLogWriter defines the interfaces used to write redo log, all operations are thread-safe.
type RedoLogWriter interface {
	// WriteEvents writes DDL or DML events to the redo log.
	WriteEvents(ctx context.Context, events ...RedoEvent) error

	// FlushLog flushes all events written by `WriteEvents` into redo storage.
	FlushLog(ctx context.Context) error

	// Close is used to close the writer.
	Close() error
}

// LogWriterConfig is the config for redo log writer.
type LogWriterConfig struct {
	config.ConsistentConfig
	LogType      string
	CaptureID    model.CaptureID
	ChangeFeedID model.ChangeFeedID

	URI                *url.URL
	UseExternalStorage bool
	Dir                string
	MaxLogSizeInBytes  int64
}

func (cfg LogWriterConfig) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%d:%s:%t",
		cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID, cfg.CaptureID,
		cfg.Dir, cfg.MaxLogSize, cfg.URI.String(), cfg.UseExternalStorage)
}

// Option define the writerOptions
type Option func(writer *LogWriterOptions)

// LogWriterOptions is the options for writer
type LogWriterOptions struct {
	GetLogFileName   func() string
	GetUUIDGenerator func() uuid.Generator
}

// WithLogFileName provide the Option for fileName
func WithLogFileName(f func() string) Option {
	return func(o *LogWriterOptions) {
		if f != nil {
			o.GetLogFileName = f
		}
	}
}

// WithUUIDGenerator provides the Option for uuid generator
func WithUUIDGenerator(f func() uuid.Generator) Option {
	return func(o *LogWriterOptions) {
		if f != nil {
			o.GetUUIDGenerator = f
		}
	}
}

// EncodeFrameSize encodes the frame size for etcd wal which uses code
// from etcd wal/encoder.go. Ref: https://github.com/etcd-io/etcd/pull/5250
func EncodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}
