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
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLogWriterWriteLog(t *testing.T) {
	t.Parallel()

	type arg struct {
		ctx  context.Context
		rows []writer.RedoEvent
	}
	tableInfo := &model.TableInfo{
		TableName: model.TableName{
			Schema: "test",
			Table:  "t",
		},
	}
	tests := []struct {
		name      string
		args      arg
		wantTs    uint64
		isRunning bool
		writerErr error
		wantErr   error
	}{
		{
			name: "happy",
			args: arg{
				ctx: context.Background(),
				rows: []writer.RedoEvent{
					&model.RowChangedEvent{CommitTs: 1, PhysicalTableID: 111, TableInfo: tableInfo},
				},
			},
			isRunning: true,
			writerErr: nil,
		},
		{
			name: "writer err",
			args: arg{
				ctx: context.Background(),
				rows: []writer.RedoEvent{
					nil,
					&model.RowChangedEvent{CommitTs: 1, PhysicalTableID: 11, TableInfo: tableInfo},
				},
			},
			writerErr: errors.New("err"),
			wantErr:   errors.New("err"),
			isRunning: true,
		},
		{
			name: "len(rows)==0",
			args: arg{
				ctx:  context.Background(),
				rows: []writer.RedoEvent{},
			},
			writerErr: errors.New("err"),
			isRunning: true,
		},
		{
			name: "isStopped",
			args: arg{
				ctx:  context.Background(),
				rows: []writer.RedoEvent{},
			},
			writerErr: errors.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   errors.ErrRedoWriterStopped,
		},
		{
			name: "context cancel",
			args: arg{
				ctx:  context.Background(),
				rows: []writer.RedoEvent{},
			},
			writerErr: nil,
			isRunning: true,
			wantErr:   context.Canceled,
		},
	}

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Write", mock.Anything).Return(1, tt.writerErr)
		mockWriter.On("IsRunning").Return(tt.isRunning)
		mockWriter.On("AdvanceTs", mock.Anything)
		w := logWriter{
			cfg:           &writer.LogWriterConfig{},
			backendWriter: mockWriter,
		}
		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}

		err := w.WriteEvents(tt.args.ctx, tt.args.rows...)
		if tt.wantErr != nil {
			log.Info("log error",
				zap.String("wantErr", tt.wantErr.Error()),
				zap.String("gotErr", err.Error()))
			require.Equal(t, tt.wantErr.Error(), err.Error(), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}

func TestLogWriterWriteDDL(t *testing.T) {
	t.Parallel()

	type arg struct {
		ctx     context.Context
		tableID int64
		ddl     *model.RedoDDLEvent
	}
	tests := []struct {
		name      string
		args      arg
		wantTs    uint64
		isRunning bool
		writerErr error
		wantErr   error
	}{
		{
			name: "happy",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ddl:     &model.RedoDDLEvent{DDL: &model.DDLEvent{CommitTs: 1}},
			},
			isRunning: true,
			writerErr: nil,
		},
		{
			name: "writer err",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ddl:     &model.RedoDDLEvent{DDL: &model.DDLEvent{CommitTs: 1}},
			},
			writerErr: errors.New("err"),
			wantErr:   errors.New("err"),
			isRunning: true,
		},
		{
			name: "ddl nil",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ddl:     nil,
			},
			writerErr: errors.New("err"),
			isRunning: true,
		},
		{
			name: "isStopped",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ddl:     &model.RedoDDLEvent{DDL: &model.DDLEvent{CommitTs: 1}},
			},
			writerErr: errors.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   errors.ErrRedoWriterStopped,
		},
		{
			name: "context cancel",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ddl:     &model.RedoDDLEvent{DDL: &model.DDLEvent{CommitTs: 1}},
			},
			writerErr: nil,
			isRunning: true,
			wantErr:   context.Canceled,
		},
	}

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Write", mock.Anything).Return(1, tt.writerErr)
		mockWriter.On("IsRunning").Return(tt.isRunning)
		mockWriter.On("AdvanceTs", mock.Anything)
		w := logWriter{
			cfg:           &writer.LogWriterConfig{},
			backendWriter: mockWriter,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}

		var e writer.RedoEvent
		if tt.args.ddl != nil {
			e = tt.args.ddl.DDL
		}
		err := w.WriteEvents(tt.args.ctx, e)
		if tt.wantErr != nil {
			log.Info("log error",
				zap.String("wantErr", tt.wantErr.Error()),
				zap.String("gotErr", err.Error()))
			require.Equal(t, tt.wantErr.Error(), err.Error(), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}

func TestLogWriterFlushLog(t *testing.T) {
	t.Parallel()

	type arg struct {
		ctx     context.Context
		tableID int64
		ts      uint64
	}
	tests := []struct {
		name      string
		args      arg
		wantTs    uint64
		isRunning bool
		flushErr  error
		wantErr   error
	}{
		{
			name: "happy",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ts:      1,
			},
			isRunning: true,
			flushErr:  nil,
		},
		{
			name: "flush err",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ts:      1,
			},
			flushErr:  errors.New("err"),
			wantErr:   errors.New("err"),
			isRunning: true,
		},
		{
			name: "isStopped",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ts:      1,
			},
			flushErr:  errors.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   errors.ErrRedoWriterStopped,
		},
		{
			name: "context cancel",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ts:      1,
			},
			flushErr:  nil,
			isRunning: true,
			wantErr:   context.Canceled,
		},
	}

	dir := t.TempDir()

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Flush", mock.Anything).Return(tt.flushErr)
		mockWriter.On("IsRunning").Return(tt.isRunning)
		cfg := &writer.LogWriterConfig{
			Dir:                dir,
			ChangeFeedID:       model.DefaultChangeFeedID("test-cf"),
			CaptureID:          "cp",
			MaxLogSizeInBytes:  10,
			UseExternalStorage: true,
		}
		w := logWriter{
			cfg:           cfg,
			backendWriter: mockWriter,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := w.FlushLog(tt.args.ctx)
		if tt.wantErr != nil {
			log.Info("log error",
				zap.String("wantErr", tt.wantErr.Error()),
				zap.String("gotErr", err.Error()))
			require.Equal(t, tt.wantErr.Error(), err.Error(), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}
