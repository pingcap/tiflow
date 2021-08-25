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
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestLogWriter_WriteLog(t *testing.T) {
	type arg struct {
		ctx     context.Context
		tableID int64
		rows    []*model.RedoRowChangedEvent
	}
	tests := []struct {
		name      string
		args      arg
		wantTs    uint64
		isRunning bool
		writerErr error
		wantErr   bool
	}{
		{
			name: "happy",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				rows: []*model.RedoRowChangedEvent{
					{
						Row: &model.RowChangedEvent{
							Table: &model.TableName{TableID: 111}, CommitTs: 1,
						},
					},
				},
			},
			isRunning: true,
			writerErr: nil,
		},
		{
			name: "Writer err",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				rows: []*model.RedoRowChangedEvent{
					{Row: nil},
					{
						Row: &model.RowChangedEvent{
							Table: &model.TableName{TableID: 11}, CommitTs: 11,
						},
					},
				},
			},
			writerErr: errors.New("err"),
			wantErr:   true,
			isRunning: true,
		},
		{
			name: "len(rows)==0",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				rows:    []*model.RedoRowChangedEvent{},
			},
			writerErr: errors.New("err"),
			isRunning: true,
		},
		{
			name: "isStopped",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				rows:    []*model.RedoRowChangedEvent{},
			},
			writerErr: cerror.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   true,
		},
		{
			name: "context cancel",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				rows:    []*model.RedoRowChangedEvent{},
			},
			writerErr: nil,
			isRunning: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Write", mock.Anything).Return(1, tt.writerErr)
		mockWriter.On("IsRunning").Return(tt.isRunning)
		mockWriter.On("AdvanceTs", mock.Anything)
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
		}
		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}

		_, err := writer.WriteLog(tt.args.ctx, tt.args.tableID, tt.args.rows)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name)
		} else {
			assert.Nil(t, err, tt.name)
		}
	}
}

func TestLogWriter_SendDDL(t *testing.T) {
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
		wantErr   bool
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
			name: "Writer err",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ddl:     &model.RedoDDLEvent{DDL: &model.DDLEvent{CommitTs: 1}},
			},
			writerErr: errors.New("err"),
			wantErr:   true,
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
			writerErr: cerror.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   true,
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
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Write", mock.Anything).Return(1, tt.writerErr)
		mockWriter.On("IsRunning").Return(tt.isRunning)
		mockWriter.On("AdvanceTs", mock.Anything)
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}

		err := writer.SendDDL(tt.args.ctx, tt.args.ddl)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name)
		} else {
			assert.Nil(t, err, tt.name)
		}
	}
}

func TestLogWriter_FlushLog(t *testing.T) {
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
		wantErr   bool
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
			wantErr:   true,
			isRunning: true,
		},
		{
			name: "isStopped",
			args: arg{
				ctx:     context.Background(),
				tableID: 1,
				ts:      1,
			},
			flushErr:  cerror.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   true,
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
			wantErr:   true,
		},
	}

	dir, err := ioutil.TempDir("", "redo-FlushLog")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Flush", mock.Anything).Return(tt.flushErr)
		mockWriter.On("IsRunning").Return(tt.isRunning)
		cfg := &LogWriterConfig{
			Dir:               dir,
			ChangeFeedID:      "test-cf",
			CaptureID:         "cp",
			MaxLogSize:        10,
			CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			FlushIntervalInMs: 5,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := writer.FlushLog(tt.args.ctx, tt.args.tableID, tt.args.ts)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name)
		} else {
			assert.Nil(t, err, tt.name)
			assert.Equal(t, tt.args.ts, writer.meta.ResolvedTsList[tt.args.tableID], tt.name)
		}
	}
}

func TestLogWriter_EmitCheckpointTs(t *testing.T) {
	type arg struct {
		ctx context.Context

		ts uint64
	}
	tests := []struct {
		name      string
		args      arg
		wantTs    uint64
		isRunning bool
		flushErr  error
		wantErr   bool
	}{
		{
			name: "happy",
			args: arg{
				ctx: context.Background(),
				ts:  1,
			},
			isRunning: true,
			flushErr:  nil,
		},
		{
			name: "isStopped",
			args: arg{
				ctx: context.Background(),
				ts:  1,
			},
			flushErr:  cerror.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   true,
		},
		{
			name: "context cancel",
			args: arg{
				ctx: context.Background(),
				ts:  1,
			},
			flushErr:  nil,
			isRunning: true,
			wantErr:   true,
		},
	}

	dir, err := ioutil.TempDir("", "redo-EmitCheckpointTs")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("IsRunning").Return(tt.isRunning)
		cfg := &LogWriterConfig{
			Dir:               dir,
			ChangeFeedID:      "test-cf",
			CaptureID:         "cp",
			MaxLogSize:        10,
			CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			FlushIntervalInMs: 5,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := writer.EmitCheckpointTs(tt.args.ctx, tt.args.ts)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name)
		} else {
			assert.Nil(t, err, tt.name)
			assert.Equal(t, tt.args.ts, writer.meta.CheckPointTs, tt.name)
		}
	}
}

func TestLogWriter_EmitResolvedTs(t *testing.T) {
	type arg struct {
		ctx context.Context

		ts uint64
	}
	tests := []struct {
		name      string
		args      arg
		wantTs    uint64
		isRunning bool
		flushErr  error
		wantErr   bool
	}{
		{
			name: "happy",
			args: arg{
				ctx: context.Background(),
				ts:  1,
			},
			isRunning: true,
			flushErr:  nil,
		},
		{
			name: "isStopped",
			args: arg{
				ctx: context.Background(),
				ts:  1,
			},
			flushErr:  cerror.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   true,
		},
		{
			name: "context cancel",
			args: arg{
				ctx: context.Background(),
				ts:  1,
			},
			flushErr:  nil,
			isRunning: true,
			wantErr:   true,
		},
	}

	dir, err := ioutil.TempDir("", "redo-ResolvedTs")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("IsRunning").Return(tt.isRunning)
		cfg := &LogWriterConfig{
			Dir:               dir,
			ChangeFeedID:      "test-cf",
			CaptureID:         "cp",
			MaxLogSize:        10,
			CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			FlushIntervalInMs: 5,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := writer.EmitResolvedTs(tt.args.ctx, tt.args.ts)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name)
		} else {
			assert.Nil(t, err, tt.name)
			assert.Equal(t, tt.args.ts, writer.meta.ResolvedTs, tt.name)
		}
	}
}

func TestLogWriter_GetCurrentResolvedTs(t *testing.T) {
	type arg struct {
		ctx      context.Context
		ts       map[int64]uint64
		tableIDs []int64
	}
	tests := []struct {
		name    string
		args    arg
		wantTs  map[int64]uint64
		wantErr bool
	}{
		{
			name: "happy",
			args: arg{
				ctx:      context.Background(),
				ts:       map[int64]uint64{1: 1, 2: 2},
				tableIDs: []int64{1, 2, 3},
			},
			wantTs: map[int64]uint64{1: 1, 2: 2},
		},
		{
			name: "len(tableIDs)==0",
			args: arg{
				ctx: context.Background(),
			},
		},
		{
			name: "context cancel",
			args: arg{
				ctx: context.Background(),
			},
			wantErr: true,
		},
	}

	dir, err := ioutil.TempDir("", "redo-GetCurrentResolvedTs")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("Flush", mock.Anything).Return(nil)
		mockWriter.On("IsRunning").Return(true)
		cfg := &LogWriterConfig{
			Dir:               dir,
			ChangeFeedID:      "test-cf",
			CaptureID:         "cp",
			MaxLogSize:        10,
			CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			FlushIntervalInMs: 5,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		for k, v := range tt.args.ts {
			_ = writer.FlushLog(tt.args.ctx, k, v)
		}
		ret, err := writer.GetCurrentResolvedTs(tt.args.ctx, tt.args.tableIDs)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name, err.Error())
		} else {
			assert.Nil(t, err, tt.name)
			assert.Equal(t, len(ret), len(tt.wantTs))
			for k, v := range tt.wantTs {
				assert.Equal(t, v, ret[k])
			}
		}
	}
}

func TestNewLogWriter(t *testing.T) {
	assert.Panics(t, func() { NewLogWriter(context.Background(), nil) })
	cfg := &LogWriterConfig{
		Dir:               "dir",
		ChangeFeedID:      "test-cf",
		CaptureID:         "cp",
		MaxLogSize:        10,
		CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		FlushIntervalInMs: 5,
	}
	originValue := defaultGCIntervalInMs
	defaultGCIntervalInMs = 1
	defer func() {
		defaultGCIntervalInMs = originValue
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NotPanics(t, func() { NewLogWriter(ctx, cfg) })

	type args struct {
		isRunning bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "running",
			args: args{
				isRunning: true,
			},
		},
		{
			name: "stopped",
			args: args{
				isRunning: false,
			},
		},
	}
	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("IsRunning").Return(tt.args.isRunning)
		mockWriter.On("Close").Return(nil)
		if tt.args.isRunning {
			mockWriter.On("GC", mock.Anything).Return(nil)
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &redo.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
		}
		go writer.runGC(ctx)
		time.Sleep(2 * time.Millisecond)

		writer.Close()
		mockWriter.AssertNumberOfCalls(t, "Close", 2)

		if tt.args.isRunning {
			mockWriter.AssertCalled(t, "GC", mock.Anything)
		} else {
			mockWriter.AssertNotCalled(t, "GC", mock.Anything)
		}
	}
}
