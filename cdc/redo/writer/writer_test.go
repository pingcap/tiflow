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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
			name: "writer err",
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
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
		}
		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}

		_, err := writer.WriteLog(tt.args.ctx, tt.args.tableID, tt.args.rows)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
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
			name: "writer err",
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
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}

		err := writer.SendDDL(tt.args.ctx, tt.args.ddl)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
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
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		controller := gomock.NewController(t)
		mockStorage := mockstorage.NewMockExternalStorage(controller)
		if tt.isRunning && tt.name != "context cancel" {
			mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test-cf_meta.meta", gomock.Any()).Return(nil).Times(1)
		}
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
			S3Storage:         true,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
			storage:   mockStorage,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := writer.FlushLog(tt.args.ctx, tt.args.tableID, tt.args.ts)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.Equal(t, tt.args.ts, writer.meta.ResolvedTsList[tt.args.tableID], tt.name)
		}
	}
}

func TestLogWriter_EmitCheckpointTs(t *testing.T) {
	type arg struct {
		ctx context.Context
		ts  uint64
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
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		controller := gomock.NewController(t)
		mockStorage := mockstorage.NewMockExternalStorage(controller)
		if tt.isRunning && tt.name != "context cancel" {
			mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test-cf_meta.meta", gomock.Any()).Return(nil).Times(1)
		}

		mockWriter := &mockFileWriter{}
		mockWriter.On("IsRunning").Return(tt.isRunning)
		cfg := &LogWriterConfig{
			Dir:               dir,
			ChangeFeedID:      "test-cf",
			CaptureID:         "cp",
			MaxLogSize:        10,
			CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			FlushIntervalInMs: 5,
			S3Storage:         true,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
			storage:   mockStorage,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := writer.EmitCheckpointTs(tt.args.ctx, tt.args.ts)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.Equal(t, tt.args.ts, writer.meta.CheckPointTs, tt.name)
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
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	for _, tt := range tests {
		controller := gomock.NewController(t)
		mockStorage := mockstorage.NewMockExternalStorage(controller)
		if tt.isRunning && tt.name != "context cancel" {
			mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test-cf_meta.meta", gomock.Any()).Return(nil).Times(1)
		}
		mockWriter := &mockFileWriter{}
		mockWriter.On("IsRunning").Return(tt.isRunning)
		cfg := &LogWriterConfig{
			Dir:               dir,
			ChangeFeedID:      "test-cf",
			CaptureID:         "cp",
			MaxLogSize:        10,
			CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			FlushIntervalInMs: 5,
			S3Storage:         true,
		}
		writer := LogWriter{
			rowWriter: mockWriter,
			ddlWriter: mockWriter,
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
			storage:   mockStorage,
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		}
		err := writer.EmitResolvedTs(tt.args.ctx, tt.args.ts)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.Equal(t, tt.args.ts, writer.meta.ResolvedTs, tt.name)
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
	require.Nil(t, err)
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
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
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
			require.NotNil(t, err, tt.name, err.Error())
		} else {
			require.Nil(t, err, tt.name)
			require.Equal(t, len(ret), len(tt.wantTs))
			for k, v := range tt.wantTs {
				require.Equal(t, v, ret[k])
			}
		}
	}
}

func TestNewLogWriter(t *testing.T) {
	require.Panics(t, func() { NewLogWriter(context.Background(), nil) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &LogWriterConfig{
		Dir:               "dirt",
		ChangeFeedID:      "test-cf",
		CaptureID:         "cp",
		MaxLogSize:        10,
		CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		FlushIntervalInMs: 5,
	}
	var ll *LogWriter
	initOnce = sync.Once{}
	require.NotPanics(t, func() { ll = NewLogWriter(ctx, cfg) })
	time.Sleep(time.Duration(defaultGCIntervalInMs+1) * time.Millisecond)
	require.Equal(t, map[int64]uint64{}, ll.meta.ResolvedTsList)

	cfg.Dir += "ttt"
	ll1 := NewLogWriter(ctx, cfg)
	require.Same(t, ll, ll1)

	dir, err := ioutil.TempDir("", "redo-NewLogWriter")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	fileName := fmt.Sprintf("%s_%s_%d_%s%s", "cp", "test-changefeed", time.Now().Unix(), common.DefaultMetaFileType, common.MetaEXT)
	path := filepath.Join(dir, fileName)
	f, err := os.Create(path)
	require.Nil(t, err)

	meta := &common.LogMeta{
		CheckPointTs: 11,
		ResolvedTs:   22,
	}
	data, err := meta.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)

	cfg.Dir = dir
	initOnce = sync.Once{}
	l := NewLogWriter(ctx, cfg)
	require.Equal(t, cfg.Dir, l.cfg.Dir)
	require.Equal(t, meta.CheckPointTs, l.meta.CheckPointTs)
	require.Equal(t, meta.ResolvedTs, l.meta.ResolvedTs)
	require.Equal(t, map[int64]uint64{}, l.meta.ResolvedTsList)
}

func TestWriter_GC(t *testing.T) {
	cfg := &LogWriterConfig{
		Dir:               "dir",
		ChangeFeedID:      "test-cf",
		CaptureID:         "cp",
		MaxLogSize:        10,
		CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		FlushIntervalInMs: 5,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			meta:      &common.LogMeta{ResolvedTsList: map[int64]uint64{}},
			cfg:       cfg,
		}
		go writer.runGC(ctx)
		time.Sleep(time.Duration(defaultGCIntervalInMs+1) * time.Millisecond)

		writer.Close()
		mockWriter.AssertNumberOfCalls(t, "Close", 2)

		if tt.args.isRunning {
			mockWriter.AssertCalled(t, "GC", mock.Anything)
		} else {
			mockWriter.AssertNotCalled(t, "GC", mock.Anything)
		}
	}
}
