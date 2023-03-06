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

package reader

import (
	"context"
	"fmt"
<<<<<<< HEAD
	"io"
	"io/ioutil"
=======
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestNewLogReader(t *testing.T) {
	t.Parallel()

	_, err := newLogReader(context.Background(), nil)
	require.NotNil(t, err)

	_, err = newLogReader(context.Background(), &LogReaderConfig{})
	require.Nil(t, err)

	dir, err := ioutil.TempDir("", "redo-NewLogReader")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	s3URI, err := url.Parse("s3://logbucket/test-changefeed?endpoint=http://111/")
	require.Nil(t, err)

	origin := redo.InitExternalStorage
	defer func() {
		redo.InitExternalStorage = origin
	}()
	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)
	// no file to download
	mockStorage.EXPECT().WalkDir(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	redo.InitExternalStorage = func(
		ctx context.Context, uri url.URL,
	) (storage.ExternalStorage, error) {
		return mockStorage, nil
	}

	// after init should rm the dir
	_, err = newLogReader(context.Background(), &LogReaderConfig{
		UseExternalStorage: true,
		Dir:                dir,
		URI:                *s3URI,
	})
	require.Nil(t, err)
	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err))
}

<<<<<<< HEAD
func TestLogReaderResetReader(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-ResetReader")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &writer.FileWriterConfig{
		MaxLogSize: 100000,
		Dir:        dir,
	}
	fileName := fmt.Sprintf(redo.RedoLogFileFormatV2, "cp",
		"default", "test-cf100",
		redo.RedoDDLLogFileType, 100, uuid.NewString(), redo.LogEXT)
	w, err := writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	require.Nil(t, err)
	log := &model.RedoLog{
		RedoRow: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 11}},
	}
	data, err := log.MarshalMsg(nil)
=======
func genLogFile(
	ctx context.Context, t *testing.T,
	dir string, logType string, maxCommitTs uint64,
) {
	cfg := &writer.LogWriterConfig{
		MaxLogSizeInBytes: 100000,
		Dir:               dir,
	}
	fileName := fmt.Sprintf(redo.RedoLogFileFormatV2, "capture", "default",
		"changefeed", logType, maxCommitTs, uuid.NewString(), redo.LogEXT)
	w, err := file.NewFileWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	require.Nil(t, err)
	log := &model.RedoLog{}
	if logType == redo.RedoRowLogFileType {
		log.RedoRow.Row = &model.RowChangedEvent{CommitTs: maxCommitTs}
	} else if logType == redo.RedoDDLLogFileType {
		log.RedoDDL.DDL = &model.DDLEvent{
			CommitTs:  maxCommitTs,
			TableInfo: &model.TableInfo{},
		}
		log.Type = model.RedoLogTypeDDL
	}
	rawData, err := codec.MarshalRedoLog(log, nil)
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	require.Nil(t, err)
	_, err = w.Write(rawData)
	require.Nil(t, err)
	err = w.Close()
	require.Nil(t, err)
}

func TestReadLogs(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD
	fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, "cp",
		"default", "test-cf10",
		redo.RedoRowLogFileType, 10, uuid.NewString(), redo.LogEXT)
	w, err = writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	require.Nil(t, err)
	log = &model.RedoLog{
		RedoRow: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 11}},
	}
	data, err = log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	require.Nil(t, err)
	err = w.Close()
	require.Nil(t, err)
	path = filepath.Join(dir, fileName)
	f1, err := os.Open(path)
	require.Nil(t, err)
=======
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))

	meta := &common.LogMeta{
		CheckpointTs: 11,
		ResolvedTs:   100,
	}
	for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, 12)
		genLogFile(ctx, t, dir, logType, meta.ResolvedTs)
	}
	expectedRows := []uint64{12, meta.ResolvedTs}
	expectedDDLs := []uint64{meta.CheckpointTs, meta.CheckpointTs, 12, meta.ResolvedTs}

	r := &LogReader{
		cfg:   &LogReaderConfig{Dir: dir},
		meta:  meta,
		rowCh: make(chan *model.RowChangedEvent, defaultReaderChanSize),
		ddlCh: make(chan *model.DDLEvent, defaultReaderChanSize),
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return r.Run(egCtx)
	})

	for _, ts := range expectedRows {
		row, err := r.ReadNextRow(egCtx)
		require.NoError(t, err)
		require.Equal(t, ts, row.CommitTs)
	}
	for _, ts := range expectedDDLs {
		ddl, err := r.ReadNextDDL(egCtx)
		require.NoError(t, err)
		require.Equal(t, ts, ddl.CommitTs)
	}

	cancel()
	require.ErrorIs(t, eg.Wait(), nil)
}

func TestLogReaderClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	meta := &common.LogMeta{
		CheckpointTs: 11,
		ResolvedTs:   100,
	}
	for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, 12)
		genLogFile(ctx, t, dir, logType, meta.ResolvedTs)
	}

	r := &LogReader{
		cfg:   &LogReaderConfig{Dir: dir},
		meta:  meta,
		rowCh: make(chan *model.RowChangedEvent, 1),
		ddlCh: make(chan *model.DDLEvent, 1),
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return r.Run(egCtx)
	})

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestLogReaderReadMeta(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-ReadMeta")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	fileName := fmt.Sprintf("%s_%s_%d_%s%s", "cp",
		"test-changefeed",
		time.Now().Unix(), redo.RedoMetaFileType, redo.MetaEXT)
	path := filepath.Join(dir, fileName)
	f, err := os.Create(path)
	require.Nil(t, err)
	meta := &common.LogMeta{
		CheckpointTs: 11,
		ResolvedTs:   22,
	}
	data, err := meta.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)

	fileName = fmt.Sprintf("%s_%s_%d_%s%s", "cp1",
		"test-changefeed",
		time.Now().Unix(), redo.RedoMetaFileType, redo.MetaEXT)
	path = filepath.Join(dir, fileName)
	f, err = os.Create(path)
	require.Nil(t, err)
	meta = &common.LogMeta{
		CheckpointTs: 12,
		ResolvedTs:   21,
	}
	data, err = meta.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)

	dir1, err := ioutil.TempDir("", "redo-NoReadMeta")
	require.Nil(t, err)
	defer os.RemoveAll(dir1)

	tests := []struct {
		name                             string
		dir                              string
		wantCheckpointTs, wantResolvedTs uint64
		wantErr                          string
	}{
		{
			name:             "happy",
			dir:              dir,
			wantCheckpointTs: 12,
			wantResolvedTs:   22,
		},
		{
			name:    "no meta file",
			dir:     dir1,
			wantErr: ".*no redo meta file found in dir*.",
		},
		{
			name:    "wrong dir",
			dir:     "xxx",
			wantErr: ".*can't read log file directory*.",
		},
		{
			name:             "context cancel",
			dir:              dir,
			wantCheckpointTs: 12,
			wantResolvedTs:   22,
			wantErr:          context.Canceled.Error(),
		},
	}
	for _, tt := range tests {
		l := &LogReader{
			cfg: &LogReaderConfig{
				Dir: tt.dir,
			},
		}
		ctx := context.Background()
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			ctx = ctx1
		}
		cts, rts, err := l.ReadMeta(ctx)
		if tt.wantErr != "" {
			require.Regexp(t, tt.wantErr, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.Equal(t, tt.wantCheckpointTs, cts, tt.name)
			require.Equal(t, tt.wantResolvedTs, rts, tt.name)
		}
	}
}
<<<<<<< HEAD

func TestLogReaderReadNextLog(t *testing.T) {
	type arg struct {
		ctx    context.Context
		maxNum uint64
	}
	tests := []struct {
		name       string
		args       arg
		wantErr    error
		readerErr  error
		readerErr1 error
		readerRet  *model.RedoLog
		readerRet1 *model.RedoLog
	}{
		{
			name: "happy",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 15,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
		},
		{
			name: "context cancel",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
			wantErr: context.Canceled,
		},
		{
			name: "happy1",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 2,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
		},
		{
			name: "sameCommitTs",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 2,
						StartTs:  2,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 2,
						StartTs:  1,
						RowID:    2,
					},
				},
			},
		},
		{
			name: "io.EOF err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
			readerErr: io.EOF,
		},
		{
			name: "err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoRow: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
			readerErr:  errors.New("xx"),
			readerErr1: errors.New("xx"),
			wantErr:    errors.New("xx"),
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Read", mock.Anything).Return(tt.readerErr).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.RedoRow = tt.readerRet.RedoRow
			arg.Type = model.RedoLogTypeRow
		}).Times(int(tt.args.maxNum))
		mockReader.On("Read", mock.Anything).Return(io.EOF).Once()

		mockReader1 := &mockFileReader{}
		mockReader1.On("Read", mock.Anything).Return(tt.readerErr1).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.RedoRow = tt.readerRet1.RedoRow
			arg.Type = model.RedoLogTypeRow
		})

		l := &LogReader{
			rowReader: []fileReader{mockReader1, mockReader},
			rowHeap:   logHeap{},
			cfg: &LogReaderConfig{
				startTs: 1,
				endTs:   10,
			},
		}
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx1
		}
		ret, err := l.ReadNextLog(tt.args.ctx, tt.args.maxNum)
		if tt.wantErr != nil {
			require.True(t, errors.ErrorEqual(tt.wantErr, err), tt.name)
			require.Equal(t, 0, len(ret), tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.EqualValues(t, tt.args.maxNum, len(ret), tt.name)
			for i := 0; i < int(tt.args.maxNum); i++ {
				if tt.name == "io.EOF err" {
					require.Equal(t, ret[i].Row.CommitTs,
						tt.readerRet1.RedoRow.Row.CommitTs, tt.name)
					continue
				}
				if tt.name == "happy1" {
					require.Equal(t, ret[i].Row.CommitTs,
						tt.readerRet.RedoRow.Row.CommitTs, tt.name)
					continue
				}
				require.Equal(t, ret[i].Row.CommitTs, tt.readerRet1.RedoRow.Row.CommitTs, tt.name)
				require.Equal(t, ret[i].Row.StartTs, tt.readerRet1.RedoRow.Row.StartTs, tt.name)
			}
		}
	}
}

func TestLogReaderReadNexDDL(t *testing.T) {
	type arg struct {
		ctx    context.Context
		maxNum uint64
	}
	tests := []struct {
		name       string
		args       arg
		wantErr    error
		readerErr  error
		readerErr1 error
		readerRet  *model.RedoLog
		readerRet1 *model.RedoLog
	}{
		{
			name: "happy",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 15,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
		},
		{
			name: "context cancel",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
			wantErr: context.Canceled,
		},
		{
			name: "happy1",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
		},
		{
			name: "io.EOF err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
			readerErr: io.EOF,
		},
		{
			name: "err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				RedoDDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
			readerErr:  errors.New("xx"),
			readerErr1: errors.New("xx"),
			wantErr:    errors.New("xx"),
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Read", mock.Anything).Return(tt.readerErr).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.RedoDDL = tt.readerRet.RedoDDL
			arg.Type = model.RedoLogTypeDDL
		}).Times(int(tt.args.maxNum))
		mockReader.On("Read", mock.Anything).Return(io.EOF).Once()
		mockReader1 := &mockFileReader{}
		mockReader1.On("Read", mock.Anything).Return(tt.readerErr1).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.RedoDDL = tt.readerRet1.RedoDDL
			arg.Type = model.RedoLogTypeDDL
		})

		l := &LogReader{
			ddlReader: []fileReader{mockReader1, mockReader},
			ddlHeap:   logHeap{},
			cfg: &LogReaderConfig{
				startTs: 1,
				endTs:   10,
			},
		}
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx1
		}
		ret, err := l.ReadNextDDL(tt.args.ctx, tt.args.maxNum)
		if tt.wantErr != nil {
			require.True(t, errors.ErrorEqual(tt.wantErr, err), tt.name)
			require.Equal(t, 0, len(ret), tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.EqualValues(t, tt.args.maxNum, len(ret), tt.name)
			for i := 0; i < int(tt.args.maxNum); i++ {
				if tt.name == "io.EOF err" {
					require.Equal(t, ret[i].DDL.CommitTs, tt.readerRet1.RedoDDL.DDL.CommitTs, tt.name)
					continue
				}
				if tt.name == "happy1" {
					require.Equal(t, ret[i].DDL.CommitTs, tt.readerRet1.RedoDDL.DDL.CommitTs, tt.name)
					continue
				}
				require.Equal(t, ret[i].DDL.CommitTs, tt.readerRet1.RedoDDL.DDL.CommitTs, tt.name)
			}
		}
	}
}

func TestLogReaderClose(t *testing.T) {
	tests := []struct {
		name    string
		wantErr error
		err     error
	}{
		{
			name: "happy",
		},
		{
			name:    "err",
			err:     errors.New("xx"),
			wantErr: multierr.Append(errors.New("xx"), errors.New("xx")),
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Close").Return(tt.err)
		l := &LogReader{
			rowReader: []fileReader{mockReader},
			ddlReader: []fileReader{mockReader},
		}
		err := l.Close()
		mockReader.AssertNumberOfCalls(t, "Close", 2)
		if tt.wantErr != nil {
			require.True(t, errors.ErrorEqual(tt.wantErr, err), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}
=======
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
