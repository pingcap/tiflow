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
<<<<<<< HEAD
	"bufio"
=======
	"context"
>>>>>>> e05cef8fe0 (redo(ticdc): simplify reader initialization (#8407))
	"fmt"
	"io"
	"net/url"
	"testing"

<<<<<<< HEAD
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/cdc/redo/writer/file"
=======
	"github.com/pingcap/log"
>>>>>>> e05cef8fe0 (redo(ticdc): simplify reader initialization (#8407))
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestReaderNewReader(t *testing.T) {
	_, err := newReaders(context.Background(), nil)
	require.NotNil(t, err)

	dir := t.TempDir()
<<<<<<< HEAD
	_, err = newReader(context.Background(), &readerConfig{dir: dir})
	require.Nil(t, err)
}

func TestReaderRead(t *testing.T) {
	dir := t.TempDir()

	cfg := &writer.LogWriterConfig{
		MaxLogSizeInBytes: 100000,
		Dir:               dir,
		ChangeFeedID:      model.DefaultChangeFeedID("test-cf"),
		CaptureID:         "cp",
		LogType:           redo.RedoRowLogFileType,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uuidGen := uuid.NewConstGenerator("const-uuid")
	w, err := file.NewFileWriter(ctx, cfg,
		writer.WithUUIDGenerator(func() uuid.Generator { return uuidGen }),
	)
	require.Nil(t, err)
	log := &model.RedoLog{
		RedoRow: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 1123}},
	}
	data, err := log.MarshalMsg(nil)
	require.Nil(t, err)
	w.AdvanceTs(11)
	_, err = w.Write(data)
	require.Nil(t, err)
	err = w.Close()
	require.Nil(t, err)
	require.True(t, !w.IsRunning())
	fileName := fmt.Sprintf(redo.RedoLogFileFormatV1, cfg.CaptureID,
		cfg.ChangeFeedID.ID,
		cfg.LogType, 11, uuidGen.NewString(), redo.LogEXT)
	path := filepath.Join(cfg.Dir, fileName)
	info, err := os.Stat(path)
	require.Nil(t, err)
	require.Equal(t, fileName, info.Name())

	r, err := newReader(ctx, &readerConfig{
		dir:      dir,
		startTs:  1,
		endTs:    12,
		fileType: redo.RedoRowLogFileType,
	})
	require.Nil(t, err)
	require.Equal(t, 1, len(r))
	defer r[0].Close() //nolint:errcheck
	log = &model.RedoLog{}
	err = r[0].Read(log)
	require.Nil(t, err)
	require.EqualValues(t, 1123, log.RedoRow.Row.CommitTs)
	time.Sleep(1001 * time.Millisecond)
=======
	require.Panics(t, func() {
		_, err = newReaders(context.Background(), &readerConfig{dir: dir})
	})
>>>>>>> e05cef8fe0 (redo(ticdc): simplify reader initialization (#8407))
}

func TestFileReaderRead(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uri, err := url.Parse(fmt.Sprintf("file://%s", dir))
	require.NoError(t, err)
	cfg := &readerConfig{
		dir:                t.TempDir(),
		startTs:            10,
		endTs:              12,
		fileType:           redo.RedoRowLogFileType,
		uri:                *uri,
		useExternalStorage: true,
	}
<<<<<<< HEAD
	uuidGen := uuid.NewGenerator()
	fileName := fmt.Sprintf(redo.RedoLogFileFormatV2, "cp",
		"default", "test-cf", redo.RedoDDLLogFileType, 11,
		uuidGen.NewString(), redo.LogEXT+redo.TmpEXT)
	w, err := file.NewFileWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	require.Nil(t, err)
	log := &model.RedoLog{
		RedoRow: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 11}},
	}
	data, err := log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	require.Nil(t, err)
	log = &model.RedoLog{
		RedoRow: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 10}},
	}
	data, err = log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	require.Nil(t, err)
	err = w.Close()
	require.Nil(t, err)
	path := filepath.Join(cfg.Dir, fileName)
	f, err := os.Open(path)
	require.Nil(t, err)
=======
	// log file with maxCommitTs<=startTs, fileter when download file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 1, cfg.startTs)
	// normal log file, include [10, 11, 12] and [11, 12, ... 20]
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.startTs, cfg.endTs+2)
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.endTs-1, 20)
	// log file with minCommitTs>endTs, filtered when sort file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 2000, 2023)
>>>>>>> e05cef8fe0 (redo(ticdc): simplify reader initialization (#8407))

	log.Info("start to read redo log files")
	readers, err := newReaders(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, len(readers))
	defer readers[0].Close() //nolint:errcheck

	for _, r := range readers {
		log, err := r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 11, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 12, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.Nil(t, log)
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, r.Close())
	}
<<<<<<< HEAD

	tests := []struct {
		name    string
		args    arg
		wantRet []io.ReadCloser
		wantErr string
	}{
		{
			name: "dir not exist",
			args: arg{
				dir:       dir + "test",
				fixedName: redo.RedoDDLLogFileType,
				startTs:   0,
			},
			wantErr: ".*CDC:ErrRedoFileOp*.",
		},
		{
			name: "happy",
			args: arg{
				dir:       dir,
				fixedName: redo.RedoDDLLogFileType,
				startTs:   0,
			},
			wantRet: []io.ReadCloser{f, f1},
		},
		{
			name: "wrong ts",
			args: arg{
				dir:       dir,
				fixedName: redo.RedoDDLLogFileType,
				startTs:   12,
			},
			wantRet: []io.ReadCloser{f},
		},
		{
			name: "wrong fixedName",
			args: arg{
				dir:       dir,
				fixedName: redo.RedoDDLLogFileType + "test",
				startTs:   0,
			},
		},
		{
			name: "wrong ext",
			args: arg{
				dir:       dir1,
				fixedName: redo.RedoDDLLogFileType,
				startTs:   0,
			},
		},
	}

	for _, tt := range tests {
		ret, err := openSelectedFiles(ctx, tt.args.dir, tt.args.fixedName, tt.args.startTs, 100)
		if tt.wantErr == "" {
			require.Nil(t, err, tt.name)
			require.Equal(t, len(tt.wantRet), len(ret), tt.name)
			for _, closer := range tt.wantRet {
				name := closer.(*os.File).Name()
				if filepath.Ext(name) != redo.SortLogEXT {
					name += redo.SortLogEXT
				}
				contains := false
				for _, r := range ret {
					if r.(*os.File).Name() == name {
						contains = true
						break
					}
				}
				require.Equal(t, true, contains, tt.name)
			}
			var preTs uint64 = 0
			for _, r := range ret {
				r := &reader{
					br:       bufio.NewReader(r),
					fileName: r.(*os.File).Name(),
					closer:   r,
				}
				for {
					rl := &model.RedoLog{}
					err := r.Read(rl)
					if err == io.EOF {
						break
					}
					require.Greater(t, rl.RedoRow.Row.CommitTs, preTs, tt.name)
					preTs = rl.RedoRow.Row.CommitTs
				}
			}
		} else {
			require.Regexp(t, tt.wantErr, err.Error(), tt.name)
		}
	}
	time.Sleep(1001 * time.Millisecond)
=======
>>>>>>> e05cef8fe0 (redo(ticdc): simplify reader initialization (#8407))
}
