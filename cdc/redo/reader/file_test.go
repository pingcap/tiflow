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
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestReaderNewReader(t *testing.T) {
	_, err := newReader(context.Background(), nil)
	require.NotNil(t, err)

	dir := t.TempDir()
	_, err = newReader(context.Background(), &readerConfig{dir: dir})
	require.Nil(t, err)
}

func TestReaderRead(t *testing.T) {
	dir := t.TempDir()

	cfg := &writer.FileWriterConfig{
		MaxLogSize:   100000,
		Dir:          dir,
		ChangeFeedID: model.DefaultChangeFeedID("test-cf"),
		CaptureID:    "cp",
		FileType:     common.DefaultRowLogFileType,
		CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uuidGen := uuid.NewConstGenerator("const-uuid")
	w, err := writer.NewWriter(ctx, cfg,
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
	fileName := fmt.Sprintf(common.RedoLogFileFormatV1, cfg.CaptureID,
		cfg.ChangeFeedID.ID,
		cfg.FileType, 11, uuidGen.NewString(), common.LogEXT)
	path := filepath.Join(cfg.Dir, fileName)
	info, err := os.Stat(path)
	require.Nil(t, err)
	require.Equal(t, fileName, info.Name())

	r, err := newReader(ctx, &readerConfig{
		dir:      dir,
		startTs:  1,
		endTs:    12,
		fileType: common.DefaultRowLogFileType,
	})
	require.Nil(t, err)
	require.Equal(t, 1, len(r))
	defer r[0].Close() //nolint:errcheck
	log = &model.RedoLog{}
	err = r[0].Read(log)
	require.Nil(t, err)
	require.EqualValues(t, 1123, log.RedoRow.Row.CommitTs)
	time.Sleep(1001 * time.Millisecond)
}

func TestReaderOpenSelectedFiles(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &writer.FileWriterConfig{
		MaxLogSize: 100000,
		Dir:        dir,
	}
	uuidGen := uuid.NewGenerator()
	fileName := fmt.Sprintf(common.RedoLogFileFormatV2, "cp",
		"default", "test-cf", common.DefaultDDLLogFileType, 11,
		uuidGen.NewString(), common.LogEXT+common.TmpEXT)
	w, err := writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string {
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

	// no data, wil not open
	fileName = fmt.Sprintf(common.RedoLogFileFormatV2, "cp",
		"default", "test-cf11", common.DefaultDDLLogFileType, 10,
		uuidGen.NewString(), common.LogEXT)
	path = filepath.Join(dir, fileName)
	_, err = os.Create(path)
	require.Nil(t, err)

	// SortLogEXT, wil open
	fileName = fmt.Sprintf(common.RedoLogFileFormatV2, "cp", "default",
		"test-cf111", common.DefaultDDLLogFileType, 10, uuidGen.NewString(),
		common.LogEXT) + common.SortLogEXT
	path = filepath.Join(dir, fileName)
	f1, err := os.Create(path)
	require.Nil(t, err)

	dir1 := t.TempDir()
	fileName = fmt.Sprintf(common.RedoLogFileFormatV2, "cp", "default", "test-cf",
		common.DefaultDDLLogFileType, 11, uuidGen.NewString(), common.LogEXT+"test")
	path = filepath.Join(dir1, fileName)
	_, err = os.Create(path)
	require.Nil(t, err)

	type arg struct {
		dir, fixedName string
		startTs        uint64
	}

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
				fixedName: common.DefaultDDLLogFileType,
				startTs:   0,
			},
			wantErr: ".*CDC:ErrRedoFileOp*.",
		},
		{
			name: "happy",
			args: arg{
				dir:       dir,
				fixedName: common.DefaultDDLLogFileType,
				startTs:   0,
			},
			wantRet: []io.ReadCloser{f, f1},
		},
		{
			name: "wrong ts",
			args: arg{
				dir:       dir,
				fixedName: common.DefaultDDLLogFileType,
				startTs:   12,
			},
			wantRet: []io.ReadCloser{f},
		},
		{
			name: "wrong fixedName",
			args: arg{
				dir:       dir,
				fixedName: common.DefaultDDLLogFileType + "test",
				startTs:   0,
			},
		},
		{
			name: "wrong ext",
			args: arg{
				dir:       dir1,
				fixedName: common.DefaultDDLLogFileType,
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
				if filepath.Ext(name) != common.SortLogEXT {
					name += common.SortLogEXT
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
}
