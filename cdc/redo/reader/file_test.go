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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo/common"
	"github.com/pingcap/ticdc/cdc/redo/writer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/net/context"
)

// LeakOptions is used to filter the goroutines.
// TODO: to common
var LeakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, LeakOptions...)
}

func TestReader_newReader(t *testing.T) {
	require.Panics(t, func() { newReader(context.Background(), nil) })
	require.Panics(t, func() { newReader(context.Background(), &readerConfig{dir: ""}) })
}

func TestReader_Read(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-reader")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	cfg := &writer.FileWriterConfig{
		MaxLogSize:   100000,
		Dir:          dir,
		ChangeFeedID: "test-cf",
		CaptureID:    "cp",
		FileType:     common.DefaultRowLogFileType,
		CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := writer.NewWriter(ctx, cfg)
	log := &model.RedoLog{
		Row: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 1123}},
	}
	data, err := log.MarshalMsg(nil)
	require.Nil(t, err)
	w.AdvanceTs(11)
	_, err = w.Write(data)
	w.Close()
	require.Nil(t, err)
	fileName := fmt.Sprintf("%s_%s_%d_%s_%d%s", cfg.CaptureID, cfg.ChangeFeedID, cfg.CreateTime.Unix(), cfg.FileType, 11, common.LogEXT)
	path := filepath.Join(cfg.Dir, fileName)
	info, err := os.Stat(path)
	require.Nil(t, err)
	require.Equal(t, fileName, info.Name())

	r := newReader(ctx, &readerConfig{
		dir:      dir,
		startTs:  1,
		endTs:    12,
		fileType: common.DefaultRowLogFileType,
	})
	require.Equal(t, 1, len(r))
	defer r[0].Close() //nolint:errcheck
	log = &model.RedoLog{}
	err = r[0].Read(log)
	require.Nil(t, err)
	require.EqualValues(t, 1123, log.Row.Row.CommitTs)
}

func TestReader_openSelectedFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-openSelectedFiles")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &writer.FileWriterConfig{
		MaxLogSize: 100000,
		Dir:        dir,
	}
	fileName := fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp", "test-cf", time.Now().Unix(), common.DefaultDDLLogFileType, 11, common.LogEXT+common.TmpEXT)
	w := writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	log := &model.RedoLog{
		Row: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 11}},
	}
	data, err := log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	require.Nil(t, err)
	log = &model.RedoLog{
		Row: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 10}},
	}
	data, err = log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	w.Close()
	require.Nil(t, err)
	path := filepath.Join(cfg.Dir, fileName)
	f, err := os.Open(path)
	require.Nil(t, err)

	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp", "test-cf11", time.Now().Unix(), common.DefaultDDLLogFileType, 10, common.LogEXT)
	path = filepath.Join(dir, fileName)
	_, err = os.Create(path)
	require.Nil(t, err)

	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp", "test-cf111", time.Now().Unix(), common.DefaultDDLLogFileType, 10, common.LogEXT) + common.SortLogEXT
	path = filepath.Join(dir, fileName)
	f1, err := os.Create(path)
	require.Nil(t, err)

	dir1, err := ioutil.TempDir("", "redo-openSelectedFiles1")
	require.Nil(t, err)
	defer os.RemoveAll(dir1) //nolint:errcheck
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp", "test-cf", time.Now().Unix(), common.DefaultDDLLogFileType, 11, common.LogEXT+"test")
	path = filepath.Join(dir1, fileName)
	_, err = os.Create(path)
	require.Nil(t, err)

	type arg struct {
		dir, fixedName string
		startTs, endTs uint64
	}

	tests := []struct {
		name    string
		args    arg
		wantRet []io.ReadCloser
		wantErr bool
	}{
		{
			name: "dir not exist",
			args: arg{
				dir:       dir + "test",
				fixedName: common.DefaultDDLLogFileType,
				startTs:   0,
				endTs:     12,
			},
			wantErr: true,
		},
		{
			name: "happy",
			args: arg{
				dir:       dir,
				fixedName: common.DefaultDDLLogFileType,
				startTs:   0,
				endTs:     12,
			},
			wantRet: []io.ReadCloser{f, f1},
		},
		{
			name: "wrong ts",
			args: arg{
				dir:       dir,
				fixedName: common.DefaultDDLLogFileType,
				startTs:   0,
				endTs:     9,
			},
			wantRet: []io.ReadCloser{f},
		},
		{
			name: "wrong fixedName",
			args: arg{
				dir:       dir,
				fixedName: common.DefaultDDLLogFileType + "test",
				startTs:   0,
				endTs:     9,
			},
		},
		{
			name: "wrong ext",
			args: arg{
				dir:       dir1,
				fixedName: common.DefaultDDLLogFileType,
				startTs:   0,
				endTs:     12,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		ret, err := openSelectedFiles(ctx, tt.args.dir, tt.args.fixedName, tt.args.startTs, tt.args.endTs)
		if !tt.wantErr {
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
					require.Greater(t, rl.Row.Row.CommitTs, preTs, tt.name)
					preTs = rl.Row.Row.CommitTs
				}
			}
		} else {
			require.NotNil(t, err, tt.name)
		}
	}
}
