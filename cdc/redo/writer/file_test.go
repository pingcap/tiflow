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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/redo/common"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/atomic"
	"go.uber.org/goleak"
	"golang.org/x/net/context"
)

// LeakOptions is used to filter the goroutines.
var LeakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, LeakOptions...)
}

func TestWriterWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-writer")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	w := &Writer{
		cfg: &FileWriterConfig{
			MaxLogSize:   10,
			Dir:          dir,
			ChangeFeedID: "test-cf",
			CaptureID:    "cp",
			FileName:     common.DefaultRowLogFileName,
			CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		},
		uint64buf: make([]byte, 8),
		state:     *atomic.NewUint32(started),
	}

	w.eventCommitTS.Store(1)
	_, err = w.Write([]byte("tes1t11111"))
	assert.Nil(t, err)
	// create a .tmp file
	fileName := fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.CaptureID, w.cfg.ChangeFeedID, w.cfg.CreateTime.Unix(), w.cfg.FileName, 1, common.LogEXT) + common.TmpEXT
	path := filepath.Join(w.cfg.Dir, fileName)
	info, err := os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())

	w.eventCommitTS.Store(12)
	_, err = w.Write([]byte("tt"))
	assert.Nil(t, err)
	w.eventCommitTS.Store(22)
	_, err = w.Write([]byte("t"))
	assert.Nil(t, err)

	// after rotate, rename to .log
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.CaptureID, w.cfg.ChangeFeedID, w.cfg.CreateTime.Unix(), w.cfg.FileName, 1, common.LogEXT)
	path = filepath.Join(w.cfg.Dir, fileName)
	info, err = os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())
	// create a .tmp file with first eventCommitTS as name
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.CaptureID, w.cfg.ChangeFeedID, w.cfg.CreateTime.Unix(), w.cfg.FileName, 12, common.LogEXT) + common.TmpEXT
	path = filepath.Join(w.cfg.Dir, fileName)
	info, err = os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())
	w.Close()
	// safe close, rename to .log with max eventCommitTS as name
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.CaptureID, w.cfg.ChangeFeedID, w.cfg.CreateTime.Unix(), w.cfg.FileName, 22, common.LogEXT)
	path = filepath.Join(w.cfg.Dir, fileName)
	info, err = os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())
}

func TestWriterGC(t *testing.T) {
	assert.Panics(t, func() { NewWriter(context.Background(), nil) })

	dir, err := ioutil.TempDir("", "redo-GC")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	megabyte = 1
	cfg := &FileWriterConfig{
		Dir:               dir,
		ChangeFeedID:      "test-cf",
		CaptureID:         "cp",
		MaxLogSize:        10,
		FileName:          common.DefaultRowLogFileName,
		CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		FlushIntervalInMs: 5,
	}
	w := NewWriter(context.Background(), cfg)

	w.eventCommitTS.Store(1)
	_, err = w.Write([]byte("t1111"))
	assert.Nil(t, err)
	w.eventCommitTS.Store(2)
	_, err = w.Write([]byte("t2222"))
	assert.Nil(t, err)
	w.eventCommitTS.Store(3)
	_, err = w.Write([]byte("t3333"))
	assert.Nil(t, err)

	files, err := ioutil.ReadDir(w.cfg.Dir)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(files), "should have 3 log file")

	err = w.GC(3)
	assert.Nil(t, err)

	err = w.Close()
	assert.Nil(t, err)

	files, err = ioutil.ReadDir(w.cfg.Dir)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(files), "should have 1 log left after GC")

	ts, err := w.parseLogFileName(files[0].Name())
	assert.Nil(t, err, files[0].Name())
	assert.EqualValues(t, 3, ts)

	time.Sleep(6 * time.Millisecond)
}

func TestAdvanceTs(t *testing.T) {
	w := &Writer{}
	w.AdvanceTs(111)
	assert.EqualValues(t, 111, w.eventCommitTS.Load())
}
