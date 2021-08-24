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

	"github.com/pingcap/ticdc/cdc/redo"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/atomic"
	"go.uber.org/goleak"
	"golang.org/x/net/context"
)

// LeakOptions is used to filter the goroutines.
var LeakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, LeakOptions...)
}

func TestWriterWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-writer")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	w := &writer{
		cfg: &writerConfig{
			maxLogSize:   10,
			dir:          dir,
			changeFeedID: "test-cf",
			captureID:    "cp",
			fileName:     redo.DefaultRowLogFileName,
			createTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		},
		uint64buf: make([]byte, 8),
		state:     *atomic.NewUint32(started),
	}

	w.eventCommitTS.Store(1)
	_, err = w.Write([]byte("tes1t11111"))
	assert.Nil(t, err)
	// create a .tmp file
	fileName := fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileName, 1, redo.LogEXT) + redo.TmpEXT
	path := filepath.Join(w.cfg.dir, fileName)
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
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileName, 1, redo.LogEXT)
	path = filepath.Join(w.cfg.dir, fileName)
	info, err = os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())
	// create a .tmp file with first eventCommitTS as name
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileName, 12, redo.LogEXT) + redo.TmpEXT
	path = filepath.Join(w.cfg.dir, fileName)
	info, err = os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())
	w.Close()
	// safe close, rename to .log with max eventCommitTS as name
	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileName, 22, redo.LogEXT)
	path = filepath.Join(w.cfg.dir, fileName)
	info, err = os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())
}

func TestWriterGC(t *testing.T) {
	assert.Panics(t, func() { newWriter(context.Background(), nil) })

	// dir := filepath.Join(os.TempDir(), "test-GC")
	// dir = "test-GC"
	// err := os.MkdirAll(dir, defaultDirMode)
	dir, err := ioutil.TempDir("", "redo-GC")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	megabyte = 1
	cfg := &writerConfig{
		dir:               dir,
		changeFeedID:      "test-cf",
		captureID:         "cp",
		maxLogSize:        10,
		fileName:          redo.DefaultRowLogFileName,
		createTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		flushIntervalInMs: 5,
	}
	w := newWriter(context.Background(), cfg)

	w.eventCommitTS.Store(1)
	_, err = w.Write([]byte("t1111"))
	assert.Nil(t, err)
	w.eventCommitTS.Store(2)
	_, err = w.Write([]byte("t2222"))
	assert.Nil(t, err)
	w.eventCommitTS.Store(3)
	_, err = w.Write([]byte("t3333"))
	assert.Nil(t, err)

	files, err := ioutil.ReadDir(w.cfg.dir)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(files), "should have 3 log file")

	err = w.GC(3)
	assert.Nil(t, err)

	err = w.Close()
	assert.Nil(t, err)

	files, err = ioutil.ReadDir(w.cfg.dir)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(files), "should have 1 log left after GC")

	ts, err := w.parseLogFileName(files[0].Name())
	assert.Nil(t, err, files[0].Name())
	assert.EqualValues(t, 3, ts)

	time.Sleep(6 * time.Millisecond)
}

func TestAdvanceTs(t *testing.T) {
	w := &writer{}
	w.AdvanceTs(111)
	assert.EqualValues(t, 111, w.eventCommitTS.Load())
}
