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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	"github.com/pingcap/ticdc/cdc/redo/writer"
	"github.com/stretchr/testify/assert"
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
	assert.Panics(t, func() { newReader(context.Background(), nil) })
}

func TestReader_Read(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-reader")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	cfg := &writer.FileWriterConfig{
		MaxLogSize:   100000,
		Dir:          dir,
		ChangeFeedID: "test-cf",
		CaptureID:    "cp",
		FileName:     redo.DefaultRowLogFileName,
		CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := writer.NewWriter(ctx, cfg)
	log := &model.RedoLog{
		Row: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 1123}},
	}
	data, err := log.MarshalMsg(nil)
	assert.Nil(t, err)
	w.AdvanceTs(11)
	_, err = w.Write(data)
	w.Close()
	assert.Nil(t, err)
	fileName := fmt.Sprintf("%s_%s_%d_%s_%d%s", cfg.CaptureID, cfg.ChangeFeedID, cfg.CreateTime.Unix(), cfg.FileName, 11, redo.LogEXT)
	path := filepath.Join(cfg.Dir, fileName)
	info, err := os.Stat(path)
	assert.Nil(t, err)
	assert.Equal(t, fileName, info.Name())

	r := newReader(ctx, &readerConfig{
		dir:      dir,
		startTs:  1,
		endTs:    12,
		fileType: redo.DefaultRowLogFileName,
	})
	assert.Equal(t, 1, len(r))
	defer r[0].Close()
	log = &model.RedoLog{}
	err = r[0].Read(log)
	assert.Nil(t, err)
	assert.EqualValues(t, 1123, log.Row.Row.CommitTs)
}
