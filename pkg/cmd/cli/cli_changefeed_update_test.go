// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func TestApplyChanges(t *testing.T) {
	defer testleak.AfterTest(t)()

	cmd := NewCmdCli()
	commonChangefeedOptions := newChangefeedCommonOptions()
	o := newUpdateChangefeedOptions(commonChangefeedOptions)
	o.addFlags(cmd)

	// Test normal update.
	oldInfo := &model.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--sink-uri=mysql://root@downstream-tidb:4000"}))
	newInfo, err := o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	require.Equal(t, newInfo.SinkURI, "mysql://root@downstream-tidb:4000")

	// Test for cli command flags that should be ignored.
	oldInfo = &model.ChangeFeedInfo{SortDir: "."}
	require.Nil(t, cmd.ParseFlags([]string{"--interact"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)

	oldInfo = &model.ChangeFeedInfo{SortDir: "."}
	require.Nil(t, cmd.ParseFlags([]string{"--pd=http://127.0.0.1:2379"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)

	dir := t.TempDir()
	filename := filepath.Join(dir, "log.txt")
	reset, err := initTestLogger(filename)
	defer reset()
	require.Nil(t, err)

	// Test for flag that cannot be updated.
	oldInfo = &model.ChangeFeedInfo{SortDir: "."}
	require.Nil(t, cmd.ParseFlags([]string{"--sort-dir=/home"}))
	newInfo, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	require.Equal(t, newInfo.SortDir, ".")
	file, err := os.ReadFile(filename)
	require.Nil(t, err)
	require.True(t, strings.Contains(string(file), "this flag cannot be updated and will be ignored"))
}

func initTestLogger(filename string) (func(), error) {
	logConfig := &log.Config{
		File: log.FileLogConfig{
			Filename: filename,
		},
	}

	logger, props, err := log.InitLogger(logConfig)
	if err != nil {
		return nil, err
	}
	log.ReplaceGlobals(logger, props)

	return func() {
		conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
		logger, props, _ := log.InitLogger(conf)
		log.ReplaceGlobals(logger, props)
	}, nil
}
