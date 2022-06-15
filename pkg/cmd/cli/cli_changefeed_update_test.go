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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/log"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestApplyChanges(t *testing.T) {
	t.Parallel()

	cmd := NewCmdCli()
	commonChangefeedOptions := newChangefeedCommonOptions()
	o := newUpdateChangefeedOptions(commonChangefeedOptions)
	o.addFlags(cmd)

	// Test normal update.
	oldInfo := &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--sink-uri=mysql://root@downstream-tidb:4000"}))
	newInfo, err := o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	require.Equal(t, "mysql://root@downstream-tidb:4000", newInfo.SinkURI)

	// Test for cli command flags that should be ignored.
	oldInfo = &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--interact"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)

	oldInfo = &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--pd=http://127.0.0.1:2379"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)

	dir := t.TempDir()
	filename := filepath.Join(dir, "log.txt")
	reset, err := initTestLogger(filename)
	defer reset()
	require.Nil(t, err)

	// Test for flag that cannot be updated.
	oldInfo = &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--sort-dir=/home"}))
	newInfo, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	file, err := os.ReadFile(filename)
	require.Nil(t, err)
	require.True(t, strings.Contains(string(file), "this flag cannot be updated and will be ignored"))

	// Test schema registry update
	oldInfo = &v2.ChangeFeedInfo{Config: v2.ToAPIReplicaConfig(config.GetDefaultReplicaConfig())}
	require.Equal(t, "", oldInfo.Config.Sink.SchemaRegistry)
	require.Nil(t, cmd.ParseFlags([]string{"--schema-registry=https://username:password@localhost:8081"}))
	newInfo, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	require.Equal(t, "https://username:password@localhost:8081", newInfo.Config.Sink.SchemaRegistry)
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
