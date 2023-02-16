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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
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
	require.Nil(t, cmd.ParseFlags([]string{"--log-level=debug"}))
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
	_, err = o.applyChanges(oldInfo, cmd)
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

func TestChangefeedUpdateCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)
	o := newUpdateChangefeedOptions(newChangefeedCommonOptions())
	o.complete(f)
	cmd := newCmdUpdateChangefeed(f)
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(nil, errors.New("test"))
	os.Args = []string{"update", "--no-confirm=true", "--changefeed-id=abc"}
	o.commonChangefeedOptions.noConfirm = true
	o.changefeedID = "abc"
	require.NotNil(t, o.run(cmd))

	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").
		Return(&v2.ChangeFeedInfo{
			ID: "abc",
			Config: &v2.ReplicaConfig{
				Sink: &v2.SinkConfig{},
			},
		}, nil)
	f.changefeeds.EXPECT().Update(gomock.Any(), gomock.Any(), "abc").
		Return(&v2.ChangeFeedInfo{}, nil)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "cf.toml")
	err := os.WriteFile(configPath, []byte(""), 0o644)
	require.Nil(t, err)
	os.Args = []string{
		"update",
		"--config=" + configPath,
		"--no-confirm=false",
		"--target-ts=10",
		"--sink-uri=abcd",
		"--schema-registry=a",
		"--sort-engine=memory",
		"--changefeed-id=abc",
		"--sort-dir=a",
		"--upstream-pd=pd",
		"--upstream-ca=ca",
		"--upstream-cert=cer",
		"--upstream-key=key",
	}

	path := filepath.Join(dir, "confirm.txt")
	err = os.WriteFile(path, []byte("y"), 0o644)
	require.Nil(t, err)
	file, err := os.Open(path)
	require.Nil(t, err)
	stdin := os.Stdin
	os.Stdin = file
	defer func() {
		os.Stdin = stdin
	}()
	require.Nil(t, cmd.Execute())

	// no diff
	cmd = newCmdUpdateChangefeed(f)
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").
		Return(&v2.ChangeFeedInfo{}, nil)
	os.Args = []string{"update", "--no-confirm=true", "-c", "abc"}
	require.Nil(t, cmd.Execute())

	cmd = newCmdUpdateChangefeed(f)
	f.changefeeds.EXPECT().Get(gomock.Any(), "abcd").
		Return(&v2.ChangeFeedInfo{ID: "abcd"}, errors.New("test"))
	o.commonChangefeedOptions.noConfirm = true
	o.commonChangefeedOptions.sortEngine = "unified"
	o.changefeedID = "abcd"
	require.NotNil(t, o.run(cmd))
}
