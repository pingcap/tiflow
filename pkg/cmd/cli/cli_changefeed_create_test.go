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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestStrictDecodeConfig(t *testing.T) {
	t.Parallel()

	cmd := new(cobra.Command)
	o := newChangefeedCommonOptions()
	o.addFlags(cmd)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
	[filter]
	rules = ['*.*', '!test.*']`
	err := os.WriteFile(path, []byte(content), 0o644)
	require.Nil(t, err)

	require.Nil(t, cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}))

	cfg := config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	require.Nil(t, err)

	path = filepath.Join(dir, "config1.toml")
	content = `
	[filter]
	rules = ['*.*', '!test.*','rtest1']`
	err = os.WriteFile(path, []byte(content), 0o644)
	require.Nil(t, err)

	require.Nil(t, cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}))

	cfg = config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	require.NotNil(t, err)
	require.Regexp(t, ".*CDC:ErrFilterRuleInvalid.*", err)
}

func TestTomlFileToApiModel(t *testing.T) {
	cmd := new(cobra.Command)
	o := newChangefeedCommonOptions()
	o.addFlags(cmd)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
	[filter]
	rules = ['*.*', '!test.*']
    ignore-dbs = ["a", "b"]
    do-dbs = ["c", "d"]
    [[filter.ignore-tables]]
    db-name = "demo-db"
    tbl-name = "tbl"
`
	err := os.WriteFile(path, []byte(content), 0o644)
	require.Nil(t, err)

	require.Nil(t, cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}))

	cfg := config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	require.Nil(t, err)
	apiModel := v2.ToAPIReplicaConfig(cfg)
	cfg2 := apiModel.ToInternalReplicaConfig()
	require.Equal(t, cfg, cfg2)
}

func TestInvalidSortEngine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input  string
		expect model.SortEngine
	}{{
		input:  "invalid",
		expect: model.SortUnified,
	}, {
		input:  "memory",
		expect: model.SortInMemory,
	}, {
		input:  "file",
		expect: model.SortInFile,
	}, {
		input:  "unified",
		expect: model.SortUnified,
	}}
	for _, cs := range cases {
		cmd := new(cobra.Command)
		o := newChangefeedCommonOptions()
		o.addFlags(cmd)
		require.Nil(t, cmd.ParseFlags([]string{"--sort-engine=" + cs.input}))
		opt := newCreateChangefeedOptions(o)
		err := opt.validate(cmd)
		require.Nil(t, err)
		require.Equal(t, cs.expect, opt.commonChangefeedOptions.sortEngine)
	}
}

func TestChangefeedCreateCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)

	cmd := newCmdCreateChangefeed(f)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "cf.toml")
	err := os.WriteFile(configPath, []byte("enable-sync-point=true\r\nsync-point-interval='20m'"), 0o644)
	require.Nil(t, err)
	os.Args = []string{
		"create",
		"--config=" + configPath,
		"--no-confirm=false",
		"--target-ts=10",
		"--sink-uri=blackhole://sss?protocol=canal",
		"--schema-registry=a",
		"--sort-engine=memory",
		"--changefeed-id=abc",
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
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(&v2.Tso{
		Timestamp: time.Now().Unix() * 1000,
	}, nil)
	f.changefeeds.EXPECT().VerifyTable(gomock.Any(), gomock.Any()).Return(&v2.Tables{
		IneligibleTables: []v2.TableName{{}},
	}, nil)
	f.changefeeds.EXPECT().Create(gomock.Any(), gomock.Any()).Return(&v2.ChangeFeedInfo{}, nil)
	require.Nil(t, cmd.Execute())

	cmd = newCmdCreateChangefeed(f)
	o := newCreateChangefeedOptions(newChangefeedCommonOptions())
	o.commonChangefeedOptions.sortDir = "/tmp/test"
	require.NoError(t, o.complete(f))
	require.Contains(t, o.validate(cmd).Error(), "creating changefeed with `--sort-dir`")
}
