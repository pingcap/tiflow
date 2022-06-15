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
