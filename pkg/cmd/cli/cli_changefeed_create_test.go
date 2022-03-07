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

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
)

func TestChangefeedSuite(t *testing.T) { check.TestingT(t) }

type changefeedSuite struct{}

var _ = check.Suite(&changefeedSuite{})

func (s *changefeedSuite) TestStrictDecodeConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newChangefeedCommonOptions()
	o.addFlags(cmd)

	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
	[filter]
	rules = ['*.*', '!test.*']`
	err := os.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	c.Assert(cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}), check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	c.Assert(err, check.IsNil)

	path = filepath.Join(dir, "config1.toml")
	content = `
	[filter]
	rules = ['*.*', '!test.*','rtest1']`
	err = os.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	c.Assert(cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}), check.IsNil)

	cfg = config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*CDC:ErrFilterRuleInvalid.*")
}

func (s *changefeedSuite) TestInvalidSortEngine(c *check.C) {
	defer testleak.AfterTest(c)()

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
		c.Assert(cmd.ParseFlags([]string{"--sort-engine=" + cs.input}), check.IsNil)
		opt := newCreateChangefeedOptions(o)
		err := opt.completeCfg(cmd,
			[]*model.CaptureInfo{{Version: version.MinTiCDCVersion.String()}})
		c.Assert(err, check.IsNil)
		c.Assert(opt.commonChangefeedOptions.sortEngine, check.Equals, cs.expect)
	}
}
