// Copyright 2020 PingCAP, Inc.
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

package cmd

import (
	"context"
	"io/ioutil"
	"path/filepath"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/spf13/cobra"
)

type clientChangefeedSuite struct{}

var _ = check.Suite(&clientChangefeedSuite{})

func (s *clientChangefeedSuite) TestVerifyChangefeedParams(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := &cobra.Command{}

	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
enable-old-value = false
`
	err := ioutil.WriteFile(path, []byte(content), 0644)
	c.Assert(err, check.IsNil)

	sinkURI = "blackhole:///?protocol=maxwell"
	info, err := verifyChangefeedParamers(ctx, cmd, false /* isCreate */, nil)
	c.Assert(err, check.IsNil)
	c.Assert(info.Config.EnableOldValue, check.IsTrue)
	c.Assert(info.SortDir, check.Equals, defaultSortDir)

	sinkURI = ""
	_, err = verifyChangefeedParamers(ctx, cmd, true /* isCreate */, nil)
	c.Assert(err, check.NotNil)

	c.Assert(info.Config.EnableOldValue, check.IsTrue)
}
