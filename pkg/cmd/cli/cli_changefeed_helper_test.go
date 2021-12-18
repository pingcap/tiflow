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

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/spf13/cobra"
)

type changefeedHelperSuite struct{}

var _ = check.Suite(&changefeedHelperSuite{})

func (s *changefeedHelperSuite) TestConfirmLargeDataGap(c *check.C) {
	defer testleak.AfterTest(c)()
	currentTs := int64(423482306736160769) // 2021-03-11 17:59:57.547
	startTs := uint64(423450030227042420)  // 2021-03-10 07:47:52.435

	cmd := &cobra.Command{}

	// check start ts more than 1 day before current ts, and type N when confirming
	dir := c.MkDir()
	path := filepath.Join(dir, "confirm.txt")
	err := os.WriteFile(path, []byte("n"), 0o644)
	c.Assert(err, check.IsNil)
	f, err := os.Open(path)
	c.Assert(err, check.IsNil)
	stdin := os.Stdin
	os.Stdin = f
	defer func() {
		os.Stdin = stdin
	}()

	err = confirmLargeDataGap(cmd, currentTs, startTs)
	c.Assert(err, check.ErrorMatches, "abort changefeed create or resume")

	// check start ts more than 1 day before current ts, and type Y when confirming
	err = os.WriteFile(path, []byte("Y"), 0o644)
	c.Assert(err, check.IsNil)
	f, err = os.Open(path)
	c.Assert(err, check.IsNil)
	os.Stdin = f
	err = confirmLargeDataGap(cmd, currentTs, startTs)
	c.Assert(err, check.IsNil)
}

func (s *changefeedHelperSuite) TestConfirmIgnoreIneligibleTables(c *check.C) {
	defer testleak.AfterTest(c)()

	cmd := &cobra.Command{}

	// check start ts more than 1 day before current ts, and type N when confirming
	dir := c.MkDir()
	path := filepath.Join(dir, "confirm.txt")
	err := os.WriteFile(path, []byte("n"), 0o644)
	c.Assert(err, check.IsNil)
	f, err := os.Open(path)
	c.Assert(err, check.IsNil)
	stdin := os.Stdin
	os.Stdin = f
	defer func() {
		os.Stdin = stdin
	}()

	err = confirmIgnoreIneligibleTables(cmd)
	c.Assert(err, check.ErrorMatches, "abort changefeed create or resume")

	// check start ts more than 1 day before current ts, and type Y when confirming
	err = os.WriteFile(path, []byte("Y"), 0o644)
	c.Assert(err, check.IsNil)
	f, err = os.Open(path)
	c.Assert(err, check.IsNil)
	os.Stdin = f
	err = confirmIgnoreIneligibleTables(cmd)
	c.Assert(err, check.IsNil)
}
