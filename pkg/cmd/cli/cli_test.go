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

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type cliSuite struct{}

var _ = check.Suite(&cliSuite{})

func (s *cliSuite) TestCliCmdNoArgs(c *check.C) {
	defer testleak.AfterTest(c)
	cmd := NewCmdCli()
	// There is a DBC space before the flag pd.
	flags := []string{"--log-level=info", "　--pd="}
	os.Args = flags
	err := cmd.Execute()
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown command.*u3000--pd.*for.*cli.*")

	// There is an unknown args "aa".
	flags = []string{"--log-level=info", "aa"}
	os.Args = flags
	err = cmd.Execute()
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown command.*aa.*for.*cli.*")
}
