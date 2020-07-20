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

package model

import "github.com/pingcap/check"

type columnFlagTypeSuite struct{}

var _ = check.Suite(&columnFlagTypeSuite{})

func (s *configSuite) TestBinaryFlag(c *check.C) {
	var flag ColumnFlagType
	c.Assert(flag.IsBinary(), check.IsFalse)
	flag.SetIsBinary()
	c.Assert(flag.IsBinary(), check.IsTrue)
	flag.UnsetIsBinary()
	c.Assert(flag.IsBinary(), check.IsFalse)
}
