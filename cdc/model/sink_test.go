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

func (s *configSuite) TestSetFlag(c *check.C) {
	var flag ColumnFlagType
	flag.SetIsBinary()
	flag.SetIsGeneratedColumn()
	c.Assert(flag.IsBinary(), check.IsTrue)
	c.Assert(flag.IsHandleKey(), check.IsFalse)
	c.Assert(flag.IsGeneratedColumn(), check.IsTrue)
	flag.UnsetIsBinary()
	c.Assert(flag.IsBinary(), check.IsFalse)
	flag.SetIsMultipleKey()
	flag.SetIsUniqueKey()
	c.Assert(flag.IsMultipleKey() && flag.IsUniqueKey(), check.IsTrue)
	flag.UnsetIsUniqueKey()
	c.Assert(flag.IsUniqueKey(), check.IsFalse)
}

func (s *configSuite) TestFlagValue(c *check.C) {
	c.Assert(BinaryFlag, check.Equals, ColumnFlagType(0b1))
	c.Assert(HandleKeyFlag, check.Equals, ColumnFlagType(0b10))
	c.Assert(GeneratedColumnFlag, check.Equals, ColumnFlagType(0b100))
	c.Assert(PrimaryKeyFlag, check.Equals, ColumnFlagType(0b1000))
	c.Assert(UniqueKeyFlag, check.Equals, ColumnFlagType(0b10000))
	c.Assert(MultipleKeyFlag, check.Equals, ColumnFlagType(0b100000))
	c.Assert(NullableFlag, check.Equals, ColumnFlagType(0b1000000))
}
