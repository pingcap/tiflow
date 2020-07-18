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

package util

import (
	"github.com/pingcap/check"
)

const (
	FlagA Flag = 1 << Flag(iota)
	FlagB
	FlagC
	FlagD
)

type bitFlagSuite struct{}

var _ = check.Suite(&bitFlagSuite{})

func (b *bitFlagSuite) TestExample(c *check.C) {
	var flag Flag

	flag.Add(FlagA)
	flag.Add(FlagB, FlagC)
	flag.Add(FlagC, FlagD)
	flag.Clear()
	flag.Add(FlagA, FlagB, FlagC, FlagD)
	flag.Remove(FlagA)
	flag.Remove(FlagB, FlagC)

	c.Assert(flag.HasAll(FlagA), check.IsFalse)
	c.Assert(flag.HasAll(FlagB), check.IsFalse)
	c.Assert(flag.HasAll(FlagC), check.IsFalse)
	c.Assert(flag.HasAll(FlagD), check.IsTrue)
}

func (b *bitFlagSuite) TestAdd(c *check.C) {
	var flag Flag

	flag.Add(FlagA)
	flag.Add(FlagB)
	c.Check(flag.HasAll(FlagA, FlagB), check.IsTrue)

	flag.Add(FlagA, FlagB)
	c.Check(flag.HasAll(FlagA, FlagB), check.IsTrue)
}

func (b *bitFlagSuite) TestRemove(c *check.C) {
	var flag Flag

	flag.Add(FlagA, FlagB, FlagC)
	c.Check(flag.HasAll(FlagA, FlagB, FlagC), check.IsTrue)

	flag.Remove(FlagB)
	c.Check(flag.HasAll(FlagB), check.IsFalse)
	c.Check(flag.HasAll(FlagA, FlagC), check.IsTrue)
}

func (b *bitFlagSuite) TestClear(c *check.C) {
	var flag Flag

	flag.Add(FlagA, FlagB, FlagC)
	c.Check(flag.HasAll(FlagA, FlagB, FlagC), check.IsTrue)

	flag.Clear()
	c.Check(flag.HasOne(FlagA, FlagB, FlagC), check.IsFalse)
}
