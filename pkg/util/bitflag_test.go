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

	if flag.HasAll(FlagA) {
		c.Fatal("A should not be set, but set!")
	}

	if flag.HasAll(FlagB) {
		c.Fatal("B should not be set, but set!")
	}

	if flag.HasAll(FlagC) {
		c.Fatal("C should not be set, but set!")
	}

	if !flag.HasAll(FlagD) {
		c.Fatal("D should not set, but not!")
	}
}

func (b *bitFlagSuite) TestSet(c *check.C) {

	var flag Flag

	flag.Add(FlagA)
	flag.Add(FlagB)

	if !flag.HasAll(FlagA) {
		c.Fatal("A should be set, but not!")
	}

	if !flag.HasAll(FlagB) {
		c.Fatal("B should be set, but not!")
	}

	flag.Add(FlagA, FlagB)

	if !flag.HasAll(FlagA, FlagB) {
		c.Fatal("A B should be set, but not!")
	}
}

func (b *bitFlagSuite) TestUnset(c *check.C) {

	var flag Flag

	flag.Add(FlagA, FlagB, FlagC)

	if !flag.HasAll(FlagA, FlagB, FlagC) {
		c.Fatal("A B C should be set, but not!")
	}

	flag.Remove(FlagB)

	if flag.HasAll(FlagB) {
		c.Fatal("B should be unset, but set!")
	}

	if !flag.HasAll(FlagA, FlagC) {
		c.Fatal("A C should be set, but not!")
	}
}

func (b *bitFlagSuite) TestClear(c *check.C) {

	var flag Flag

	flag.Add(FlagA, FlagB, FlagC)

	if !flag.HasAll(FlagA, FlagB, FlagC) {
		c.Fatal("A B C should be set, but not!")
	}

	flag.Clear()

	if flag.HasOne(FlagA, FlagB, FlagC) {
		c.Fatal("A B C should be cleared, but not!")
	}
}
