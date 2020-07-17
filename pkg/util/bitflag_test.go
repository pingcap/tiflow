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

	flag.Set(FlagA)
	flag.Set(FlagB, FlagC)
	flag.Set(FlagC | FlagD)

	flag.Clear()

	flag.Set(FlagA, FlagB, FlagC, FlagD)

	flag.Unset(FlagA)

	flag.Unset(FlagB, FlagC)

	if flag.Isset(FlagA) {
		c.Fatal("A should not be set, but set!")
	}

	if flag.Isset(FlagB) {
		c.Fatal("B should not be set, but set!")
	}

	if flag.Isset(FlagC) {
		c.Fatal("C should not be set, but set!")
	}

	if !flag.Isset(FlagD) {
		c.Fatal("D should not set, but not!")
	}
}

func (b *bitFlagSuite) TestSet(c *check.C) {

	var flag Flag

	flag.Set(FlagA)
	flag.Set(FlagB)

	if !flag.Isset(FlagA) {
		c.Fatal("A should be set, but not!")
	}

	if !flag.Isset(FlagB) {
		c.Fatal("B should be set, but not!")
	}

	flag.Set(FlagA, FlagB)

	if !flag.Isset(FlagA, FlagB) {
		c.Fatal("A B should be set, but not!")
	}

	if !flag.One(FlagB) {
		c.Fatal("B should be set, but not!")
	}
}

func (b *bitFlagSuite) TestUnset(c *check.C) {

	var flag Flag

	flag.Set(FlagA, FlagB, FlagC)

	if !flag.Isset(FlagA, FlagB, FlagC) {
		c.Fatal("A should be set, but not!")
	}

	flag.Unset(FlagB)

	if flag.Isset(FlagB) {
		c.Fatal("B should be unset, but set!")
	}

	if !flag.Isset(FlagA, FlagC) {
		c.Fatal("A C should be set, but not!")
	}
}

func (b *bitFlagSuite) TestClear(c *check.C) {

	var flag Flag

	flag.Set(FlagA, FlagB, FlagC)

	if !flag.Isset(FlagA, FlagB, FlagC) {
		c.Fatal("A B C should be set, but not!")
	}

	flag.Clear()

	if flag.One(FlagA, FlagB, FlagC) {
		c.Fatal("A B C should be cleared, but not!")
	}
}
