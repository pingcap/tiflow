// Copyright 2019 PingCAP, Inc.
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

package helper

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/pingcap/check"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testValueSuite{})

type testValueSuite struct{}

type fIsNil func()

func fIsNil1() {}

func (t *testValueSuite) TestIsNil(c *check.C) {
	// nil value
	i := 123
	c.Assert(IsNil(nil), check.IsTrue)
	c.Assert(IsNil(i), check.IsFalse)

	// chan
	c.Assert(IsNil((chan int)(nil)), check.IsTrue)
	c.Assert(IsNil(make(chan int)), check.IsFalse)

	// func
	c.Assert(IsNil((fIsNil)(nil)), check.IsTrue)
	c.Assert(IsNil(fIsNil1), check.IsFalse)

	// interface (error is an interface)
	c.Assert(IsNil((error)(nil)), check.IsTrue)
	c.Assert(IsNil(errors.New("")), check.IsFalse)

	// map
	c.Assert(IsNil((map[int]int)(nil)), check.IsTrue)
	c.Assert(IsNil(make(map[int]int)), check.IsFalse)

	// pointer
	var piNil *int
	piNotNil := &i
	c.Assert(IsNil(piNil), check.IsTrue)
	c.Assert(IsNil(piNotNil), check.IsFalse)

	// unsafe pointer
	var upiNil unsafe.Pointer
	upiNotNil := unsafe.Pointer(piNotNil)
	c.Assert(IsNil(upiNil), check.IsTrue)
	c.Assert(IsNil(upiNotNil), check.IsFalse)

	// slice
	c.Assert(IsNil(([]int)(nil)), check.IsTrue)
	c.Assert(IsNil(make([]int, 0)), check.IsFalse)
}
