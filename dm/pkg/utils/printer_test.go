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

package utils

import (
	capturer "github.com/kami-zh/go-capturer"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/log"
)

var _ = Suite(&testPrinterSuite{})

type testPrinterSuite struct{}

func (t *testPrinterSuite) SetUpTest(c *C) {
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (t *testPrinterSuite) TestPrinter(c *C) {
	noneInfo :=
		`Release Version: None
Git Commit Hash: None
Git Branch: None
UTC Build Time: None
Go Version: None
`

	c.Assert(GetRawInfo(), Equals, noneInfo)
	out := capturer.CaptureStdout(func() {
		PrintInfo2("test")
	})
	c.Assert(out, Equals, "Welcome to test\n"+noneInfo)

	counter := 0
	callback := func() {
		counter++
	}
	PrintInfo("test", callback)
	c.Assert(counter, Equals, 1) // only check callback called.
}
