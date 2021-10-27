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

package mode

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"

	tcontext "github.com/pingcap/dm/pkg/context"
)

var _ = Suite(&testModeSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testModeSuite struct{}

func (t *testModeSuite) TestMode(c *C) {
	m := NewSafeMode()
	c.Assert(m.Enable(), IsFalse)

	tctx := tcontext.Background()
	// Add 1
	err := m.Add(tctx, 1)
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(tctx, -1)
	c.Assert(m.Enable(), IsFalse)
	c.Assert(err, IsNil)

	// Add n
	err = m.Add(tctx, 101)
	c.Assert(m.Enable(), IsTrue)
	c.Assert(err, IsNil)
	err = m.Add(tctx, -1)
	c.Assert(m.Enable(), IsTrue)
	c.Assert(err, IsNil)
	err = m.Add(tctx, -100)
	c.Assert(m.Enable(), IsFalse)
	c.Assert(err, IsNil)

	// IncrForTable
	table := &filter.Table{
		Schema: "schema",
		Name:   "table",
	}
	err = m.IncrForTable(tctx, table)
	c.Assert(err, IsNil)
	err = m.IncrForTable(tctx, table) // re-Add
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsTrue)
	err = m.DescForTable(tctx, table)
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsFalse)

	// Add n + IncrForTable
	err = m.Add(tctx, 100)
	c.Assert(err, IsNil)
	err = m.IncrForTable(tctx, table)
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(tctx, -100)
	c.Assert(err, IsNil)
	err = m.DescForTable(tctx, table)
	c.Assert(m.Enable(), IsFalse)
	c.Assert(err, IsNil)

	// Add becomes to negative
	err = m.Add(tctx, -1)
	c.Assert(err, NotNil)
}
