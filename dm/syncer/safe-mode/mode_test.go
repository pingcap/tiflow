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

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/util/filter"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
)

var _ = check.Suite(&testModeSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type testModeSuite struct{}

func (t *testModeSuite) TestMode(c *check.C) {
	m := NewSafeMode()
	c.Assert(m.Enable(), check.IsFalse)

	tctx := tcontext.Background()
	// Add 1
	err := m.Add(tctx, 1)
	c.Assert(err, check.IsNil)
	c.Assert(m.Enable(), check.IsTrue)
	err = m.Add(tctx, -1)
	c.Assert(m.Enable(), check.IsFalse)
	c.Assert(err, check.IsNil)

	// Add n
	err = m.Add(tctx, 101)
	c.Assert(m.Enable(), check.IsTrue)
	c.Assert(err, check.IsNil)
	err = m.Add(tctx, -1)
	c.Assert(m.Enable(), check.IsTrue)
	c.Assert(err, check.IsNil)
	err = m.Add(tctx, -100)
	c.Assert(m.Enable(), check.IsFalse)
	c.Assert(err, check.IsNil)

	// IncrForTable
	table := &filter.Table{
		Schema: "schema",
		Name:   "table",
	}
	err = m.IncrForTable(tctx, table)
	c.Assert(err, check.IsNil)
	err = m.IncrForTable(tctx, table) // re-Add
	c.Assert(err, check.IsNil)
	c.Assert(m.Enable(), check.IsTrue)
	err = m.DescForTable(tctx, table)
	c.Assert(err, check.IsNil)
	c.Assert(m.Enable(), check.IsFalse)

	// Add n + IncrForTable
	err = m.Add(tctx, 100)
	c.Assert(err, check.IsNil)
	err = m.IncrForTable(tctx, table)
	c.Assert(err, check.IsNil)
	c.Assert(m.Enable(), check.IsTrue)
	err = m.Add(tctx, -100)
	c.Assert(err, check.IsNil)
	err = m.DescForTable(tctx, table)
	c.Assert(m.Enable(), check.IsFalse)
	c.Assert(err, check.IsNil)

	// Add becomes to negative
	err = m.Add(tctx, -1)
	c.Assert(err, check.NotNil)
}
