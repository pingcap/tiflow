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

package sink

import (
	"github.com/pingcap/check"
)

type testCausalitySuite struct{}

var _ = check.Suite(&testCausalitySuite{})

func (s *testCausalitySuite) TestCausality(c *check.C) {
	rows := [][]string{
		{"a"},
		{"b"},
		{"c"},
	}
	ca := newCausality()
	for i, row := range rows {
		conflict, idx := ca.detectConflict(row)
		c.Assert(conflict, check.IsFalse)
		c.Assert(idx, check.Equals, -1)
		ca.add(row, i)
		// Test for single key index conflict.
		conflict, idx = ca.detectConflict(row)
		c.Assert(conflict, check.IsTrue)
		c.Assert(idx, check.Equals, i)
	}
	c.Assert(len(ca.relations), check.Equals, 3)
	cases := []struct {
		keys     []string
		conflict bool
		idx      int
	}{
		// Test for single key index conflict.
		{[]string{"a", "ab"}, true, 0},
		{[]string{"b", "ba"}, true, 1},
		{[]string{"a", "a"}, true, 0},
		{[]string{"b", "b"}, true, 1},
		{[]string{"c", "c"}, true, 2},
		// Test for multi-key index conflict.
		{[]string{"a", "b"}, true, -1},
		{[]string{"b", "a"}, true, -1},
		{[]string{"b", "c"}, true, -1},
	}
	for _, cas := range cases {
		conflict, idx := ca.detectConflict(cas.keys)
		comment := check.Commentf("keys: %v", cas.keys)
		c.Assert(conflict, check.Equals, cas.conflict, comment)
		c.Assert(idx, check.Equals, cas.idx, comment)
	}
	ca.reset()
	c.Assert(len(ca.relations), check.Equals, 0)
}
