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

package model

import (
	"testing"

	"github.com/pingcap/check"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type cloneSubChangeFeedInfoSuite struct{}

var _ = check.Suite(&cloneSubChangeFeedInfoSuite{})

func (s *cloneSubChangeFeedInfoSuite) TestShouldBeDeepCopy(c *check.C) {
	info := SubChangeFeedInfo{
		CheckPointTs: 12,
		ResolvedTs:   20,
		TableInfos: []*ProcessTableInfo{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		},
		TablePLock: &TableLock{Ts: 11},
	}

	clone := info.Clone()
	assertIsSnapshot := func() {
		c.Assert(clone.CheckPointTs, check.Equals, uint64(12))
		c.Assert(clone.ResolvedTs, check.Equals, uint64(20))
		c.Assert(clone.TableInfos, check.HasLen, 3)
		for i, info := range clone.TableInfos {
			c.Assert(info.ID, check.Equals, uint64(i+1))
		}
		c.Assert(clone.TablePLock.Ts, check.Equals, uint64(11))
		c.Assert(clone.TableCLock, check.IsNil)
	}

	assertIsSnapshot()

	info.CheckPointTs = 1111
	info.TableInfos[2] = &ProcessTableInfo{ID: 1212}
	info.TablePLock.Ts = 100
	info.TableCLock = &TableLock{Ts: 100}

	assertIsSnapshot()
}

type removeTableSuite struct{}

var _ = check.Suite(&removeTableSuite{})

func (s *removeTableSuite) TestShouldReturnRemovedTable(c *check.C) {
	info := SubChangeFeedInfo{
		TableInfos: []*ProcessTableInfo{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		},
	}

	t, found := info.RemoveTable(2)
	c.Assert(found, check.IsTrue)
	c.Assert(t.ID, check.Equals, uint64(2))
}

func (s *removeTableSuite) TestShouldHandleTableNotFoundCorrectly(c *check.C) {
	info := SubChangeFeedInfo{}
	t, found := info.RemoveTable(404)
	c.Assert(found, check.IsFalse)
	c.Assert(t, check.IsNil)
}
