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

package cdc

import (
	"database/sql"

	"github.com/pingcap/check"
)

type CachedInspectorSuite struct {
	getCount  int
	inspector cachedInspector
}

func (s *CachedInspectorSuite) SetUpTest(c *check.C) {
	s.getCount = 0
	s.inspector = cachedInspector{
		cache:       make(map[string]*tableInfo),
		tableGetter: s.get,
	}
}

func (s *CachedInspectorSuite) get(db *sql.DB, schema, table string) (*tableInfo, error) {
	s.getCount++
	t := &tableInfo{
		columns: []string{table + "col1", table + "col2"},
	}
	return t, nil
}

var _ = check.Suite(&CachedInspectorSuite{})

func (s *CachedInspectorSuite) TestGetShouldCacheResult(c *check.C) {
	for i := 0; i < 3; i++ {
		t1, err := s.inspector.Get("test", "t1")
		c.Assert(err, check.IsNil)
		c.Assert(t1.columns, check.DeepEquals, []string{"t1col1", "t1col2"})
		c.Assert(s.getCount, check.Equals, 1)
	}
}

func (s *CachedInspectorSuite) TestRefreshCanInvalidateCache(c *check.C) {
	_, err := s.inspector.Get("test", "t1")
	c.Assert(err, check.IsNil)
	_, err = s.inspector.Get("test", "t2")
	c.Assert(err, check.IsNil)
	c.Assert(s.getCount, check.Equals, 2)

	s.inspector.Refresh("test", "t1")

	_, err = s.inspector.Get("test", "t1")
	c.Assert(err, check.IsNil)
	_, err = s.inspector.Get("test", "t2")
	c.Assert(err, check.IsNil)

	c.Assert(s.getCount, check.Equals, 3)
}
