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

package filter

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type filterSuite struct{}

var _ = check.Suite(&filterSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *filterSuite) TestShouldUseDefaultRules(c *check.C) {
	filter, err := NewFilter(&ReplicaConfig{})
	c.Assert(err, check.IsNil)
	c.Assert(filter.ShouldIgnoreTable("information_schema", ""), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("information_schema", "statistics"), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("performance_schema", ""), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("metric_schema", "query_duration"), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("sns", "user"), check.IsFalse)
}

func (s *filterSuite) TestShouldUseCustomRules(c *check.C) {
	filter, err := NewFilter(&ReplicaConfig{
		FilterRules: &filter.Rules{
			DoDBs: []string{"sns", "ecom"},
			IgnoreTables: []*filter.Table{
				{Schema: "sns", Name: "log"},
				{Schema: "ecom", Name: "test"},
			},
		},
	})
	c.Assert(err, check.IsNil)
	assertIgnore := func(db, tbl string, boolCheck check.Checker) {
		c.Assert(filter.ShouldIgnoreTable(db, tbl), boolCheck)
	}
	assertIgnore("other", "", check.IsTrue)
	assertIgnore("other", "what", check.IsTrue)
	assertIgnore("sns", "", check.IsFalse)
	assertIgnore("ecom", "order", check.IsFalse)
	assertIgnore("ecom", "order", check.IsFalse)
	assertIgnore("ecom", "test", check.IsTrue)
	assertIgnore("sns", "log", check.IsTrue)
	assertIgnore("information_schema", "", check.IsTrue)
}

func (s *filterSuite) TestShouldIgnoreTxn(c *check.C) {
	filter, err := NewFilter(&ReplicaConfig{
		IgnoreTxnCommitTs: []uint64{1, 3},
		FilterRules: &filter.Rules{
			DoDBs: []string{"sns", "ecom"},
			IgnoreTables: []*filter.Table{
				{Schema: "sns", Name: "log"},
				{Schema: "ecom", Name: "test"},
			}}})
	c.Assert(err, check.IsNil)
	testCases := []struct {
		schema string
		table  string
		ts     uint64
		ignore bool
	}{
		{"sns", "ttta", 1, true},
		{"ecom", "aabb", 2, false},
		{"sns", "log", 3, true},
		{"sns", "log", 4, true},
		{"ecom", "test", 5, true},
		{"test", "test", 6, true},
		{"ecom", "log", 6, false},
	}

	for _, tc := range testCases {
		c.Assert(filter.ShouldIgnoreDMLEvent(tc.ts, tc.schema, tc.table), check.Equals, tc.ignore)
		c.Assert(filter.ShouldIgnoreDDLEvent(tc.ts, tc.schema, tc.table), check.Equals, tc.ignore)
	}
}

func (s *filterSuite) TestShouldDiscardDDL(c *check.C) {
	config := &ReplicaConfig{
		DDLWhitelist: []model.ActionType{model.ActionAddForeignKey},
	}
	filter, err := NewFilter(config)
	c.Assert(err, check.IsNil)
	c.Assert(filter.ShouldDiscardDDL(model.ActionDropSchema), check.IsFalse)
	c.Assert(filter.ShouldDiscardDDL(model.ActionAddForeignKey), check.IsFalse)
	c.Assert(filter.ShouldDiscardDDL(model.ActionCreateSequence), check.IsTrue)
}
