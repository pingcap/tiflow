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

package filter

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
)

type filterSuite struct{}

var _ = check.Suite(&filterSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *filterSuite) TestShouldUseDefaultRules(c *check.C) {
	defer testleak.AfterTest(c)()
	filter, err := NewFilter(config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)
	c.Assert(filter.ShouldIgnoreTable("information_schema", ""), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("information_schema", "statistics"), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("performance_schema", ""), check.IsTrue)
	c.Assert(filter.ShouldIgnoreTable("metric_schema", "query_duration"), check.IsFalse)
	c.Assert(filter.ShouldIgnoreTable("sns", "user"), check.IsFalse)
	c.Assert(filter.ShouldIgnoreTable("tidb_cdc", "repl_mark_a_a"), check.IsFalse)
}

func (s *filterSuite) TestShouldUseCustomRules(c *check.C) {
	defer testleak.AfterTest(c)()
	filter, err := NewFilter(&config.ReplicaConfig{
		Filter: &config.FilterConfig{
			Rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		},
		Cyclic: &config.CyclicConfig{Enable: true},
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
	assertIgnore("tidb_cdc", "repl_mark_a_a", check.IsFalse)
}

func (s *filterSuite) TestShouldIgnoreTxn(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		cases []struct {
			schema string
			table  string
			ts     uint64
			ignore bool
		}
		ignoreTxnStartTs []uint64
		rules            []string
	}{
		{
			cases: []struct {
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
			},
			ignoreTxnStartTs: []uint64{1, 3},
			rules:            []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		},
		{
			cases: []struct {
				schema string
				table  string
				ts     uint64
				ignore bool
			}{
				{"S", "D1", 1, true},
				{"S", "Da", 1, false},
				{"S", "Db", 1, false},
				{"S", "Daa", 1, false},
			},
			ignoreTxnStartTs: []uint64{},
			rules:            []string{"*.*", "!S.D[!a-d]"},
		},
	}

	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				IgnoreTxnStartTs: ftc.ignoreTxnStartTs,
				Rules:            ftc.rules,
			},
		})
		c.Assert(err, check.IsNil)
		for _, tc := range ftc.cases {
			c.Assert(filter.ShouldIgnoreDMLEvent(tc.ts, tc.schema, tc.table), check.Equals, tc.ignore)
			c.Assert(filter.ShouldIgnoreDDLEvent(tc.ts, model.ActionCreateTable, tc.schema, tc.table), check.Equals, tc.ignore)
		}
	}
}

func (s *filterSuite) TestShouldDiscardDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	config := &config.ReplicaConfig{
		Filter: &config.FilterConfig{
			DDLAllowlist: []model.ActionType{model.ActionAddForeignKey},
		},
	}
	filter, err := NewFilter(config)
	c.Assert(err, check.IsNil)
	c.Assert(filter.ShouldDiscardDDL(model.ActionDropSchema), check.IsFalse)
	c.Assert(filter.ShouldDiscardDDL(model.ActionAddForeignKey), check.IsFalse)
	c.Assert(filter.ShouldDiscardDDL(model.ActionCreateSequence), check.IsTrue)
}

func (s *filterSuite) TestShouldIgnoreDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		cases []struct {
			schema  string
			table   string
			ddlType model.ActionType
			ignore  bool
		}
		rules []string
	}{{
		cases: []struct {
			schema  string
			table   string
			ddlType model.ActionType
			ignore  bool
		}{
			{"sns", "", model.ActionCreateSchema, false},
			{"sns", "", model.ActionDropSchema, false},
			{"sns", "", model.ActionModifySchemaCharsetAndCollate, false},
			{"ecom", "", model.ActionCreateSchema, false},
			{"ecom", "aa", model.ActionCreateTable, false},
			{"ecom", "", model.ActionCreateSchema, false},
			{"test", "", model.ActionCreateSchema, true},
		},
		rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
	}, {
		cases: []struct {
			schema  string
			table   string
			ddlType model.ActionType
			ignore  bool
		}{
			{"sns", "", model.ActionCreateSchema, false},
			{"sns", "", model.ActionDropSchema, false},
			{"sns", "", model.ActionModifySchemaCharsetAndCollate, false},
			{"sns", "aa", model.ActionCreateTable, true},
			{"sns", "C1", model.ActionCreateTable, false},
			{"sns", "", model.ActionCreateTable, true},
		},
		rules: []string{"sns.C1"},
	}}
	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				IgnoreTxnStartTs: []uint64{},
				Rules:            ftc.rules,
			},
		})
		c.Assert(err, check.IsNil)
		for _, tc := range ftc.cases {
			c.Assert(filter.ShouldIgnoreDDLEvent(1, tc.ddlType, tc.schema, tc.table), check.Equals, tc.ignore, check.Commentf("%#v", tc))
		}
	}
}
