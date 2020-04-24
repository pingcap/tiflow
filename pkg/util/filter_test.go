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

package util

import (
	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type filterSuite struct{}

var _ = check.Suite(&filterSuite{})

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
		c.Assert(filter.ShouldIgnoreEvent(tc.ts, tc.schema, tc.table), check.Equals, tc.ignore)
	}
}

func (s *filterSuite) TestShouldDiscardDDL(c *check.C) {
	config := &ReplicaConfig{
		DDLWhitelist: []model.ActionType{model.ActionAddForeignKey},
	}
	filter, err := NewFilter(config)
	c.Assert(err, check.IsNil)
	job := &model.Job{Type: model.ActionDropSchema}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsFalse)
	job = &model.Job{Type: model.ActionAddForeignKey}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsFalse)
	job = &model.Job{Type: model.ActionCreateSequence}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsTrue)
}

func (s *filterSuite) TestShouldDiscardCyclicDDL(c *check.C) {
	// Discard CyclicSchema DDLs only.
	config := &ReplicaConfig{
		Cyclic: &CyclicConfig{ReplicaID: 1, SyncDDL: true},
	}
	filter, err := NewFilter(config)
	c.Assert(err, check.IsNil)
	job := &model.Job{Type: model.ActionCreateTable, SchemaName: CyclicSchemaName}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsTrue)
	job = &model.Job{Type: model.ActionCreateTable, SchemaName: "test"}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsFalse)

	// Discard all DDLs.
	config = &ReplicaConfig{
		Cyclic: &CyclicConfig{ReplicaID: 1, SyncDDL: false},
	}
	filter, err = NewFilter(config)
	c.Assert(err, check.IsNil)
	job = &model.Job{Type: model.ActionCreateTable, SchemaName: CyclicSchemaName}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsTrue)
	job = &model.Job{Type: model.ActionCreateTable, SchemaName: "test"}
	c.Assert(filter.ShouldDiscardDDL(job), check.IsTrue)
}
