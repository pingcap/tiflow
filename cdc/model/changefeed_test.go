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
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type filterSuite struct{}

var _ = check.Suite(&filterSuite{})

func (s *filterSuite) TestShouldUseDefaultRules(c *check.C) {
	detail := ChangeFeedDetail{Config: &ReplicaConfig{}}
	c.Assert(detail.ShouldIgnoreTable("information_schema", ""), check.IsTrue)
	c.Assert(detail.ShouldIgnoreTable("information_schema", "statistics"), check.IsTrue)
	c.Assert(detail.ShouldIgnoreTable("performance_schema", ""), check.IsTrue)
	c.Assert(detail.ShouldIgnoreTable("metric_schema", "query_duration"), check.IsTrue)
	c.Assert(detail.ShouldIgnoreTable("sns", "user"), check.IsFalse)
	txn := Txn{DDL: &DDL{
		Database: "information_schema",
	}}
	detail.FilterTxn(&txn)
	c.Assert(txn.DDL, check.IsNil)
}

func (s *filterSuite) TestShouldUseCustomRules(c *check.C) {
	detail := ChangeFeedDetail{
		Config: &ReplicaConfig{
			FilterRules: &filter.Rules{
				DoDBs: []string{"sns", "ecom"},
				IgnoreTables: []*filter.Table{
					{Schema: "sns", Name: "log"},
					{Schema: "ecom", Name: "test"},
				},
			},
		},
	}
	assertIgnore := func(db, tbl string, boolCheck check.Checker) {
		c.Assert(detail.ShouldIgnoreTable(db, tbl), boolCheck)
	}
	assertIgnore("other", "", check.IsTrue)
	assertIgnore("other", "what", check.IsTrue)
	assertIgnore("sns", "", check.IsFalse)
	assertIgnore("ecom", "order", check.IsFalse)
	assertIgnore("ecom", "order", check.IsFalse)
	assertIgnore("ecom", "test", check.IsTrue)
	assertIgnore("sns", "log", check.IsTrue)
	assertIgnore("information_schema", "", check.IsTrue)
	txn := Txn{DMLs: []*DML{
		{Database: "other"},
		{Database: "sns"},
		{Database: "ecom"},
		{Database: "ecom", Table: "test"},
	}}
	detail.FilterTxn(&txn)
	c.Assert(txn.DMLs, check.HasLen, 2)

	txn = Txn{DDL: &DDL{
		Database: "sns",
		Table:    "log",
	}}
	detail.FilterTxn(&txn)
	c.Assert(txn.DDL, check.IsNil)
}

func (s *filterSuite) TestShouldIgnoreTxn(c *check.C) {
	detail := ChangeFeedDetail{
		Config: &ReplicaConfig{
			IgnoreTxnCommitTs: []uint64{1, 3},
		},
	}
	testCases := []struct {
		txn    *Txn
		ignore bool
	}{
		{&Txn{DDL: &DDL{Database: "sns"}, Ts: 1}, true},
		{&Txn{DDL: &DDL{Database: "ecom"}, Ts: 2}, false},
		{&Txn{DMLs: []*DML{{Database: "sns", Table: "log"}}, Ts: 3}, true},
	}

	for _, tc := range testCases {
		c.Assert(detail.ShouldIgnoreTxn(tc.txn), check.Equals, tc.ignore)
	}
}
