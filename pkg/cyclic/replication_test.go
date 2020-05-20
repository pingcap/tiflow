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

package cyclic

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type cyclicSuit struct{}

var _ = check.Suite(&cyclicSuit{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *cyclicSuit) TestRelaxSQLMode(c *check.C) {
	tests := []struct {
		oldMode string
		newMode string
	}{
		{"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
		{"ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,STRICT_TRANS_TABLES", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
		{"STRICT_TRANS_TABLES", ""},
		{"ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
	}

	for _, test := range tests {
		getNew := RelaxSQLMode(test.oldMode)
		c.Assert(getNew, check.Equals, test.newMode)
	}
}

func (s *cyclicSuit) TestIsTablePaired(c *check.C) {
	tests := []struct {
		tables   []model.TableName
		isParied bool
	}{
		{[]model.TableName{}, true},
		{[]model.TableName{{Schema: SchemaName, Table: "repl_mark_1"}},
			true},
		{[]model.TableName{{Schema: "a", Table: "a"}},
			false},
		{[]model.TableName{{Schema: SchemaName, Table: "repl_mark_a_a"},
			{Schema: "a", Table: "a"}},
			true},
		{[]model.TableName{
			{Schema: SchemaName, Table: "repl_mark_a_a"},
			{Schema: SchemaName, Table: "repl_mark_a_b"},
			{Schema: "a", Table: "a"},
			{Schema: "a", Table: "b"}},
			true},
	}

	for _, test := range tests {
		c.Assert(IsTablesPaired(test.tables), check.Equals, test.isParied,
			check.Commentf("%v", test))
	}
}

func (s *cyclicSuit) TestIsMarkTable(c *check.C) {
	tests := []struct {
		schema, table string
		isMarkTable   bool
	}{
		{"", "", false},
		{"a", "a", false},
		{"a", "", false},
		{"", "a", false},
		{SchemaName, "", true},
		{"", tableName, true},
		{"`" + SchemaName + "`", "", true},
		{"`" + SchemaName + "`", "repl_mark_1", true},
		{SchemaName, tableName, true},
		{SchemaName, "`repl_mark_1`", true},
	}

	for _, test := range tests {
		c.Assert(IsMarkTable(test.schema, test.table), check.Equals, test.isMarkTable,
			check.Commentf("%v", test))
	}
}
