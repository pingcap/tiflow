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
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type cyclicSuite struct{}

var _ = check.Suite(&cyclicSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *cyclicSuite) TestRelaxSQLMode(c *check.C) {
	defer testleak.AfterTest(c)()
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

func (s *cyclicSuite) TestIsTablePaired(c *check.C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		tables   []model.TableName
		isParied bool
	}{
		{[]model.TableName{}, true},
		{[]model.TableName{{Schema: mark.SchemaName, Table: "repl_mark_1"}},
			true},
		{[]model.TableName{{Schema: "a", Table: "a"}},
			false},
		{[]model.TableName{{Schema: mark.SchemaName, Table: "repl_mark_a_a"},
			{Schema: "a", Table: "a"}},
			true},
		{[]model.TableName{
			{Schema: mark.SchemaName, Table: "repl_mark_a_a"},
			{Schema: mark.SchemaName, Table: "repl_mark_a_b"},
			{Schema: "a", Table: "a"},
			{Schema: "a", Table: "b"}},
			true},
	}

	for _, test := range tests {
		c.Assert(IsTablesPaired(test.tables), check.Equals, test.isParied,
			check.Commentf("%v", test))
	}
}
