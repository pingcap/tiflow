package cyclic

import (
	"testing"

	"github.com/pingcap/check"
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
		tables   []TableName
		isParied bool
	}{
		{[]TableName{}, true},
		{[]TableName{{Schema: SchemaName, Table: "repl_mark_1"}},
			true},
		{[]TableName{{Schema: "a", Table: "a"}},
			false},
		{[]TableName{{Schema: SchemaName, Table: "repl_mark_1"},
			{Schema: "a", Table: "a", ID: 1}},
			true},
		{[]TableName{
			{Schema: SchemaName, Table: "repl_mark_1"},
			{Schema: SchemaName, Table: "repl_mark_2"},
			{Schema: "a", Table: "a", ID: 1},
			{Schema: "a", Table: "b", ID: 2}},
			true},
	}

	for _, test := range tests {
		c.Assert(IsTablesPaired(test.tables), check.Equals, test.isParied,
			check.Commentf("%v", test))
	}
}
