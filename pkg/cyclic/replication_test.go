package cyclic

import (
	"github.com/pingcap/check"
)

type cyclicSuit struct{}

var _ = check.Suite(&cyclicSuit{})

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
