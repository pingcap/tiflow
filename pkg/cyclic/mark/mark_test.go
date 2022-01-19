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

package mark

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type markSuite struct{}

var _ = check.Suite(&markSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *markSuite) TestIsMarkTable(c *check.C) {
	defer testleak.AfterTest(c)()
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
