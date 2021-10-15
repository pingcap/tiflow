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

package common

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (t *testUtilSuite) TestCheckIsDDL(c *check.C) {
	var (
		cases = []struct {
			sql   string
			isDDL bool
		}{
			{
				sql:   "CREATE DATABASE test_is_ddl",
				isDDL: true,
			},
			{
				sql:   "BEGIN",
				isDDL: false,
			},
			{
				sql:   "INSERT INTO test_is_ddl.test_is_ddl_table VALUES (1)",
				isDDL: false,
			},
			{
				sql:   "INVAID SQL STATEMENT",
				isDDL: false,
			},
		}
		parser2 = parser.New()
	)

	for _, cs := range cases {
		c.Assert(CheckIsDDL(cs.sql, parser2), check.Equals, cs.isDDL)
	}
}
