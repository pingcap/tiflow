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

package quotes

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type quotesSuite struct{}

var _ = check.Suite(&quotesSuite{})

func (s *quotesSuite) TestQuoteSchema(c *check.C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		schema   string
		table    string
		expected string
	}{
		{"testdb", "t1", "`testdb`.`t1`"},
		{"test-db-1`2", "t`1", "`test-db-1``2`.`t``1`"},
	}
	for _, testCase := range cases {
		name := QuoteSchema(testCase.schema, testCase.table)
		c.Assert(name, check.Equals, testCase.expected)
	}
}

func (s *quotesSuite) TestQuoteName(c *check.C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		name     string
		expected string
	}{
		{"tbl", "`tbl`"},
		{"t`bl", "`t``bl`"},
		{"t``bl", "`t````bl`"},
		{"", "``"},
	}
	for _, testCase := range cases {
		escaped := QuoteName(testCase.name)
		c.Assert(escaped, check.Equals, testCase.expected)
	}
}

func (s *quotesSuite) TestEscapeName(c *check.C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		name     string
		expected string
	}{
		{"tbl", "tbl"},
		{"t`bl", "t``bl"},
		{"t``bl", "t````bl"},
		{"`", "``"},
		{"", ""},
	}
	for _, testCase := range cases {
		escaped := EscapeName(testCase.name)
		c.Assert(escaped, check.Equals, testCase.expected)
	}
}
