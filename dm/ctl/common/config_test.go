// Copyright 2022 PingCAP, Inc.
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

	. "github.com/pingcap/check"
)

func TestConfig(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (t *testConfigSuite) TestInteractiveQuotes(c *C) {
	cases := []struct {
		input    string
		expected []string
	}{
		{`123`, []string{`123`}},
		{`"123"`, []string{`123`}},
		{`'123'`, []string{`123`}},
		{`123 456`, []string{`123`, `456`}},
		{`'123 456'`, []string{`123 456`}},
		{`"123 456"`, []string{`123 456`}},
		{`"123 456" 789`, []string{`123 456`, `789`}},
		{`0 '123"456 789'`, []string{`0`, `123"456 789`}},
		{`0'123"456 789'`, []string{`0123"456 789`}},
		{`"123""456" 7 "89"`, []string{`123456`, `7`, `89`}},
		// return original string when failed to split
		{`123"456`, []string{`123"456`}},
	}

	for _, ca := range cases {
		got := SplitArgsRespectQuote(ca.input)
		c.Assert(got, DeepEquals, ca.expected)
	}
}
