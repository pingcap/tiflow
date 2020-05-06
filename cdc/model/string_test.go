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
)

type stringSuite struct{}

var _ = check.Suite(&stringSuite{})

func (s *stringSuite) TestExtractKeySuffix(c *check.C) {
	testCases := []struct {
		input  string
		expect string
		hasErr bool
	}{
		{"/tidb/cdc/capture/info/6a6c6dd290bc8732", "6a6c6dd290bc8732", false},
		{"/tidb/cdc/capture/info/6a6c6dd290bc8732/", "", false},
		{"/tidb/cdc", "cdc", false},
		{"/tidb", "tidb", false},
		{"", "", true},
	}
	for _, tc := range testCases {
		key, err := ExtractKeySuffix(tc.input)
		if tc.hasErr {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(key, check.Equals, tc.expect)
		}
	}
}
