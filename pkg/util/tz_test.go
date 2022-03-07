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

package util

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type tzSuite struct{}

var _ = check.Suite(&tzSuite{})

func (s *tzSuite) TestGetTimezoneFromZonefile(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		hasErr   bool
		zonefile string
		name     string
	}{
		{true, "", ""},
		{false, "UTC", "UTC"},
		{false, "/usr/share/zoneinfo/UTC", "UTC"},
		{false, "/usr/share/zoneinfo/Etc/UTC", "Etc/UTC"},
		{false, "/usr/share/zoneinfo/Asia/Shanghai", "Asia/Shanghai"},
	}
	for _, tc := range testCases {
		loc, err := getTimezoneFromZonefile(tc.zonefile)
		if tc.hasErr {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(loc.String(), check.Equals, tc.name)
		}
	}
}
