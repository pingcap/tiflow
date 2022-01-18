// Copyright 2021 PingCAP, Inc.
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

package checker

import (
	tc "github.com/pingcap/check"
)

func (t *testCheckSuite) TestMysqlVersion(c *tc.C) {
	versionChecker := &MySQLVersionChecker{}

	cases := []struct {
		rawVersion string
		pass       bool
	}{
		{"5.5.0-log", false},
		{"5.6.0-log", true},
		{"5.7.0-log", true},
		{"5.8.0-log", true}, // although it does not exist
		{"8.0.1-log", false},
		{"8.0.20", false},
		{"5.5.50-MariaDB-1~wheezy", false},
		{"10.1.1-MariaDB-1~wheezy", false},
		{"10.1.2-MariaDB-1~wheezy", false},
		{"10.13.1-MariaDB-1~wheezy", false},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateWarning,
		}
		versionChecker.checkVersion(cs.rawVersion, result)
		c.Assert(result.State == StateSuccess, tc.Equals, cs.pass)
	}
}
