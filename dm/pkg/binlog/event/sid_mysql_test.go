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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testSIDMySQLSuite{})

type testSIDMySQLSuite struct{}

func (t *testSIDMySQLSuite) TestParseSID(c *C) {
	s := "9f61c5f9-1eef-11e9-b6cf-0242ac140003"

	// parse from string
	sid, err := ParseSID(s)
	c.Assert(err, IsNil)

	// convert to string
	s2 := sid.String()
	c.Assert(s2, Equals, s)

	// invalid format
	s = "1eef-11e9-b6cf-0242ac140003"
	_, err = ParseSID(s)
	c.Assert(err, NotNil)

	// invalid characters
	s = "zzz1c5f9-1eef-11e9-b6cf-0242ac140003"
	_, err = ParseSID(s)
	c.Assert(err, NotNil)
}
