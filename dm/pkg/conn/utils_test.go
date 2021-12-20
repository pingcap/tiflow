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

package conn

import (
	"context"

	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/dm/config"
)

var _ = Suite(testUtilSuite{})

type testUtilSuite struct{}

func (s testUtilSuite) TestFetchTZSetting(c *C) {
	m := InitMockDB(c)

	m.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(m.NewRows([]string{""}).AddRow("01:00:00"))
	tz, err := FetchTimeZoneSetting(context.Background(), &config.DBConfig{})
	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "+01:00")
}
