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

package terror

import (
	"database/sql/driver"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (t *testTErrorSuite) TestDBAdapter(c *check.C) {
	defaultErr := ErrDBDriverError
	testCases := []struct {
		err    error
		expect *Error
	}{
		{nil, nil},
		{ErrDBBadConn, ErrDBBadConn},
		{driver.ErrBadConn, ErrDBBadConn},
		{errors.Annotate(driver.ErrBadConn, "annotate error"), ErrDBBadConn},
		{mysql.ErrInvalidConn, ErrDBInvalidConn},
		{mysql.ErrUnknownPlugin, defaultErr},
	}

	for _, tc := range testCases {
		err := DBErrorAdapt(tc.err, defaultErr)
		if tc.expect == nil {
			c.Assert(err, check.IsNil)
		} else {
			c.Assert(tc.expect.Equal(err), check.IsTrue)
			if err != tc.err {
				exp := tc.expect
				obj, ok := err.(*Error)
				c.Assert(ok, check.IsTrue)
				c.Assert(obj.getMsg(), check.Equals, tc.expect.message)
				c.Assert(obj.Error(), check.Equals, fmt.Sprintf(errBaseFormat+", Message: %s, RawCause: %s, Workaround: %s", exp.code, exp.class, exp.scope, exp.level, exp.message, tc.err.Error(), exp.workaround))
			}
		}
	}
}
