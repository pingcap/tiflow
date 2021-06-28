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

package errors

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type helperSuite struct{}

var _ = check.Suite(&helperSuite{})

func (s *helperSuite) TestWrapError(c *check.C) {
	defer testleak.AfterTest(c)()
	var (
		rfcError  = ErrDecodeFailed
		err       = errors.New("test")
		testCases = []struct {
			err      error
			isNil    bool
			expected string
		}{
			{nil, true, ""},
			{err, false, "[CDC:ErrDecodeFailed]test"},
		}
	)
	for _, tc := range testCases {
		we := WrapError(rfcError, tc.err)
		if tc.isNil {
			c.Assert(we, check.IsNil)
		} else {
			c.Assert(we, check.NotNil)
			c.Assert(we.Error(), check.Equals, tc.expected)
		}
	}
}
