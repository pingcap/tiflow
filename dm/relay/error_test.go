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

package relay

import (
	"context"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func TestErrorSuite(t *testing.T) {
	var _ = check.Suite(&testErrorSuite{})
	check.TestingT(t)
}

type testErrorSuite struct{}

func (t *testErrorSuite) TestRetryable(c *check.C) {
	err := errors.New("custom error")
	c.Assert(isRetryableError(err), check.IsFalse)

	cases := []error{
		context.DeadlineExceeded,
		errors.Annotate(context.DeadlineExceeded, "annotated"),
	}
	for _, cs := range cases {
		c.Assert(isRetryableError(cs), check.IsTrue)
	}
}
