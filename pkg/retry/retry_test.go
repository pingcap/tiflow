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

package retry

import (
	"context"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func Test(t *testing.T) { check.TestingT(t) }

type runSuite struct{}

var _ = check.Suite(&runSuite{})

func (s *runSuite) TestShouldRetryAtMostSpecifiedTimes(c *check.C) {
	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Run(f, 3)
	c.Assert(err, check.ErrorMatches, "test")
	// It's weird that backoff may retry one more time than maxTries.
	// Because the steps in backoff.Retry is:
	// 1. Call function
	// 2. Compare numTries and maxTries
	// 3. Increment numTries
	c.Assert(callCount, check.Equals, 3+1)
}

func (s *runSuite) TestShouldStopOnSuccess(c *check.C) {
	var callCount int
	f := func() error {
		callCount++
		if callCount == 2 {
			return nil
		}
		return errors.New("test")
	}

	err := Run(f, 3)
	c.Assert(err, check.IsNil)
	c.Assert(callCount, check.Equals, 2)
}

func (s *runSuite) TestShouldBeCtxAware(c *check.C) {
	var callCount int
	f := func() error {
		callCount++
		return context.Canceled
	}

	err := Run(f, 3)
	c.Assert(err, check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 1)

	callCount = 0
	f = func() error {
		callCount++
		return errors.Annotate(context.Canceled, "test")
	}
	err = Run(f, 3)
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 1)
}
