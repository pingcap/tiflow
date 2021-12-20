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

package retry

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type runSuite struct{}

var _ = check.Suite(&runSuite{})

func (s *runSuite) TestShouldRetryAtMostSpecifiedTimes(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Run(500*time.Millisecond, 3, f)
	c.Assert(err, check.ErrorMatches, "test")
	// 👇 i think tries = first call + maxRetries, so not weird 😎

	// It's weird that backoff may retry one more time than maxTries.
	// Because the steps in backoff.Retry is:
	// 1. Call function
	// 2. Compare numTries and maxTries
	// 3. Increment numTries
	c.Assert(callCount, check.Equals, 3+1)
}

func (s *runSuite) TestShouldStopOnSuccess(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		if callCount == 2 {
			return nil
		}
		return errors.New("test")
	}

	err := Run(500*time.Millisecond, 3, f)
	c.Assert(err, check.IsNil)
	c.Assert(callCount, check.Equals, 2)
}

func (s *runSuite) TestShouldBeCtxAware(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		return context.Canceled
	}

	err := Run(500*time.Millisecond, 3, f)
	c.Assert(err, check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 1)

	callCount = 0
	f = func() error {
		callCount++
		return errors.Annotate(context.Canceled, "test")
	}
	err = Run(500*time.Millisecond, 3, f)
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 1)
}

func (s *runSuite) TestInfiniteRetry(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		return context.Canceled
	}

	var reportedElapsed time.Duration
	notify := func(elapsed time.Duration) {
		reportedElapsed = elapsed
	}

	err := RunWithInfiniteRetry(10*time.Millisecond, f, notify)
	c.Assert(err, check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 1)
	c.Assert(reportedElapsed, check.Equals, 0*time.Second)

	callCount = 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	f = func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		callCount++
		return errors.New("test")
	}

	err = RunWithInfiniteRetry(10*time.Millisecond, f, notify)
	c.Assert(err, check.Equals, context.DeadlineExceeded)
	c.Assert(reportedElapsed, check.Greater, time.Second)
	c.Assert(reportedElapsed, check.LessEqual, 3*time.Second)
}

func (s *runSuite) TestDoShouldRetryAtMostSpecifiedTimes(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Do(context.Background(), f, WithMaxTries(3))
	c.Assert(errors.Cause(err), check.ErrorMatches, "test")
	c.Assert(callCount, check.Equals, 3)
}

func (s *runSuite) TestDoShouldStopOnSuccess(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		if callCount == 2 {
			return nil
		}
		return errors.New("test")
	}

	err := Do(context.Background(), f, WithMaxTries(3))
	c.Assert(err, check.IsNil)
	c.Assert(callCount, check.Equals, 2)
}

func (s *runSuite) TestIsRetryable(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		return errors.Annotate(context.Canceled, "test")
	}

	err := Do(context.Background(), f, WithMaxTries(3), WithIsRetryableErr(func(err error) bool {
		switch errors.Cause(err) {
		case context.Canceled:
			return false
		}
		return true
	}))

	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 1)

	callCount = 0
	err = Do(context.Background(), f, WithMaxTries(3))

	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 3)
}

func (s *runSuite) TestDoCancelInfiniteRetry(c *check.C) {
	defer testleak.AfterTest(c)()
	callCount := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()
	f := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		callCount++
		return errors.New("test")
	}

	err := Do(ctx, f, WithInfiniteTries(), WithBackoffBaseDelay(2), WithBackoffMaxDelay(10))
	c.Assert(errors.Cause(err), check.Equals, context.DeadlineExceeded)
	c.Assert(callCount, check.GreaterEqual, 1, check.Commentf("tries: %d", callCount))
	c.Assert(callCount, check.Less, math.MaxInt64)
}

func (s *runSuite) TestDoCancelAtBeginning(c *check.C) {
	defer testleak.AfterTest(c)()
	callCount := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Do(ctx, f, WithInfiniteTries(), WithBackoffBaseDelay(2), WithBackoffMaxDelay(10))
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(callCount, check.Equals, 0, check.Commentf("tries:%d", callCount))
}

func (s *runSuite) TestDoCornerCases(c *check.C) {
	defer testleak.AfterTest(c)()
	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Do(context.Background(), f, WithBackoffBaseDelay(math.MinInt64), WithBackoffMaxDelay(math.MaxInt64), WithMaxTries(2))
	c.Assert(errors.Cause(err), check.ErrorMatches, "test")
	c.Assert(callCount, check.Equals, 2)

	callCount = 0
	err = Do(context.Background(), f, WithBackoffBaseDelay(math.MaxInt64), WithBackoffMaxDelay(math.MinInt64), WithMaxTries(2))
	c.Assert(errors.Cause(err), check.ErrorMatches, "test")
	c.Assert(callCount, check.Equals, 2)

	callCount = 0
	err = Do(context.Background(), f, WithBackoffBaseDelay(math.MinInt64), WithBackoffMaxDelay(math.MinInt64), WithMaxTries(2))
	c.Assert(errors.Cause(err), check.ErrorMatches, "test")
	c.Assert(callCount, check.Equals, 2)

	callCount = 0
	err = Do(context.Background(), f, WithBackoffBaseDelay(math.MaxInt64), WithBackoffMaxDelay(math.MaxInt64), WithMaxTries(2))
	c.Assert(errors.Cause(err), check.ErrorMatches, "test")
	c.Assert(callCount, check.Equals, 2)

	var i int64
	for i = -10; i < 10; i++ {
		callCount = 0
		err = Do(context.Background(), f, WithBackoffBaseDelay(i), WithBackoffMaxDelay(i), WithMaxTries(i))
		c.Assert(errors.Cause(err), check.ErrorMatches, "test")
		c.Assert(err, check.ErrorMatches, ".*CDC:ErrReachMaxTry.*")
		if i > 0 {
			c.Assert(int64(callCount), check.Equals, i)
		} else {
			c.Assert(callCount, check.Equals, defaultMaxTries)
		}
	}
}
