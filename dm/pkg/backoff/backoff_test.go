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

package backoff

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testBackoffSuite{})

type testBackoffSuite struct{}

func (t *testBackoffSuite) TestNewBackoff(c *check.C) {
	var (
		backoffFactor float64 = 2
		backoffMin            = 1 * time.Second
		backoffMax            = 5 * time.Minute
		backoffJitter         = true
		bf            *Backoff
		err           error
	)
	testCases := []struct {
		factor float64
		jitter bool
		min    time.Duration
		max    time.Duration
		hasErr bool
	}{
		{backoffFactor, backoffJitter, backoffMin, backoffMax, false},
		{0, backoffJitter, backoffMin, backoffMax, true},
		{-1, backoffJitter, backoffMin, backoffMax, true},
		{backoffFactor, backoffJitter, -1, backoffMax, true},
		{backoffFactor, backoffJitter, backoffMin, -1, true},
		{backoffFactor, backoffJitter, backoffMin, backoffMin - 1, true},
	}
	for _, tc := range testCases {
		bf, err = NewBackoff(tc.factor, tc.jitter, tc.min, tc.max)
		if tc.hasErr {
			c.Assert(bf, check.IsNil)
			c.Assert(terror.ErrBackoffArgsNotValid.Equal(err), check.IsTrue)
		} else {
			c.Assert(err, check.IsNil)
		}
	}
}

func (t *testBackoffSuite) TestExponentialBackoff(c *check.C) {
	var (
		min            = 1 * time.Millisecond
		max            = 1 * time.Second
		factor float64 = 2
	)
	b := &Backoff{
		Min:    min,
		Max:    max,
		Factor: factor,
	}

	for i := 0; i < 10; i++ {
		expected := min * time.Duration(math.Pow(factor, float64(i)))
		c.Assert(b.Duration(), check.Equals, expected)
	}
	b.Rollback()
	c.Assert(b.Current(), check.Equals, 512*min)
	b.Forward()
	for i := 0; i < 10; i++ {
		c.Assert(b.Duration(), check.Equals, max)
	}
	b.Reset()
	c.Assert(b.Duration(), check.Equals, min)
}

func (t *testBackoffSuite) checkBetween(c *check.C, value, low, high time.Duration) {
	c.Assert(value > low, check.IsTrue)
	c.Assert(value < high, check.IsTrue)
}

func (t *testBackoffSuite) TestBackoffJitter(c *check.C) {
	var (
		min            = 1 * time.Millisecond
		max            = 1 * time.Second
		factor float64 = 2
	)
	b := &Backoff{
		Min:    min,
		Max:    max,
		Factor: factor,
		Jitter: true,
	}
	c.Assert(b.Duration(), check.Equals, min)
	t.checkBetween(c, b.Duration(), min, 2*min)
	t.checkBetween(c, b.Duration(), 2*min, 4*min)
	t.checkBetween(c, b.Duration(), 4*min, 8*min)
	b.Reset()
	c.Assert(b.Duration(), check.Equals, min)
}

func (t *testBackoffSuite) TestFixedBackoff(c *check.C) {
	var (
		min            = 100 * time.Millisecond
		max            = 100 * time.Millisecond
		factor float64 = 2
	)
	b := &Backoff{
		Min:    min,
		Max:    max,
		Factor: factor,
	}
	for i := 0; i < 10; i++ {
		c.Assert(b.Duration(), check.Equals, max)
	}
}

func (t *testBackoffSuite) TestOverflowBackoff(c *check.C) {
	testCases := []struct {
		min    time.Duration
		max    time.Duration
		factor float64
	}{
		{time.Duration(math.MaxInt64/2 + math.MaxInt64/4 + 2), time.Duration(math.MaxInt64), 2},
		{time.Duration(math.MaxInt64/2 + 1), time.Duration(math.MaxInt64), 2},
		{time.Duration(math.MaxInt64), time.Duration(math.MaxInt64), 2},
	}
	for _, tc := range testCases {
		b := &Backoff{
			Min:    tc.min,
			Max:    tc.max,
			Factor: tc.factor,
		}
		c.Assert(b.Duration(), check.Equals, tc.min)
		c.Assert(b.Duration(), check.Equals, tc.max)
	}
}

func (t *testBackoffSuite) TestForward(c *check.C) {
	var (
		factor float64 = 2
		min            = 1 * time.Second
		max            = 5 * time.Second
		n              = 10
	)
	b := &Backoff{
		Min:    min,
		Max:    max,
		Factor: factor,
	}
	for i := 0; i < n; i++ {
		b.Forward()
	}
	c.Assert(b.cwnd, check.Equals, n)
	b.Reset()
	c.Assert(b.cwnd, check.Equals, 0)
	for i := 0; i < n; i++ {
		b.BoundaryForward()
	}
	c.Assert(b.cwnd, check.Equals, 3)
}
