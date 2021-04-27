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

package context

import (
	"context"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type customCancelSuite struct{}

var _ = check.Suite(&customCancelSuite{})

func (s *customCancelSuite) TestCustomCancelBasic(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := WithCustomErrCancel(context.TODO())

	go func() {
		time.Sleep(time.Second)
		cancel(errors.New("test custom error"))
	}()

	<-ctx.Done()
	c.Assert(ctx.Err(), check.ErrorMatches, "test custom error")
}

func (s *customCancelSuite) TestCustomCancelInnerCanceled(c *check.C) {
	defer testleak.AfterTest(c)()

	innerCtx, cancel := context.WithCancel(context.TODO())
	ctx, cancel1 := WithCustomErrCancel(innerCtx)
	defer cancel1(nil)

	go func() {
		time.Sleep(time.Second)
		cancel()
	}()

	<-ctx.Done()
	c.Assert(ctx.Err(), check.ErrorMatches, ".*canceled.*")
}

func (s *customCancelSuite) TestCustomCancelDoubleCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := WithCustomErrCancel(context.TODO())

	go func() {
		time.Sleep(time.Second)
		cancel(errors.New("test custom error"))
		cancel(errors.New("test custom double error"))
	}()

	<-ctx.Done()
	c.Assert(ctx.Err(), check.ErrorMatches, "test custom error")
}

func (s *customCancelSuite) TestDeadlinePropagated(c *check.C) {
	defer testleak.AfterTest(c)()

	deadline := time.Now().Add(3 * time.Second)
	innerCtx, cancel := context.WithDeadline(context.TODO(), deadline)
	defer cancel()

	ctx, cancel1 := WithCustomErrCancel(innerCtx)
	defer cancel1(nil)

	obtainedDDL, ok := ctx.Deadline()
	c.Assert(ok, check.IsTrue)
	c.Assert(obtainedDDL, check.Equals, deadline)

	<-ctx.Done()
	c.Assert(ctx.Err(), check.ErrorMatches, ".*deadline.*")
}

type testContextKey struct{}

func (s *customCancelSuite) TestValuePropagated(c *check.C) {
	defer testleak.AfterTest(c)()

	innerCtx := context.WithValue(context.TODO(), testContextKey{}, "test_value")
	ctx, cancel := WithCustomErrCancel(innerCtx)
	defer cancel(nil)

	c.Assert(ctx.Value(testContextKey{}), check.Equals, "test_value")
}
