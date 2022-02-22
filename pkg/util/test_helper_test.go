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

package util

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type testHelperSuite struct{}

var _ = check.Suite(&testHelperSuite{})

func (s *testHelperSuite) TestWaitSomething(c *check.C) {
	defer testleak.AfterTest(c)()
	var (
		backoff  = 10
		waitTime = 10 * time.Millisecond
		count    = 0
	)

	// wait fail
	f1 := func() bool {
		count++
		return false
	}
	c.Assert(WaitSomething(backoff, waitTime, f1), check.IsFalse)
	c.Assert(count, check.Equals, backoff)

	count = 0 // reset
	// wait success
	f2 := func() bool {
		count++
		return count >= 5
	}

	c.Assert(WaitSomething(backoff, waitTime, f2), check.IsTrue)
	c.Assert(count, check.Equals, 5)
}

func (s *testHelperSuite) TestHandleErr(c *check.C) {
	defer testleak.AfterTest(c)()
	var (
		ctx, cancel = context.WithCancel(context.Background())
		errCh       = make(chan error)
		count       int32
	)
	errg := HandleErrWithErrGroup(ctx, errCh, func(e error) { atomic.AddInt32(&count, 1) })
	for i := 0; i < 5; i++ {
		errCh <- errors.New("test error")
	}
	c.Assert(WaitSomething(5, time.Millisecond*10, func() bool { return atomic.LoadInt32(&count) == int32(5) }), check.IsTrue)
	cancel()
	c.Assert(errg.Wait(), check.IsNil)
}
