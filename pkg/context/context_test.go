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

package context

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type contextSuite struct{}

var _ = check.Suite(&contextSuite{})

func (s *contextSuite) TestVars(c *check.C) {
	defer testleak.AfterTest(c)()
	stdCtx := context.Background()
	conf := config.GetDefaultReplicaConfig()
	conf.Filter.Rules = []string{"hello.world"}
	info := &model.ChangeFeedInfo{Config: conf}
	ctx := NewContext(stdCtx, &GlobalVars{
		CaptureInfo: &model.CaptureInfo{ID: "capture1"},
	})
	ctx = WithChangefeedVars(ctx, &ChangefeedVars{
		Info: info,
	})
	c.Assert(ctx.ChangefeedVars().Info, check.DeepEquals, info)
	c.Assert(ctx.GlobalVars().CaptureInfo.ID, check.Equals, "capture1")
}

func (s *contextSuite) TestStdCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	stdCtx := context.Background()
	stdCtx, cancel := context.WithCancel(stdCtx)
	ctx := NewContext(stdCtx, &GlobalVars{})
	cancel()
	<-ctx.Done()
}

func (s *contextSuite) TestCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx, cancel := WithCancel(ctx)
	cancel()
	<-ctx.Done()
}

func (s *contextSuite) TestCancelCascade(c *check.C) {
	defer testleak.AfterTest(c)()
	startTime := time.Now()
	stdCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx1, _ := WithCancel(ctx)
	ctx2, cancel2 := WithCancel(ctx)
	cancel2()
	<-ctx2.Done()
	c.Assert(time.Since(startTime), check.Less, time.Second)
	<-ctx1.Done()
	c.Assert(time.Since(startTime), check.GreaterEqual, time.Second)
	cancel()
}

func (s *contextSuite) TestThrow(c *check.C) {
	defer testleak.AfterTest(c)()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx, cancel := WithCancel(ctx)
	ctx = WithErrorHandler(ctx, func(err error) error {
		c.Assert(err.Error(), check.Equals, "mock error")
		cancel()
		return nil
	})
	ctx.Throw(nil)
	ctx.Throw(errors.New("mock error"))
	<-ctx.Done()
}

func (s *contextSuite) TestThrowCascade(c *check.C) {
	defer testleak.AfterTest(c)()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx, cancel := WithCancel(ctx)
	var errNum1, errNum2, errNum3 int
	ctx = WithErrorHandler(ctx, func(err error) error {
		if err.Error() == "mock error" {
			errNum1++
		} else if err.Error() == "mock error2" {
			errNum2++
		} else {
			c.Fail()
		}
		return nil
	})
	ctx2 := WithErrorHandler(ctx, func(err error) error {
		if err.Error() == "mock error2" {
			errNum2++
			return err
		} else if err.Error() == "mock error3" {
			errNum3++
		} else {
			c.Fail()
		}
		return nil
	})
	ctx2.Throw(errors.New("mock error2"))
	ctx2.Throw(errors.New("mock error3"))
	ctx.Throw(errors.New("mock error"))
	c.Assert(errNum1, check.Equals, 1)
	c.Assert(errNum2, check.Equals, 2)
	c.Assert(errNum3, check.Equals, 1)
	cancel()
	<-ctx.Done()
}

func (s *contextSuite) TestThrowPanic(c *check.C) {
	defer testleak.AfterTest(c)()
	defer func() {
		panicMsg := recover()
		c.Assert(panicMsg, check.Equals, "an error has escaped, please report a bug")
	}()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx.Throw(nil)
	ctx.Throw(errors.New("mock error"))
}
