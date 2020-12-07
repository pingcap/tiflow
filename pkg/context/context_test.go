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

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/config"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type contextSuite struct{}

var _ = check.Suite(&contextSuite{})

func (s *contextSuite) TestVars(c *check.C) {
	stdCtx := context.Background()
	conf := config.GetDefaultReplicaConfig()
	conf.Filter.Rules = []string{"hello.world"}
	ctx, _ := NewContext(stdCtx, &Vars{
		Config: conf,
	})
	c.Assert(ctx.Vars().Config, check.DeepEquals, conf)
}

func (s *contextSuite) TestStdCancel(c *check.C) {
	stdCtx := context.Background()
	stdCtx, cancel := context.WithCancel(stdCtx)
	ctx, _ := NewContext(stdCtx, &Vars{})
	cancel()
	<-ctx.StdContext().Done()
	<-ctx.Done()
}

func (s *contextSuite) TestCancel(c *check.C) {
	stdCtx := context.Background()
	ctx, cancel := NewContext(stdCtx, &Vars{})
	cancel()
	<-ctx.StdContext().Done()
	<-ctx.Done()
}

func (s *contextSuite) TestThrow(c *check.C) {
	stdCtx := context.Background()
	ctx, cancel := NewContext(stdCtx, &Vars{})
	ctx = WatchThrow(ctx, func(err error) {
		c.Assert(err.Error(), check.Equals, "mock error")
		cancel()
	})
	ctx.Throw(errors.New("mock error"))
	<-ctx.Done()
}
