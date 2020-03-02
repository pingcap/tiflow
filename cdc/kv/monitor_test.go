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

package kv

import (
	"context"
	"time"

	"github.com/pingcap/check"
)

type MonitorSuite struct{}

var _ = check.Suite(&MonitorSuite{})

func (s *MonitorSuite) TestRegionActive(c *check.C) {
	var (
		ctx       = context.TODO()
		threshold = int64(1)
		reconnect = int64(2)
		rch       = make(chan struct{}, 1)
	)

	ra := newRegionActive(1000, threshold, reconnect)
	ra.SetKv()
	ra.SetResolve(100)
	ra.Check(ctx, rch)
	c.Assert(ra.IsResolveActive(), check.IsTrue)
	c.Assert(ra.IsKvActive(), check.IsTrue)

	time.Sleep(time.Second * time.Duration(threshold))
	ra.Check(ctx, rch)
	c.Assert(ra.IsResolveActive(), check.IsFalse)
	c.Assert(ra.IsKvActive(), check.IsFalse)

	ra.SetResolve(100)
	time.Sleep(time.Second * time.Duration(reconnect-threshold))
	ra.Check(ctx, rch)
	select {
	case <-rch:
	case <-time.After(time.Second):
		c.Error("failed to recv reconnect notify")
	}
}
