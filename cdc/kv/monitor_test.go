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
	)

	ra := newRegionActive(1000, threshold)
	time.Sleep(time.Second * time.Duration(threshold))
	ra.Check(ctx)
	c.Assert(ra.resolveActive, check.IsFalse)
	c.Assert(ra.kvActive, check.IsFalse)

	ra.SetKv()
	ra.SetResolve()
	ra.Check(ctx)
	c.Assert(ra.resolveActive, check.IsTrue)
	c.Assert(ra.kvActive, check.IsTrue)
}
