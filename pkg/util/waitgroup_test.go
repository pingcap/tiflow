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

package util

import (
	"sync"
	"time"

	"github.com/pingcap/check"
)

type waitgroupSuite struct{}

var _ = check.Suite(waitgroupSuite{})

func (s *gcServiceSuite) TestWaitTimeout(c *check.C) {
	var wg sync.WaitGroup
	wg.Add(1)
	c.Assert(WaitTimeout(&wg, 100*time.Millisecond), check.IsTrue)
}
