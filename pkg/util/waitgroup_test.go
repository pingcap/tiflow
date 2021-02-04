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
