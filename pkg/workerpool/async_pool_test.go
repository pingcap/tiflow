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

package workerpool

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"golang.org/x/sync/errgroup"
)

type asyncPoolSuite struct{}

var _ = check.Suite(&asyncPoolSuite{})

func (s *asyncPoolSuite) TestBasic(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	errg, ctx := errgroup.WithContext(ctx)

	pool := newDefaultAsyncPoolImpl(4)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	var sum int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		finalI := i
		err := pool.Go(ctx, func() {
			time.Sleep(time.Millisecond * time.Duration(rand.Int()%100))
			atomic.AddInt32(&sum, int32(finalI+1))
			wg.Done()
		})
		c.Assert(err, check.IsNil)
	}

	wg.Wait()
	c.Assert(sum, check.Equals, int32(5050))

	cancel()
	err := errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}
