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

	"github.com/pingcap/log"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
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

func (s *asyncPoolSuite) TestEventuallyRun(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	errg, ctx := errgroup.WithContext(ctx)
	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()

	pool := newDefaultAsyncPoolImpl(4)
	errg.Go(func() error {
		defer cancelLoop()
		for i := 0; i < 10; i++ {
			log.Info("running pool")
			err := runForDuration(ctx, time.Millisecond*500, func(ctx context.Context) error {
				return pool.Run(ctx)
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})

	var sum int32
	var sumExpected int32
loop:
	for i := 0; ; i++ {
		select {
		case <-loopCtx.Done():
			break loop
		default:
		}
		finalI := i
		err := pool.Go(loopCtx, func() {
			if rand.Int()%128 == 0 {
				time.Sleep(2 * time.Millisecond)
			}
			atomic.AddInt32(&sum, int32(finalI+1))
		})
		if err != nil {
			c.Assert(err, check.ErrorMatches, "context canceled")
		} else {
			sumExpected += int32(i + 1)
		}
	}

	cancel()
	err := errg.Wait()
	c.Assert(err, check.IsNil)
	c.Assert(sum, check.Equals, sumExpected)
}

func runForDuration(ctx context.Context, duration time.Duration, f func(ctx context.Context) error) error {
	timedCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	errCh := make(chan error)
	go func() {
		errCh <- f(timedCtx)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if errors.Cause(err) == context.DeadlineExceeded {
			return nil
		}
		return errors.Trace(err)
	}
}
