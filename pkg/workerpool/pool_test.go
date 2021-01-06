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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type workerPoolSuite struct{}

var _ = check.Suite(&workerPoolSuite{})

func (s *workerPoolSuite) TestTaskError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	handle := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
		if event.(int) == 3 {
			return errors.New("test error")
		}
		return nil
	}).OnExit(func(err error) {
		c.Assert(err, check.ErrorMatches, "test error")
	})

	errg.Go(func() error {
		for i := 0; i < 10; i++ {
			err := handle.AddEvent(ctx, i)
			if err != nil {
				c.Assert(err, check.ErrorMatches, ".*ErrWorkerPoolHandleCancelled.*")
				return nil
			}
		}
		return nil
	})

	select {
	case <-ctx.Done():
		c.FailNow()
	case err := <-handle.ErrCh():
		c.Assert(err, check.ErrorMatches, "test error")
	}
	cancel()

	err := errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}

func (s *workerPoolSuite) TestTimerError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	counter := 0
	handle := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
		return nil
	}).SetTimer(ctx, time.Millisecond*200, func(ctx context.Context) error {
		if counter == 3 {
			return errors.New("timer error")
		}
		counter++
		return nil
	})

	select {
	case <-ctx.Done():
		c.FailNow()
	case err := <-handle.ErrCh():
		c.Assert(err, check.ErrorMatches, "timer error")
	}
	cancel()

	err := errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}

func (s *workerPoolSuite) TestMultiError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	handle := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
		if event.(int) >= 3 {
			return errors.New("test error")
		}
		return nil
	})

	errg.Go(func() error {
		for i := 0; i < 10; i++ {
			err := handle.AddEvent(ctx, i)
			if err != nil {
				c.Assert(err, check.ErrorMatches, ".*ErrWorkerPoolHandleCancelled.*")
			}
		}
		return nil
	})

	select {
	case <-ctx.Done():
		c.FailNow()
	case err := <-handle.ErrCh():
		c.Assert(err, check.ErrorMatches, "test error")
	}
	cancel()

	err := errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}

func (s *workerPoolSuite) TestCancelHandle(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	var num int32
	handle := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
		atomic.StoreInt32(&num, int32(event.(int)))
		return nil
	})

	errg.Go(func() error {
		i := 0
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			err := handle.AddEvent(ctx, i)
			if err != nil {
				c.Assert(err, check.ErrorMatches, ".*ErrWorkerPoolHandleCancelled.*")
				c.Assert(i, check.GreaterEqual, 5000)
				return nil
			}
			i++
		}
	})

	for {
		select {
		case <-ctx.Done():
			c.FailNow()
		default:
		}
		if atomic.LoadInt32(&num) > 5000 {
			break
		}
	}

	err := failpoint.Enable("github.com/pingcap/ticdc/pkg/workerpool/addEventDelayPoint", "1*sleep(500)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/pkg/workerpool/addEventDelayPoint")
	}()

	handle.Unregister()
	handle.Unregister() // Unregistering many times does not matter
	handle.Unregister()

	lastNum := atomic.LoadInt32(&num)
	for i := 0; i <= 1000; i++ {
		c.Assert(atomic.LoadInt32(&num), check.Equals, lastNum)
	}

	time.Sleep(1 * time.Second)
	cancel()

	err = errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}

func (s *workerPoolSuite) TestCancelTimer(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	err := failpoint.Enable("github.com/pingcap/ticdc/pkg/workerpool/unregisterDelayPoint", "sleep(5000)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/pkg/workerpool/unregisterDelayPoint")
	}()

	handle := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
		return nil
	}).SetTimer(ctx, 200*time.Millisecond, func(ctx context.Context) error {
		return nil
	})

	errg.Go(func() error {
		i := 0
		for {
			err := handle.AddEvent(ctx, i)
			if err != nil {
				c.Assert(err, check.ErrorMatches, ".*ErrWorkerPoolHandleCancelled.*")
				return nil
			}
			i++
		}
	})

	handle.Unregister()

	cancel()
	err = errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}

func (s *workerPoolSuite) TestTimer(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return pool.Run(ctx)
	})

	time.Sleep(200 * time.Millisecond)

	handle := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
		if event.(int) == 3 {
			return errors.New("test error")
		}
		return nil
	})

	var lastTime time.Time
	count := 0
	handle.SetTimer(ctx, time.Second*1, func(ctx context.Context) error {
		if !lastTime.IsZero() {
			c.Assert(time.Since(lastTime), check.GreaterEqual, 900*time.Millisecond)
			c.Assert(time.Since(lastTime), check.LessEqual, 1200*time.Millisecond)
		}
		if count == 3 {
			cancel()
			return nil
		}
		count++

		lastTime = time.Now()
		return nil
	})

	err := errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}

func (s *workerPoolSuite) TestBasics(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	pool := newDefaultPoolImpl(&defaultHasher{}, 4)
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return pool.Run(ctx)
	})

	var wg sync.WaitGroup

	wg.Add(16)
	for i := 0; i < 16; i++ {
		finalI := i
		resultCh := make(chan int, 128)
		handler := pool.RegisterEvent(func(ctx context.Context, event interface{}) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resultCh <- event.(int):
			}
			log.Debug("result added", zap.Int("id", finalI), zap.Int("result", event.(int)))
			return nil
		})

		errg.Go(func() error {
			for j := 0; j < 256; j++ {
				err := handler.AddEvent(ctx, j)
				if err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		})

		errg.Go(func() error {
			defer wg.Done()
			nextExpected := 0
			for n := range resultCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				log.Debug("result received", zap.Int("id", finalI), zap.Int("result", n))
				c.Assert(n, check.Equals, nextExpected)
				nextExpected++
				if nextExpected == 256 {
					break
				}
			}
			return nil
		})
	}

	wg.Wait()
	cancel()

	err := errg.Wait()
	c.Assert(err, check.ErrorMatches, "context canceled")
}
