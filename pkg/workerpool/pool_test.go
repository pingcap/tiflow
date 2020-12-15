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
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
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
	})

	errg.Go(func() error {
		for i := 0; i < 10; i++ {
			err := handle.AddEvent(ctx, i)
			c.Assert(err, check.IsNil)
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
