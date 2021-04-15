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

package common

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

type flowControlSuite struct{}

var _ = check.Suite(&flowControlSuite{})

func (s *flowControlSuite) TestMemoryControlBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemorySizeController(1024)
	sizeCh := make(chan uint64, 1024)
	var (
		wg       sync.WaitGroup
		consumed uint64
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 100000; i++ {
			size := (rand.Int() % 128) + 128
			err := controller.ConsumeWithBlocking(uint64(size))
			c.Assert(err, check.IsNil)

			c.Assert(atomic.AddUint64(&consumed, uint64(size)), check.Less, uint64(1024))
			sizeCh <- uint64(size)
		}

		close(sizeCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for size := range sizeCh {
			c.Assert(atomic.LoadUint64(&consumed), check.GreaterEqual, size)
			atomic.AddUint64(&consumed, -size)
			controller.Release(size)
		}
	}()

	wg.Wait()
	c.Assert(atomic.LoadUint64(&consumed), check.Equals, uint64(0))
}

func (s *flowControlSuite) TestMemoryControlForceConsume(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemorySizeController(1024)
	sizeCh := make(chan uint64, 1024)
	var (
		wg       sync.WaitGroup
		consumed uint64
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 100000; i++ {
			size := (rand.Int() % 128) + 128

			if rand.Int()%3 == 0 {
				err := controller.ConsumeWithBlocking(uint64(size))
				c.Assert(err, check.IsNil)
				c.Assert(atomic.AddUint64(&consumed, uint64(size)), check.Less, uint64(1024))
			} else {
				err := controller.ForceConsume(uint64(size))
				c.Assert(err, check.IsNil)
				atomic.AddUint64(&consumed, uint64(size))
			}
			sizeCh <- uint64(size)
		}

		close(sizeCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for size := range sizeCh {
			c.Assert(atomic.LoadUint64(&consumed), check.GreaterEqual, size)
			atomic.AddUint64(&consumed, -size)
			controller.Release(size)
		}
	}()

	wg.Wait()
	c.Assert(atomic.LoadUint64(&consumed), check.Equals, uint64(0))
}

// TestMemoryControlAbort verifies that Abort works
func (s *flowControlSuite) TestMemoryControlAbort(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemorySizeController(1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := controller.ConsumeWithBlocking(700)
		c.Assert(err, check.IsNil)

		err = controller.ConsumeWithBlocking(700)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")

		err = controller.ForceConsume(700)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")
	}()

	time.Sleep(2 * time.Second)
	controller.Abort()

	wg.Wait()
}

// TestMemoryControlReleaseZero verifies that releasing 0 bytes is successful
func (s *flowControlSuite) TestMemoryControlReleaseZero(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemorySizeController(1024)
	controller.Release(0)
}

type mockedEvent struct {
	resolvedTs uint64
	size       uint64
}

func (s *flowControlSuite) TestFlowControlBasic(c *check.C) {
	defer testleak.AfterTest(c)()
	var consumedBytes uint64
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	mockedRowsCh := make(chan *commitTsSizeEntry, 1024)
	flowController := NewTableFlowController(2048)

	errg.Go(func() error {
		lastCommitTs := uint64(1)
		for i := 0; i < 100000; i++ {
			if rand.Int()%15 == 0 {
				lastCommitTs += 10
			}
			size := uint64(128 + rand.Int()%64)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case mockedRowsCh <- &commitTsSizeEntry{
				CommitTs: lastCommitTs,
				Size:     size,
			}:
			}
		}

		close(mockedRowsCh)
		return nil
	})

	eventCh := make(chan *mockedEvent, 1024)
	errg.Go(func() error {
		defer close(eventCh)
		resolvedTs := uint64(0)
		for {
			var mockedRow *commitTsSizeEntry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case mockedRow = <-mockedRowsCh:
			}

			if mockedRow == nil {
				break
			}

			atomic.AddUint64(&consumedBytes, mockedRow.Size)
			updatedResolvedTs := false
			if resolvedTs != mockedRow.CommitTs {
				c.Assert(resolvedTs, check.Less, mockedRow.CommitTs)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case eventCh <- &mockedEvent{
					resolvedTs: resolvedTs,
				}:
				}
				resolvedTs = mockedRow.CommitTs
				updatedResolvedTs = true
			}
			err := flowController.Consume(mockedRow.CommitTs, mockedRow.Size)
			c.Check(err, check.IsNil)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case eventCh <- &mockedEvent{
				size: mockedRow.Size,
			}:
			}
			if updatedResolvedTs {
				// new Txn
				c.Assert(atomic.LoadUint64(&consumedBytes), check.Less, uint64(2048))
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case eventCh <- &mockedEvent{
			resolvedTs: resolvedTs,
		}:
		}

		return nil
	})

	errg.Go(func() error {
		for {
			var event *mockedEvent
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event = <-eventCh:
			}

			if event == nil {
				break
			}

			if event.size != 0 {
				atomic.AddUint64(&consumedBytes, -event.size)
			} else {
				flowController.Release(event.resolvedTs)
			}
		}

		return nil
	})

	c.Assert(errg.Wait(), check.IsNil)
	c.Assert(atomic.LoadUint64(&consumedBytes), check.Equals, uint64(0))
}

func (s *flowControlSuite) TestFlowControlAbort(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableFlowController(1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := controller.Consume(1, 1000)
		c.Assert(err, check.IsNil)
		err = controller.Consume(2, 1000)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")
	}()

	time.Sleep(3 * time.Second)
	controller.Abort()

	wg.Wait()
}
