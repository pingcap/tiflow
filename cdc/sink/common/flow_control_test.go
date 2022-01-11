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
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"golang.org/x/sync/errgroup"
)

type flowControlSuite struct{}

var _ = check.Suite(&flowControlSuite{})

func dummyCallBack() error {
	return nil
}

type mockCallBacker struct {
	timesCalled int
	injectedErr error
}

func (c *mockCallBacker) cb() error {
	c.timesCalled += 1
	return c.injectedErr
}

func (s *flowControlSuite) TestMemoryQuotaBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemoryQuota(1024)
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
			err := controller.ConsumeWithBlocking(uint64(size), dummyCallBack)
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
	c.Assert(controller.GetConsumption(), check.Equals, uint64(0))
}

func (s *flowControlSuite) TestMemoryQuotaForceConsume(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemoryQuota(1024)
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
				err := controller.ConsumeWithBlocking(uint64(size), dummyCallBack)
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

// TestMemoryQuotaAbort verifies that Abort works
func (s *flowControlSuite) TestMemoryQuotaAbort(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemoryQuota(1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := controller.ConsumeWithBlocking(700, dummyCallBack)
		c.Assert(err, check.IsNil)

		err = controller.ConsumeWithBlocking(700, dummyCallBack)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")

		err = controller.ForceConsume(700)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")
	}()

	time.Sleep(2 * time.Second)
	controller.Abort()

	wg.Wait()
}

// TestMemoryQuotaReleaseZero verifies that releasing 0 bytes is successful
func (s *flowControlSuite) TestMemoryQuotaReleaseZero(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableMemoryQuota(1024)
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
		for i := 0; i < 10000; i++ {
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
			err := flowController.Consume(mockedRow.CommitTs, mockedRow.Size, dummyCallBack)
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
				c.Assert(flowController.GetConsumption(), check.Less, uint64(2048))
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

	callBacker := &mockCallBacker{}
	controller := NewTableFlowController(1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := controller.Consume(1, 1000, callBacker.cb)
		c.Assert(err, check.IsNil)
		c.Assert(callBacker.timesCalled, check.Equals, 0)
		err = controller.Consume(2, 1000, callBacker.cb)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")
		c.Assert(callBacker.timesCalled, check.Equals, 1)
		err = controller.Consume(2, 10, callBacker.cb)
		c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerAborted.*")
		c.Assert(callBacker.timesCalled, check.Equals, 1)
	}()

	time.Sleep(3 * time.Second)
	controller.Abort()

	wg.Wait()
}

func (s *flowControlSuite) TestFlowControlCallBack(c *check.C) {
	defer testleak.AfterTest(c)()
	var consumedBytes uint64
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	mockedRowsCh := make(chan *commitTsSizeEntry, 1024)
	flowController := NewTableFlowController(512)

	errg.Go(func() error {
		lastCommitTs := uint64(1)
		for i := 0; i < 10000; i++ {
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
		lastCRTs := uint64(0)
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
			err := flowController.Consume(mockedRow.CommitTs, mockedRow.Size, func() error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case eventCh <- &mockedEvent{
					resolvedTs: lastCRTs,
				}:
				}
				return nil
			})
			c.Assert(err, check.IsNil)
			lastCRTs = mockedRow.CommitTs

			select {
			case <-ctx.Done():
				return ctx.Err()
			case eventCh <- &mockedEvent{
				size: mockedRow.Size,
			}:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case eventCh <- &mockedEvent{
			resolvedTs: lastCRTs,
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

func (s *flowControlSuite) TestFlowControlCallBackNotBlockingRelease(c *check.C) {
	defer testleak.AfterTest(c)()

	var wg sync.WaitGroup
	controller := NewTableFlowController(512)
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go func() {
		defer wg.Done()
		err := controller.Consume(1, 511, func() error {
			c.Fatalf("unreachable")
			return nil
		})
		c.Assert(err, check.IsNil)

		var isBlocked int32
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-time.After(time.Second * 1)
			// makes sure that this test case is valid
			c.Assert(atomic.LoadInt32(&isBlocked), check.Equals, int32(1))
			controller.Release(1)
			cancel()
		}()

		err = controller.Consume(2, 511, func() error {
			atomic.StoreInt32(&isBlocked, 1)
			<-ctx.Done()
			atomic.StoreInt32(&isBlocked, 0)
			return ctx.Err()
		})

		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	wg.Wait()
}

func (s *flowControlSuite) TestFlowControlCallBackError(c *check.C) {
	defer testleak.AfterTest(c)()

	var wg sync.WaitGroup
	controller := NewTableFlowController(512)
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go func() {
		defer wg.Done()
		err := controller.Consume(1, 511, func() error {
			c.Fatalf("unreachable")
			return nil
		})
		c.Assert(err, check.IsNil)
		err = controller.Consume(2, 511, func() error {
			<-ctx.Done()
			return ctx.Err()
		})
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	wg.Wait()
}

func (s *flowControlSuite) TestFlowControlConsumeLargerThanQuota(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewTableFlowController(1024)
	err := controller.Consume(1, 2048, func() error {
		c.Fatalf("unreachable")
		return nil
	})
	c.Assert(err, check.ErrorMatches, ".*ErrFlowControllerEventLargerThanQuota.*")
}

func BenchmarkTableFlowController(B *testing.B) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	mockedRowsCh := make(chan *commitTsSizeEntry, 102400)
	flowController := NewTableFlowController(20 * 1024 * 1024) // 20M

	errg.Go(func() error {
		lastCommitTs := uint64(1)
		for i := 0; i < B.N; i++ {
			if rand.Int()%15 == 0 {
				lastCommitTs += 10
			}
			size := uint64(1024 + rand.Int()%1024)
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

	eventCh := make(chan *mockedEvent, 102400)
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

			if resolvedTs != mockedRow.CommitTs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case eventCh <- &mockedEvent{
					resolvedTs: resolvedTs,
				}:
				}
				resolvedTs = mockedRow.CommitTs
			}
			err := flowController.Consume(mockedRow.CommitTs, mockedRow.Size, dummyCallBack)
			if err != nil {
				B.Fatal(err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case eventCh <- &mockedEvent{
				size: mockedRow.Size,
			}:
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

			if event.size == 0 {
				flowController.Release(event.resolvedTs)
			}
		}

		return nil
	})
}
