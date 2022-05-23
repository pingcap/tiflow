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

package flowcontrol

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func dummyCallBack(_ bool) error {
	return nil
}

type mockCallBacker struct {
	timesCalled int
	injectedErr error
}

func (c *mockCallBacker) cb(_ bool) error {
	c.timesCalled += 1
	return c.injectedErr
}

func TestMemoryQuotaBasic(t *testing.T) {
	t.Parallel()

	controller := newTableMemoryQuota(1024)
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
			err := controller.consumeWithBlocking(uint64(size), dummyCallBack)
			require.Nil(t, err)

			require.Less(t, atomic.AddUint64(&consumed, uint64(size)), uint64(1024))
			sizeCh <- uint64(size)
		}

		close(sizeCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for size := range sizeCh {
			require.GreaterOrEqual(t, atomic.LoadUint64(&consumed), size)
			atomic.AddUint64(&consumed, -size)
			controller.release(size)
		}
	}()

	wg.Wait()
	require.Equal(t, uint64(0), atomic.LoadUint64(&consumed))
	require.Equal(t, uint64(0), controller.getConsumption())
}

func TestMemoryQuotaForceConsume(t *testing.T) {
	t.Parallel()

	controller := newTableMemoryQuota(1024)
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
				err := controller.consumeWithBlocking(uint64(size), dummyCallBack)
				require.Nil(t, err)
				require.Less(t, atomic.AddUint64(&consumed, uint64(size)), uint64(1024))
			} else {
				err := controller.forceConsume(uint64(size))
				require.Nil(t, err)
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
			require.GreaterOrEqual(t, atomic.LoadUint64(&consumed), size)
			atomic.AddUint64(&consumed, -size)
			controller.release(size)
		}
	}()

	wg.Wait()
	require.Equal(t, uint64(0), atomic.LoadUint64(&consumed))
}

// TestMemoryQuotaAbort verifies that abort works
func TestMemoryQuotaAbort(t *testing.T) {
	t.Parallel()

	controller := newTableMemoryQuota(1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := controller.consumeWithBlocking(700, dummyCallBack)
		require.Nil(t, err)

		err = controller.consumeWithBlocking(700, dummyCallBack)
		require.Regexp(t, ".*ErrFlowControllerAborted.*", err)

		err = controller.forceConsume(700)
		require.Regexp(t, ".*ErrFlowControllerAborted.*", err)
	}()

	time.Sleep(2 * time.Second)
	controller.abort()

	wg.Wait()
}

// TestMemoryQuotaReleaseZero verifies that releasing 0 bytes is successful
func TestMemoryQuotaReleaseZero(t *testing.T) {
	t.Parallel()

	controller := newTableMemoryQuota(1024)
	controller.release(0)
}

type mockedEvent struct {
	resolvedTs uint64
	size       uint64
}

func TestFlowControlBasic(t *testing.T) {
	t.Parallel()

	var consumedBytes uint64
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	mockedRowsCh := make(chan *txnSizeEntry, 1024)
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
			case mockedRowsCh <- &txnSizeEntry{
				commitTs: lastCommitTs,
				size:     size,
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
			var mockedRow *txnSizeEntry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case mockedRow = <-mockedRowsCh:
			}

			if mockedRow == nil {
				break
			}

			atomic.AddUint64(&consumedBytes, mockedRow.size)
			updatedResolvedTs := false
			if resolvedTs != mockedRow.commitTs {
				require.Less(t, resolvedTs, mockedRow.commitTs)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case eventCh <- &mockedEvent{
					resolvedTs: resolvedTs,
				}:
				}
				resolvedTs = mockedRow.commitTs
				updatedResolvedTs = true
			}
			err := flowController.Consume(model.NewEmptyPolymorphicEvent(mockedRow.commitTs),
				mockedRow.size, dummyCallBack)
			require.Nil(t, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case eventCh <- &mockedEvent{
				size: mockedRow.size,
			}:
			}
			if updatedResolvedTs {
				// new Txn
				require.Less(t, atomic.LoadUint64(&consumedBytes), uint64(2048))
				require.Less(t, flowController.GetConsumption(), uint64(2048))
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

	require.Nil(t, errg.Wait())
	require.Equal(t, uint64(0), atomic.LoadUint64(&consumedBytes))
}

func TestFlowControlAbort(t *testing.T) {
	t.Parallel()

	callBacker := &mockCallBacker{}
	controller := NewTableFlowController(1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := controller.Consume(model.NewEmptyPolymorphicEvent(1), 1000, callBacker.cb)
		require.Nil(t, err)
		require.Equal(t, 0, callBacker.timesCalled)
		err = controller.Consume(model.NewEmptyPolymorphicEvent(2), 1000, callBacker.cb)
		require.Regexp(t, ".*ErrFlowControllerAborted.*", err)
		require.Equal(t, 1, callBacker.timesCalled)
		err = controller.Consume(model.NewEmptyPolymorphicEvent(2), 10, callBacker.cb)
		require.Regexp(t, ".*ErrFlowControllerAborted.*", err)
		require.Equal(t, 1, callBacker.timesCalled)
	}()

	time.Sleep(3 * time.Second)
	controller.Abort()

	wg.Wait()
}

func TestFlowControlCallBack(t *testing.T) {
	t.Parallel()

	var consumedBytes uint64
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	mockedRowsCh := make(chan *txnSizeEntry, 1024)
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
			case mockedRowsCh <- &txnSizeEntry{
				commitTs: lastCommitTs,
				size:     size,
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
			var mockedRow *txnSizeEntry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case mockedRow = <-mockedRowsCh:
			}

			if mockedRow == nil {
				break
			}

			atomic.AddUint64(&consumedBytes, mockedRow.size)
			err := flowController.Consume(model.NewEmptyPolymorphicEvent(mockedRow.commitTs),
				mockedRow.size, func(bool) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case eventCh <- &mockedEvent{
						resolvedTs: lastCRTs,
					}:
					}
					return nil
				})
			require.Nil(t, err)
			lastCRTs = mockedRow.commitTs

			select {
			case <-ctx.Done():
				return ctx.Err()
			case eventCh <- &mockedEvent{
				size: mockedRow.size,
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

	require.Nil(t, errg.Wait())
	require.Equal(t, uint64(0), atomic.LoadUint64(&consumedBytes))
}

func TestFlowControlCallBackNotBlockingRelease(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	controller := NewTableFlowController(512)
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go func() {
		defer wg.Done()
		err := controller.Consume(model.NewEmptyPolymorphicEvent(1), 511, func(bool) error {
			t.Error("unreachable")
			return nil
		})
		require.Nil(t, err)

		var isBlocked int32
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-time.After(time.Second * 1)
			// makes sure that this test case is valid
			require.Equal(t, int32(1), atomic.LoadInt32(&isBlocked))
			controller.Release(1)
			cancel()
		}()

		err = controller.Consume(model.NewEmptyPolymorphicEvent(2), 511, func(bool) error {
			atomic.StoreInt32(&isBlocked, 1)
			<-ctx.Done()
			atomic.StoreInt32(&isBlocked, 0)
			return ctx.Err()
		})

		require.Regexp(t, ".*context canceled.*", err)
	}()

	wg.Wait()
}

func TestFlowControlCallBackError(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	controller := NewTableFlowController(512)
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go func() {
		defer wg.Done()
		err := controller.Consume(model.NewEmptyPolymorphicEvent(1), 511, func(bool) error {
			t.Error("unreachable")
			return nil
		})
		require.Nil(t, err)
		err = controller.Consume(model.NewEmptyPolymorphicEvent(2), 511, func(bool) error {
			<-ctx.Done()
			return ctx.Err()
		})
		require.Regexp(t, ".*context canceled.*", err)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	wg.Wait()
}

func TestFlowControlConsumeLargerThanQuota(t *testing.T) {
	t.Parallel()

	controller := NewTableFlowController(1024)
	err := controller.Consume(model.NewEmptyPolymorphicEvent(1), 2048, func(bool) error {
		t.Error("unreachable")
		return nil
	})
	require.Regexp(t, ".*ErrFlowControllerEventLargerThanQuota.*", err)
}

func BenchmarkTableFlowController(B *testing.B) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	mockedRowsCh := make(chan *txnSizeEntry, 102400)
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
			case mockedRowsCh <- &txnSizeEntry{
				commitTs: lastCommitTs,
				size:     size,
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
			var mockedRow *txnSizeEntry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case mockedRow = <-mockedRowsCh:
			}

			if mockedRow == nil {
				break
			}

			if resolvedTs != mockedRow.commitTs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case eventCh <- &mockedEvent{
					resolvedTs: resolvedTs,
				}:
				}
				resolvedTs = mockedRow.commitTs
			}
			err := flowController.Consume(model.NewEmptyPolymorphicEvent(mockedRow.commitTs),
				mockedRow.size, dummyCallBack)
			if err != nil {
				B.Fatal(err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case eventCh <- &mockedEvent{
				size: mockedRow.size,
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
