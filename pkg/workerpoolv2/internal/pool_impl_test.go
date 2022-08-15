// Copyright 2022 PingCAP, Inc.
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

package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestPoolBasics(t *testing.T) {
	t.Parallel()

	const (
		workerNum         = 8
		handleNum         = 16
		producerNum       = 8
		eventPerHandler   = 1024 * 32
		handlePerProducer = handleNum / producerNum
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewPoolImpl(workerNum)

	var (
		handles  [handleNum]*EventHandleImpl[int64]
		counters [handleNum]atomic.Int64
		wg       sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		require.Error(t, pool.Run(ctx))
	}()

	for i := range handles {
		i := i
		handles[i] = NewEventHandlerImpl(func(n int64) error {
			require.Equal(t, n, counters[i].Add(1)-1)
			return nil
		}, 128, 128)
		pool.RegisterHandle(handles[i])
	}

	for i := 0; i < producerNum; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < eventPerHandler; j++ {
				for k := handlePerProducer * i; k < handlePerProducer*(i+1); k++ {
					if err := handles[k].AddEvent(ctx, int64(j)); err != nil {
						log.Panic("unexpected error", zap.Error(err))
					}
				}
			}
		}()
	}

	require.Eventually(t, func() bool {
		for i := range counters {
			if counters[i].Load() < eventPerHandler {
				return false
			}
		}
		return true
	}, 100*time.Second, 100*time.Millisecond)

	for _, handle := range handles {
		handle.Unregister()
	}

	cancel()
	wg.Wait()
}

func TestPoolUnregister(t *testing.T) {
	t.Parallel()

	const (
		handleNum = 1024
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var poolWg sync.WaitGroup
	pool := NewPoolImpl(1)
	poolWg.Add(1)
	go func() {
		defer poolWg.Done()
		require.Error(t, pool.Run(ctx))
	}()

	var (
		handles           [handleNum]*EventHandleImpl[int64]
		unregisteredFlags [handleNum]atomic.Bool
		lastValue         [handleNum]atomic.Int64
		wg                sync.WaitGroup
	)

	for i := 0; i < handleNum; i++ {
		i := i
		fn := func(n int64) error {
			require.Equal(t, n+1, lastValue[i].Add(1))
			require.False(t, unregisteredFlags[i].Load())
			return nil
		}
		handles[i] = NewEventHandlerImpl[int64](fn, 1024, 128)
		pool.RegisterHandle(handles[i])

		wg.Add(1)
		go func() {
			defer wg.Done()
			j := int64(0)
			for {
				if err := handles[i].AddEvent(ctx, j); err != nil {
					require.ErrorIs(t, err, ErrEventHandleCanceled)
					break
				}
				j++
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			handles[i].Unregister()
			unregisteredFlags[i].Store(true)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			handles[i].Unregister()
			unregisteredFlags[i].Store(true)
		}()
	}

	wg.Wait()
	cancel()
	poolWg.Wait()
}

func TestWorkerPoolError(t *testing.T) {
	t.Parallel()

	const handleNum = 32
	var (
		handles       [handleNum]*EventHandleImpl[string]
		errorCBCalled [handleNum]atomic.Bool
		wg            sync.WaitGroup
		poolWg        sync.WaitGroup
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewPoolImpl(1)
	poolWg.Add(1)
	go func() {
		defer poolWg.Done()
		require.Error(t, pool.Run(ctx))
	}()

	for i := 0; i < handleNum; i++ {
		i := i
		fn := func(input string) error {
			switch input {
			case "start":
				return nil
			case "error":
				return errors.New("fake")
			case "poison":
				require.FailNow(t, "should not be called")
			default:
			}
			panic("unreachable")
		}
		handles[i] = NewEventHandlerImpl[string](fn, 1024, 64)
		handles[i].OnExit(func(err error) {
			require.ErrorContains(t, err, "fake")
			require.False(t, errorCBCalled[i].Swap(true))
		})
		pool.RegisterHandle(handles[i])

		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()

			require.NoError(t, handles[i].AddEvent(ctx, "start"))
			require.NoError(t, handles[i].AddEvent(ctx, "error"))
			_ = handles[i].AddEvent(ctx, "poison")

			<-done
			require.True(t, errorCBCalled[i].Load())
			require.ErrorIs(t, ErrEventHandleCanceled, handles[i].AddEvent(ctx, "poison"))
			handles[i].Unregister() // should be no-op
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			require.ErrorContains(t, <-handles[i].ErrCh(), "fake")
		}()
	}

	wg.Wait()
	cancel()
}

func TestPoolGracefulUnregister(t *testing.T) {
	t.Parallel()

	const (
		handleNum = 1024
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var poolWg sync.WaitGroup
	pool := NewPoolImpl(1)
	poolWg.Add(1)
	go func() {
		defer poolWg.Done()
		require.Error(t, pool.Run(ctx))
	}()

	var (
		handles            [handleNum]*EventHandleImpl[int64]
		unregisteredFlags  [handleNum]atomic.Bool
		lastAddedValue     [handleNum]atomic.Int64
		lastProcessedValue [handleNum]atomic.Int64
		wg                 sync.WaitGroup
	)

	for i := 0; i < handleNum; i++ {
		i := i
		fn := func(n int64) error {
			lastProcessedValue[i].Swap(n)
			require.False(t, unregisteredFlags[i].Load())
			return nil
		}
		handles[i] = NewEventHandlerImpl[int64](fn, 1024, 128)
		pool.RegisterHandle(handles[i])

		wg.Add(1)
		go func() {
			defer wg.Done()
			j := int64(0)
			for {
				if err := handles[i].AddEvent(ctx, j); err != nil {
					require.ErrorIs(t, err, ErrEventHandleCanceled)
					break
				}
				lastAddedValue[i].Store(j)
				j++
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(500 * time.Millisecond)
			require.NoError(t, handles[i].GracefulUnregister(ctx))
			unregisteredFlags[i].Store(true)
			require.Equal(t, lastAddedValue[i].Load(), lastProcessedValue[i].Load())
		}()
	}

	wg.Wait()
	cancel()
	poolWg.Wait()
}
