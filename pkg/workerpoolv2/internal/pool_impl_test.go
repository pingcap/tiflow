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

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestPoolBasics(t *testing.T) {
	t.Parallel()

	const (
		workerNum          = 8
		handlerNum         = 16
		producerNum        = 16
		eventPerHandler    = 1024 * 32
		handlerPerProducer = handlerNum / producerNum
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewPoolImpl(workerNum)

	var (
		handlers [handlerNum]*EventHandlerImpl[int]
		counters [handlerNum]atomic.Int64
		wg       sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		require.Error(t, pool.Run(ctx))
	}()

	for i := range handlers {
		i := i
		handlers[i] = NewEventHandlerImpl(func(n int) error {
			if n != 0 {
				if counters[i].Swap(int64(n)) != int64(n-1) {
					panic("unreachable")
				}
			} else {
				counters[i].Store(int64(n))
			}
			return nil
		}, 128, 64)
		pool.RegisterHandler(handlers[i])
	}

	for i := 0; i < producerNum; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < eventPerHandler; j++ {
				for k := handlerPerProducer * i; k < handlerPerProducer*(i+1); k++ {
					if err := handlers[k].AddEvent(ctx, j); err != nil {
						log.Panic("unexpected error", zap.Error(err))
					}
				}
			}
		}()
	}

	require.Eventually(t, func() bool {
		for _, counter := range counters {
			if counter.Load() < eventPerHandler-1 {
				return false
			}
		}
		return true
	}, 100*time.Second, 100*time.Millisecond)

	for _, handler := range handlers {
		handler.Unregister()
	}

	cancel()
	wg.Wait()
}
