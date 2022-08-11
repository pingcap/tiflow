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

package syncutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCond(t *testing.T) {
	t.Parallel()

	const (
		totalCount    = 4096
		consumerCount = 16
	)
	var (
		wg            sync.WaitGroup
		mu            sync.Mutex
		counter       int
		atomicCounter atomic.Int64
	)
	cond := NewCond(&mu)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Runs the producer
		for i := 0; i < totalCount; i++ {
			for {
				mu.Lock()
				c := counter
				mu.Unlock()
				if c < 10 {
					break
				}
				time.Sleep(1 * time.Millisecond)
			}
			mu.Lock()
			counter++
			cond.Broadcast()
			mu.Unlock()
		}
		log.Info("done")
	}()

	for n := 0; n < consumerCount; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomicCounter.Load() < totalCount {
				mu.Lock()
				for counter == 0 && atomicCounter.Load() < totalCount {
					cond.Wait()
				}
				counter--
				atomicCounter.Add(1)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
}

func TestCondWaitWithContext(t *testing.T) {
	t.Parallel()

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	cond := NewCond(&mu)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mu.Lock()
		defer mu.Unlock()
		err := cond.WaitWithContext(ctx)
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(100 * time.Millisecond)
		cond.Broadcast()
	}()

	wg.Wait()
}

func TestCondCancel(t *testing.T) {
	t.Parallel()

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	cond := NewCond(&mu)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mu.Lock() // must lock before WaitWithContext
		err := cond.WaitWithContext(ctx)
		require.Error(t, err)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	wg.Wait()
}
