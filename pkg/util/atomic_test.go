// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMustCompareAndIncrease(t *testing.T) {
	t.Parallel()

	var target atomic.Int64
	target.Store(10)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	doIncrease := func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				delta := rand.Int63n(100)
				v := target.Load() + delta
				MustCompareAndMonotonicIncrease(&target, v)
				require.GreaterOrEqual(t, target.Load(), v)
			}
		}
	}

	// Test target increase.
	wg.Add(2)
	go func() {
		defer wg.Done()
		doIncrease()
	}()
	go func() {
		defer wg.Done()
		doIncrease()
	}()

	wg.Add(1)
	// Test target never decrease.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				v := target.Load() - 1
				MustCompareAndMonotonicIncrease(&target, v)
				require.Greater(t, target.Load(), v)
			}
		}
	}()

	cancel()
	wg.Wait()
}

func TestCompareAndIncrease(t *testing.T) {
	t.Parallel()

	var target atomic.Int64
	target.Store(10)
	require.True(t, CompareAndIncrease(&target, 10))
	require.Equal(t, int64(10), target.Load())

	require.True(t, CompareAndIncrease(&target, 20))
	require.Equal(t, int64(20), target.Load())
	require.False(t, CompareAndIncrease(&target, 19))
	require.Equal(t, int64(20), target.Load())
}

func TestCompareAndMonotonicIncrease(t *testing.T) {
	t.Parallel()

	var target atomic.Int64
	target.Store(10)
	require.False(t, CompareAndMonotonicIncrease(&target, 10))
	require.Equal(t, int64(10), target.Load())

	require.True(t, CompareAndMonotonicIncrease(&target, 11))
	require.Equal(t, int64(11), target.Load())
	require.False(t, CompareAndMonotonicIncrease(&target, 10))
	require.Equal(t, int64(11), target.Load())
}
