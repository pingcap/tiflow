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

package retry

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestShouldRetryAtMostSpecifiedTimes(t *testing.T) {
	t.Parallel()
	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Run(500*time.Millisecond, 3, f)
	require.Regexp(t, "test", err)
	// 👇 i think tries = first call + maxRetries, so not weird 😎

	// It's weird that backoff may retry one more time than maxTries.
	// Because the steps in backoff.Retry is:
	// 1. Call function
	// 2. Compare numTries and maxTries
	// 3. Increment numTries
	require.Equal(t, callCount, 3+1)
}

func TestShouldStopOnSuccess(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		if callCount == 2 {
			return nil
		}
		return errors.New("test")
	}

	err := Run(500*time.Millisecond, 3, f)
	require.Nil(t, err)
	require.Equal(t, callCount, 2)
}

func TestShouldBeCtxAware(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		return context.Canceled
	}

	err := Run(500*time.Millisecond, 3, f)
	require.Equal(t, err, context.Canceled)
	require.Equal(t, callCount, 1)

	callCount = 0
	f = func() error {
		callCount++
		return errors.Annotate(context.Canceled, "test")
	}
	err = Run(500*time.Millisecond, 3, f)
	require.Equal(t, errors.Cause(err), context.Canceled)
	require.Equal(t, callCount, 1)
}

func TestInfiniteRetry(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		return context.Canceled
	}

	var reportedElapsed time.Duration
	notify := func(elapsed time.Duration) {
		reportedElapsed = elapsed
	}

	err := RunWithInfiniteRetry(10*time.Millisecond, f, notify)
	require.Equal(t, err, context.Canceled)
	require.Equal(t, callCount, 1)
	require.Equal(t, reportedElapsed, 0*time.Second)

	callCount = 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	f = func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		callCount++
		return errors.New("test")
	}

	err = RunWithInfiniteRetry(10*time.Millisecond, f, notify)
	require.Equal(t, err, context.DeadlineExceeded)
	require.Greater(t, reportedElapsed, time.Second)
	require.LessOrEqual(t, reportedElapsed, 3*time.Second)
}

func TestDoShouldRetryAtMostSpecifiedTimes(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Do(context.Background(), f, WithMaxTries(3))
	require.Regexp(t, "test", errors.Cause(err))
	require.Equal(t, callCount, 3)
}

func TestDoShouldStopOnSuccess(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		if callCount == 2 {
			return nil
		}
		return errors.New("test")
	}

	err := Do(context.Background(), f, WithMaxTries(3))
	require.Nil(t, err)
	require.Equal(t, callCount, 2)
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		return errors.Annotate(context.Canceled, "test")
	}

	err := Do(context.Background(), f, WithMaxTries(3), WithIsRetryableErr(func(err error) bool {
		switch errors.Cause(err) {
		case context.Canceled:
			return false
		}
		return true
	}))

	require.Equal(t, errors.Cause(err), context.Canceled)
	require.Equal(t, callCount, 1)

	callCount = 0
	err = Do(context.Background(), f, WithMaxTries(3))

	require.Equal(t, errors.Cause(err), context.Canceled)
	require.Equal(t, callCount, 3)
}

func TestDoCancelInfiniteRetry(t *testing.T) {
	t.Parallel()

	callCount := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()
	f := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		callCount++
		return errors.New("test")
	}

	err := Do(ctx, f, WithInfiniteTries(), WithBackoffBaseDelay(2), WithBackoffMaxDelay(10))
	require.Equal(t, errors.Cause(err), context.DeadlineExceeded)
	require.GreaterOrEqual(t, callCount, 1, "tries: %d", callCount)
	require.Less(t, callCount, math.MaxInt64)
}

func TestDoCancelAtBeginning(t *testing.T) {
	t.Parallel()

	callCount := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Do(ctx, f, WithInfiniteTries(), WithBackoffBaseDelay(2), WithBackoffMaxDelay(10))
	require.Equal(t, errors.Cause(err), context.Canceled)
	require.Equal(t, callCount, 0, "tries:%d", callCount)
}

func TestDoCornerCases(t *testing.T) {
	t.Parallel()

	var callCount int
	f := func() error {
		callCount++
		return errors.New("test")
	}

	err := Do(context.Background(), f, WithBackoffBaseDelay(math.MinInt64), WithBackoffMaxDelay(math.MaxInt64), WithMaxTries(2))
	require.Regexp(t, "test", errors.Cause(err))
	require.Equal(t, callCount, 2)

	callCount = 0
	err = Do(context.Background(), f, WithBackoffBaseDelay(math.MaxInt64), WithBackoffMaxDelay(math.MinInt64), WithMaxTries(2))
	require.Regexp(t, "test", errors.Cause(err))
	require.Equal(t, callCount, 2)

	callCount = 0
	err = Do(context.Background(), f, WithBackoffBaseDelay(math.MinInt64), WithBackoffMaxDelay(math.MinInt64), WithMaxTries(2))
	require.Regexp(t, "test", errors.Cause(err))
	require.Equal(t, callCount, 2)

	callCount = 0
	err = Do(context.Background(), f, WithBackoffBaseDelay(math.MaxInt64), WithBackoffMaxDelay(math.MaxInt64), WithMaxTries(2))
	require.Regexp(t, "test", errors.Cause(err))
	require.Equal(t, callCount, 2)

	var i int64
	for i = -10; i < 10; i++ {
		callCount = 0
		err = Do(context.Background(), f, WithBackoffBaseDelay(i), WithBackoffMaxDelay(i), WithMaxTries(i))
		require.Regexp(t, "test", errors.Cause(err))
		require.Regexp(t, ".*CDC:ErrReachMaxTry.*", err)
		if i > 0 {
			require.Equal(t, int64(callCount), i)
		} else {
			require.Equal(t, callCount, defaultMaxTries)
		}
	}
}
