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

	err := Do(ctx, f, WithBackoffBaseDelay(2), WithBackoffMaxDelay(10))
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

	err := Do(ctx, f, WithBackoffBaseDelay(2), WithBackoffMaxDelay(10))
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

	var i uint64
	for i = 0; i < 10; i++ {
		callCount = 0
		err = Do(context.Background(), f,
			WithBackoffBaseDelay(int64(i)), WithBackoffMaxDelay(int64(i)), WithMaxTries(i))
		require.Regexp(t, "test", errors.Cause(err))
		require.Regexp(t, ".*CDC:ErrReachMaxTry.*", err)
		if i == 0 {
			require.Equal(t, 1, callCount)
		} else {
			require.Equal(t, int(i), callCount)
		}
	}
}

func TestTotalRetryDuration(t *testing.T) {
	t.Parallel()

	f := func() error {
		return errors.New("test")
	}

	start := time.Now()
	err := Do(
		context.Background(), f,
		WithBackoffBaseDelay(math.MinInt64),
		WithTotalRetryDuration(time.Second),
	)
	require.Regexp(t, "test", errors.Cause(err))
	require.LessOrEqual(t, 1, int(math.Round(time.Since(start).Seconds())))

	start = time.Now()
	err = Do(
		context.Background(), f,
		WithBackoffBaseDelay(math.MinInt64),
		WithTotalRetryDuration(2*time.Second),
	)
	require.Regexp(t, "test", errors.Cause(err))
	require.LessOrEqual(t, 2, int(math.Round(time.Since(start).Seconds())))
}

func TestRetryError(t *testing.T) {
	t.Parallel()

	f := func() error {
		return errors.New("some error info")
	}

	err := Do(
		context.Background(), f, WithBackoffBaseDelay(math.MinInt64), WithMaxTries(2),
	)
	require.Regexp(t, "some error info", errors.Cause(err))
	require.Regexp(t, ".*some error info.*", err.Error())
	require.Regexp(t, ".*CDC:ErrReachMaxTry.*", err.Error())
}
