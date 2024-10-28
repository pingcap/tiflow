// Copyright 2019 PingCAP, Inc.
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

package backoff

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestNewBackoff(t *testing.T) {
	t.Parallel()
	var (
		backoffFactor float64 = 2
		backoffMin            = 1 * time.Second
		backoffMax            = 5 * time.Minute
		backoffJitter         = true
		bf            *Backoff
		err           error
	)
	testCases := []struct {
		factor float64
		jitter bool
		min    time.Duration
		max    time.Duration
		hasErr bool
	}{
		{backoffFactor, backoffJitter, backoffMin, backoffMax, false},
		{0, backoffJitter, backoffMin, backoffMax, true},
		{-1, backoffJitter, backoffMin, backoffMax, true},
		{backoffFactor, backoffJitter, -1, backoffMax, true},
		{backoffFactor, backoffJitter, backoffMin, -1, true},
		{backoffFactor, backoffJitter, backoffMin, backoffMin - 1, true},
	}
	for _, tc := range testCases {
		bf, err = NewBackoff(tc.factor, tc.jitter, tc.min, tc.max)
		if tc.hasErr {
			require.Nil(t, bf)
			require.True(t, terror.ErrBackoffArgsNotValid.Equal(err))
		} else {
			require.NoError(t, err)
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()
	var (
		minT           = 1 * time.Millisecond
		maxT           = 1 * time.Second
		factor float64 = 2
	)
	b := &Backoff{
		Min:    minT,
		Max:    maxT,
		Factor: factor,
	}

	for i := 0; i < 10; i++ {
		expected := minT * time.Duration(math.Pow(factor, float64(i)))
		require.Equal(t, expected, b.Duration())
	}
	b.Rollback()
	require.Equal(t, 512*minT, b.Current())
	b.Forward()
	for i := 0; i < 10; i++ {
		require.Equal(t, maxT, b.Duration())
	}
	b.Reset()
	require.Equal(t, minT, b.Duration())
}

func checkBetween(t *testing.T, value, low, high time.Duration) {
	t.Helper()
	require.True(t, value > low)
	require.True(t, value < high)
}

func TestBackoffJitter(t *testing.T) {
	t.Parallel()
	var (
		minT           = 1 * time.Millisecond
		maxT           = 1 * time.Second
		factor float64 = 2
	)
	b := &Backoff{
		Min:    minT,
		Max:    maxT,
		Factor: factor,
		Jitter: true,
	}
	require.Equal(t, minT, b.Duration())
	checkBetween(t, b.Duration(), minT, 2*minT)
	checkBetween(t, b.Duration(), 2*minT, 4*minT)
	checkBetween(t, b.Duration(), 4*minT, 8*minT)
	b.Reset()
	require.Equal(t, minT, b.Duration())
}

func TestFixedBackoff(t *testing.T) {
	t.Parallel()
	var (
		minT           = 100 * time.Millisecond
		maxT           = 100 * time.Millisecond
		factor float64 = 2
	)
	b := &Backoff{
		Min:    minT,
		Max:    maxT,
		Factor: factor,
	}
	for i := 0; i < 10; i++ {
		require.Equal(t, maxT, b.Duration())
	}
}

func TestOverflowBackoff(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		min    time.Duration
		max    time.Duration
		factor float64
	}{
		{time.Duration(math.MaxInt64/2 + math.MaxInt64/4 + 2), time.Duration(math.MaxInt64), 2},
		{time.Duration(math.MaxInt64/2 + 1), time.Duration(math.MaxInt64), 2},
		{time.Duration(math.MaxInt64), time.Duration(math.MaxInt64), 2},
	}
	for _, tc := range testCases {
		b := &Backoff{
			Min:    tc.min,
			Max:    tc.max,
			Factor: tc.factor,
		}
		require.Equal(t, tc.min, b.Duration())
		require.Equal(t, tc.max, b.Duration())
	}
}

func TestForward(t *testing.T) {
	t.Parallel()
	var (
		factor float64 = 2
		minT           = 1 * time.Second
		maxT           = 5 * time.Second
		n              = 10
	)
	b := &Backoff{
		Min:    minT,
		Max:    maxT,
		Factor: factor,
	}
	for i := 0; i < n; i++ {
		b.Forward()
	}
	require.Equal(t, n, b.cwnd)
	b.Reset()
	require.Equal(t, 0, b.cwnd)
	for i := 0; i < n; i++ {
		b.BoundaryForward()
	}
	require.Equal(t, 3, b.cwnd)
}
