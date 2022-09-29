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

package jobop

import (
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/stretchr/testify/require"
)

func TestJobBackoff(t *testing.T) {
	t.Parallel()

	clocker := clock.NewMock()
	resetInterval := 20 * time.Second
	config := &BackoffConfig{
		ResetInterval:   resetInterval,
		InitialInterval: time.Second,
		MaxInterval:     time.Second * 10,
		Multiplier:      2.0,
	}
	bkf := NewJobBackoff("test-job-id", clocker, config)

	// allow is true on fresh backoff
	require.True(t, bkf.Allow())

	// Not allow before next interval
	bkf.Fail()
	require.False(t, bkf.Allow())
	// Allowed after time passes next interval
	clocker.Add(time.Second * 2)
	require.True(t, bkf.Allow())

	// simulate consecutive failures
	for i := 0; i < 10; i++ {
		bkf.Fail()
	}
	require.False(t, bkf.Allow())
	// interval less than maxInterval * (1+randomFactor) = 10 * 1.5
	clocker.Add(time.Second * 15)
	require.True(t, bkf.Allow())

	// if success during is longer than resetInterval, the backkoff will be reset
	bkf.Success()
	clocker.Add(resetInterval)
	bkf.Fail()
	require.False(t, bkf.Allow())
	clocker.Add(time.Second * 2)
	require.True(t, bkf.Allow())
}

func TestJobBackoffMaxTry(t *testing.T) {
	t.Parallel()

	var (
		clocker       = clock.NewMock()
		resetInterval = 20 * time.Second
		maxTryTime    = 5
	)
	config := &BackoffConfig{
		ResetInterval: resetInterval,
		MaxTryTime:    maxTryTime,
	}
	bkf := NewJobBackoff("test-job-id", clocker, config)

	for i := 0; i < maxTryTime-1; i++ {
		bkf.Fail()
		require.False(t, bkf.Terminate())
	}
	bkf.Success()
	clocker.Add(resetInterval)

	for i := 0; i < maxTryTime-1; i++ {
		bkf.Fail()
		require.False(t, bkf.Terminate())
	}
	bkf.Fail()
	require.True(t, bkf.Terminate())
}
