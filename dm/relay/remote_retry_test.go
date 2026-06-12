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

package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	rr, err := NewReaderRetry(ReaderRetryConfig{
		BackoffRollback: 200 * time.Millisecond,
		BackoffMax:      1 * time.Second,
		BackoffMin:      1 * time.Millisecond,
		BackoffFactor:   2,
	})
	require.NoError(t, err)
	require.NotNil(t, rr)

	retryableErr := gmysql.ErrBadConn
	unRetryableErr := errors.New("custom error")
	ctx := context.Background()

	// check some times
	for i := 0; i < 3; i++ {
		require.True(t, rr.Check(ctx, retryableErr))
	}
	require.Equal(t, 8*time.Millisecond, rr.bf.Current())

	// check more times, until reach Max
	for i := 0; i < 10; i++ {
		require.True(t, rr.Check(ctx, retryableErr))
	}
	require.Equal(t, rr.cfg.BackoffMax, rr.bf.Current())

	// sleep 1s
	time.Sleep(1 * time.Second)

	// check with rollback, rollback 5 times, forward 1 time
	require.True(t, rr.Check(ctx, retryableErr))
	require.Equal(t, 64*time.Millisecond, rr.bf.Current())

	// check un-retryable error
	require.False(t, rr.Check(ctx, unRetryableErr))

	// check with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	require.False(t, rr.Check(ctx, retryableErr))
}
