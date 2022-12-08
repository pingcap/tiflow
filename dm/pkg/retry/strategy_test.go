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

package retry

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/dbutil"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestFiniteRetryStrategy(t *testing.T) {
	t.Parallel()
	strategy := &FiniteRetryStrategy{}

	params := Params{
		RetryCount:         1,
		BackoffStrategy:    Stable,
		FirstRetryDuration: time.Second,
		IsRetryableFn: func(int, error) bool {
			return false
		},
	}
	ctx := tcontext.Background()

	operateFn := func(*tcontext.Context) (interface{}, error) {
		return nil, terror.ErrDBDriverError.Generate("test database error")
	}

	_, opCount, err := strategy.Apply(ctx, params, operateFn)
	require.Equal(t, 0, opCount)
	require.True(t, terror.ErrDBDriverError.Equal(err))

	params.IsRetryableFn = func(int, error) bool {
		return true
	}
	_, opCount, err = strategy.Apply(ctx, params, operateFn)
	require.Equal(t, params.RetryCount, opCount)
	require.True(t, terror.ErrDBDriverError.Equal(err))

	params.RetryCount = 3

	_, opCount, err = strategy.Apply(ctx, params, operateFn)
	require.Equal(t, params.RetryCount, opCount)
	require.True(t, terror.ErrDBDriverError.Equal(err))

	// invalid connection will return ErrInvalidConn immediately no matter how many retries left
	params.IsRetryableFn = func(int, error) bool {
		return dbutil.IsRetryableError(err)
	}
	operateFn = func(*tcontext.Context) (interface{}, error) {
		mysqlErr := driver.ErrBadConn
		return nil, terror.ErrDBInvalidConn.Delegate(mysqlErr, "test invalid connection")
	}
	_, opCount, err = strategy.Apply(ctx, params, operateFn)
	require.Equal(t, 0, opCount)
	require.True(t, terror.ErrDBInvalidConn.Equal(err))

	params.IsRetryableFn = func(int, error) bool {
		return IsConnectionError(err)
	}
	_, opCount, err = strategy.Apply(ctx, params, operateFn)
	require.Equal(t, 3, opCount)
	require.True(t, terror.ErrDBInvalidConn.Equal(err))

	retValue := "success"
	operateFn = func(*tcontext.Context) (interface{}, error) {
		return retValue, nil
	}
	ret, opCount, err := strategy.Apply(ctx, params, operateFn)
	require.Equal(t, retValue, ret.(string))
	require.Equal(t, 0, opCount)
	require.NoError(t, err)
}
