// Copyright 2020 PingCAP, Inc.
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

package errors

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestWrapError(t *testing.T) {
	t.Parallel()
	var (
		rfcError  = ErrDecodeFailed
		err       = errors.New("test")
		testCases = []struct {
			err      error
			isNil    bool
			expected string
		}{
			{nil, true, ""},
			{err, false, "[CDC:ErrDecodeFailed]test: test"},
		}
	)
	for _, tc := range testCases {
		we := WrapError(rfcError, tc.err)
		if tc.isNil {
			require.Nil(t, we)
		} else {
			require.NotNil(t, we)
			require.Equal(t, we.Error(), tc.expected)
		}
	}
}

func TestIsRetryableError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"context Canceled err", context.Canceled, false},
		{"context DeadlineExceeded err", context.DeadlineExceeded, false},
		{"normal err", errors.New("test"), true},
		{"cdc reachMaxTry err", ErrReachMaxTry, true},
	}
	for _, tt := range tests {
		ret := IsRetryableError(tt.err)
		require.Equal(t, ret, tt.want, "case:%s", tt.name)
	}
}

func TestIsBadRequestError(t *testing.T) {
	err := ErrAPIInvalidParam.GenWithStack("aa")
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = ErrPDEtcdAPIError.GenWithStack("aa")
	require.Equal(t, false, IsHTTPBadRequestError(err))
	err = nil
	require.Equal(t, false, IsHTTPBadRequestError(err))
}
