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

package model

import (
	"testing"
	"time"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsChangefeedNotRetryError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err    RunningError
		result bool
	}{
		{
			RunningError{
				Addr:    "",
				Code:    string(cerror.ErrAPIGetPDClientFailed.RFCCode()),
				Message: cerror.ErrAPIGetPDClientFailed.Error(),
			},
			false,
		},
		{
			RunningError{
				Addr:    "",
				Code:    string(cerror.ErrExpressionColumnNotFound.RFCCode()),
				Message: cerror.ErrExpressionColumnNotFound.Error(),
			},
			true,
		},
		{
			RunningError{
				Addr:    "",
				Code:    string(cerror.ErrExpressionParseFailed.RFCCode()),
				Message: cerror.ErrExpressionParseFailed.Error(),
			},
			true,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.result, c.err.ShouldFailChangefeed())
	}
}

func TestRunningErrorScan(t *testing.T) {
	t.Parallel()

	timeNow := time.Now()
	timeNowJSON, err := timeNow.MarshalJSON()
	require.Nil(t, err)

	newTime := time.Time{}
	err = newTime.UnmarshalJSON(timeNowJSON)
	require.Nil(t, err)
	// timeNow:  2023-10-13 16:48:08.345614 +0800 CST m=+0.027639459
	// newTime:  2023-10-13 16:48:08.345614 +0800 CST
	require.NotEqual(t, timeNow, newTime)

	cases := []struct {
		err    RunningError
		result string
	}{
		{
			RunningError{
				Time:    timeNow,
				Addr:    "",
				Code:    string(cerror.ErrAPIGetPDClientFailed.RFCCode()),
				Message: cerror.ErrAPIGetPDClientFailed.Error(),
			},
			`{"time":` + string(timeNowJSON) +
				`,"addr":"","code":"CDC:ErrAPIGetPDClientFailed","message":"` +
				cerror.ErrAPIGetPDClientFailed.Error() + `"}`,
		},
	}

	for _, c := range cases {
		v, err := c.err.Value()
		b, ok := v.([]byte)
		require.True(t, ok)
		require.Nil(t, err)
		require.Equal(t, c.result, string(b))

		var err2 RunningError
		err = err2.Scan(b)
		require.Nil(t, err)
		require.Equal(t, c.err.Addr, err2.Addr)
		require.Equal(t, c.err.Code, err2.Code)
		require.Equal(t, c.err.Message, err2.Message)
	}
}
