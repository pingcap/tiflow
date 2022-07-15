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
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestWrapError(t *testing.T) {
	t.Parallel()
	var (
		err       = errors.New("cause error")
		testCases = []struct {
			rfcError *errors.Error
			err      error
			isNil    bool
			expected string
			args     []interface{}
		}{
			{ErrDecodeFailed, nil, true, "", nil},
			{
				ErrDecodeFailed, err, false,
				"[CDC:ErrDecodeFailed]decode failed: args data: cause error",
				[]interface{}{"args data"},
			},
			{
				ErrWriteTsConflict, err, false,
				"[CDC:ErrWriteTsConflict]write ts conflict: cause error", nil,
			},
			{ErrBuildJobFailed, nil, true, "", []interface{}{}},
			{
				ErrBuildJobFailed, err, false,
				"[DFLOW:ErrBuildJobFailed]build job failed: cause error",
				[]interface{}{},
			},
			{
				ErrSubJobFailed, err, false,
				"[DFLOW:ErrSubJobFailed]executor e-1 job 2: cause error",
				[]interface{}{"e-1", 2},
			},
		}
	)
	for _, tc := range testCases {
		we := WrapError(tc.rfcError, tc.err, tc.args...)
		if tc.isNil {
			require.Nil(t, we)
		} else {
			require.NotNil(t, we)
			require.Equal(t, we.Error(), tc.expected)
		}
	}
}

func TestRFCCode(t *testing.T) {
	t.Parallel()
	rfc, ok := RFCCode(ErrAPIInvalidParam)
	require.Equal(t, true, ok)
	require.Contains(t, rfc, "ErrAPIInvalidParam")

	err := fmt.Errorf("inner error: invalid request")
	rfc, ok = RFCCode(err)
	require.Equal(t, false, ok)
	require.Equal(t, rfc, errors.RFCErrorCode(""))

	rfcErr := ErrAPIInvalidParam
	Err := WrapError(rfcErr, err)
	rfc, ok = RFCCode(Err)
	require.Equal(t, true, ok)
	require.Contains(t, rfc, "ErrAPIInvalidParam")

	anoErr := errors.Annotate(ErrEtcdTryAgain, "annotated Etcd Try again")
	rfc, ok = RFCCode(anoErr)
	require.Equal(t, true, ok)
	require.Contains(t, rfc, "ErrEtcdTryAgain")
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

func TestChangefeedFastFailError(t *testing.T) {
	t.Parallel()
	err := ErrGCTTLExceeded.FastGenByArgs()
	rfcCode, _ := RFCCode(err)
	require.Equal(t, true, ChangefeedFastFailError(err))
	require.Equal(t, true, ChangefeedFastFailErrorCode(rfcCode))

	err = ErrGCTTLExceeded.GenWithStack("aa")
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, ChangefeedFastFailError(err))
	require.Equal(t, true, ChangefeedFastFailErrorCode(rfcCode))

	err = ErrGCTTLExceeded.Wrap(errors.New("aa"))
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, ChangefeedFastFailError(err))
	require.Equal(t, true, ChangefeedFastFailErrorCode(rfcCode))

	err = ErrSnapshotLostByGC.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, ChangefeedFastFailError(err))
	require.Equal(t, true, ChangefeedFastFailErrorCode(rfcCode))

	err = ErrStartTsBeforeGC.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, ChangefeedFastFailError(err))
	require.Equal(t, true, ChangefeedFastFailErrorCode(rfcCode))

	err = ErrToTLSConfigFailed.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, false, ChangefeedFastFailError(err))
	require.Equal(t, false, ChangefeedFastFailErrorCode(rfcCode))
}
