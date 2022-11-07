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
	"net/http"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
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
	require.Equal(t, true, IsChangefeedFastFailError(err))
	require.Equal(t, true, IsChangefeedFastFailErrorCode(rfcCode))

	err = ErrGCTTLExceeded.GenWithStack("aa")
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, IsChangefeedFastFailError(err))
	require.Equal(t, true, IsChangefeedFastFailErrorCode(rfcCode))

	err = ErrGCTTLExceeded.Wrap(errors.New("aa"))
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, IsChangefeedFastFailError(err))
	require.Equal(t, true, IsChangefeedFastFailErrorCode(rfcCode))

	err = ErrSnapshotLostByGC.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, IsChangefeedFastFailError(err))
	require.Equal(t, true, IsChangefeedFastFailErrorCode(rfcCode))

	err = ErrStartTsBeforeGC.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, IsChangefeedFastFailError(err))
	require.Equal(t, true, IsChangefeedFastFailErrorCode(rfcCode))

	err = ErrToTLSConfigFailed.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, false, IsChangefeedFastFailError(err))
	require.Equal(t, false, IsChangefeedFastFailErrorCode(rfcCode))
}

func TestIsChangefeedUnRetryableError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err      error
		expected bool
	}{
		{
			err:      ErrInvalidIgnoreEventType.FastGenByArgs(),
			expected: false,
		},
		{
			err:      ErrExpressionColumnNotFound.FastGenByArgs(),
			expected: true,
		},
		{
			err:      ErrExpressionParseFailed.FastGenByArgs(),
			expected: true,
		},
		{
			err:      WrapError(ErrFilterRuleInvalid, ErrExpressionColumnNotFound.FastGenByArgs()),
			expected: true,
		},
		{
			err:      errors.New("CDC:ErrExpressionColumnNotFound"),
			expected: true,
		},
		{
			err:      ErrSyncRenameTableFailed.FastGenByArgs(),
			expected: true,
		},
		{
			err:      WrapChangefeedUnretryableErr(errors.New("whatever")),
			expected: true,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.expected, IsChangefeedUnRetryableError(c.err))
	}
}

func TestIsCliUnprintableError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"context Canceled err", context.Canceled, false},
		{"context DeadlineExceeded err", context.DeadlineExceeded, false},
		{"normal err", errors.New("test"), false},
		{"cdc reachMaxTry err", ErrReachMaxTry, false},
		{"cli unprint err", ErrCliAborted, true},
	}
	for _, tt := range tests {
		ret := IsCliUnprintableError(tt.err)
		require.Equal(t, ret, tt.want, "case:%s", tt.name)
	}
}

func TestHTTPStatusCode(t *testing.T) {
	require.Equal(t, http.StatusOK, HTTPStatusCode(nil))
	require.Equal(t, 499, HTTPStatusCode(context.Canceled))
	require.Equal(t, http.StatusGatewayTimeout, HTTPStatusCode(context.DeadlineExceeded))
	require.Equal(t, http.StatusInternalServerError, HTTPStatusCode(errors.New("unknown error")))
	for rfcCode, httpCode := range httpStatusCodeMapping {
		err := errors.Normalize(string(rfcCode), errors.RFCCodeText(string(rfcCode)))
		require.Equal(t, httpCode, HTTPStatusCode(err))
	}
}

func TestGRPCStatusCode(t *testing.T) {
	require.Equal(t, codes.OK, GRPCStatusCode(nil))
	require.Equal(t, codes.Canceled, GRPCStatusCode(context.Canceled))
	require.Equal(t, codes.DeadlineExceeded, GRPCStatusCode(context.DeadlineExceeded))
	require.Equal(t, codes.Unknown, GRPCStatusCode(errors.New("unknown error")))
	require.Equal(t, codes.Internal, GRPCStatusCode(errors.Normalize("internal error", errors.RFCCodeText("TEST:ErrInternal"))))
	for rfcCode, gRPCCode := range gRPCStatusCodeMapping {
		err := errors.Normalize(string(rfcCode), errors.RFCCodeText(string(rfcCode)))
		require.Equal(t, gRPCCode, GRPCStatusCode(err))
	}
}
