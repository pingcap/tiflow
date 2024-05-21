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
	err := ErrSnapshotLostByGC.FastGenByArgs()
	rfcCode, _ := RFCCode(err)
	require.Equal(t, true, IsChangefeedGCFastFailError(err))
	require.Equal(t, true, IsChangefeedGCFastFailErrorCode(rfcCode))

	err = ErrStartTsBeforeGC.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, true, IsChangefeedGCFastFailError(err))
	require.Equal(t, true, IsChangefeedGCFastFailErrorCode(rfcCode))

	err = ErrToTLSConfigFailed.FastGenByArgs()
	rfcCode, _ = RFCCode(err)
	require.Equal(t, false, IsChangefeedGCFastFailError(err))
	require.Equal(t, false, IsChangefeedGCFastFailErrorCode(rfcCode))
}

func TestShouldFailChangefeed(t *testing.T) {
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
		{
			err:      WrapError(ErrSinkURIInvalid, errors.New("test")),
			expected: true,
		},
		{
			err:      WrapError(ErrKafkaInvalidConfig, errors.New("test")),
			expected: true,
		},
		{
			err:      WrapError(ErrMySQLInvalidConfig, errors.New("test")),
			expected: true,
		},
		{
			err:      WrapError(ErrStorageSinkInvalidConfig, errors.New("test")),
			expected: true,
		},
		{
			err:      errors.Trace(WrapError(ErrStorageSinkInvalidConfig, errors.New("test"))),
			expected: true,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.expected, ShouldFailChangefeed(c.err))
	}

	var code errors.RFCErrorCode
	var ok bool
	code, ok = RFCCode(ErrChangefeedUnretryable)
	require.True(t, ok)
	require.True(t, ShouldFailChangefeed(errors.New(string(code))))
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

func TestOriginError(t *testing.T) {
	require.NoError(t, OriginError(nil))

	err1 := errors.New("err1")
	require.Equal(t, err1, OriginError(err1))

	err2 := errors.Trace(err1)
	require.Equal(t, err1, OriginError(err2))

	err3 := errors.Trace(err2)
	require.Equal(t, err1, OriginError(err3))
}
