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

package rpcutil

import (
	"testing"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCError(t *testing.T) {
	t.Parallel()

	// nil
	require.NoError(t, ToGRPCError(nil))

	// already a gRPC error
	err := status.New(codes.NotFound, "not found").Err()
	require.Equal(t, err, ToGRPCError(err))

	// unknown error
	err = errors.New("unknown error")
	gerr := ToGRPCError(err)
	require.Equal(t, codes.Unknown, status.Code(gerr))
	st, ok := status.FromError(gerr)
	require.True(t, ok)
	require.Equal(t, err.Error(), st.Message())
	require.Len(t, st.Details(), 1)
	errInfo := st.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, errors.ErrUnknown.RFCCode(), perrors.RFCErrorCode(errInfo.Reason))

	// job not found
	err = errors.ErrJobNotFound.GenWithStackByArgs("job-1")
	gerr = ToGRPCError(err)
	require.Equal(t, codes.NotFound, status.Code(gerr))
	st, ok = status.FromError(gerr)
	require.True(t, ok)
	require.Equal(t, "job job-1 is not found", st.Message())
	require.Len(t, st.Details(), 1)
	errInfo = st.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, errors.ErrJobNotFound.RFCCode(), perrors.RFCErrorCode(errInfo.Reason))

	// create worker terminated
	err = errors.ErrCreateWorkerTerminate.Wrap(perrors.New("invalid config")).GenWithStackByArgs()
	gerr = ToGRPCError(err)
	st, ok = status.FromError(gerr)
	require.True(t, ok)
	require.Equal(t, "create worker is terminated", st.Message())
	require.Len(t, st.Details(), 1)
	errInfo = st.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, errors.ErrCreateWorkerTerminate.RFCCode(), perrors.RFCErrorCode(errInfo.Reason))
	require.Equal(t, "invalid config", errInfo.Metadata[metadataCauseKey])
}

func TestFromGRPCError(t *testing.T) {
	t.Parallel()

	// nil
	require.NoError(t, FromGRPCError(nil))

	// not a gRPC error
	err := errors.New("unknown error")
	require.Equal(t, err, FromGRPCError(err))

	// gRPC error
	srvErr := errors.ErrJobNotFound.GenWithStackByArgs("job-1")
	clientErr := FromGRPCError(ToGRPCError(srvErr))
	require.True(t, errors.Is(clientErr, errors.ErrJobNotFound))
	require.Equal(t, srvErr.Error(), clientErr.Error())

	// create worker terminated
	srvErr = errors.ErrCreateWorkerTerminate.Wrap(perrors.New("invalid config")).GenWithStackByArgs()
	clientErr = FromGRPCError(ToGRPCError(srvErr))
	require.True(t, errors.Is(clientErr, errors.ErrCreateWorkerTerminate))
	require.Equal(t, srvErr.Error(), clientErr.Error())
	cause := errors.Cause(clientErr)
	require.Error(t, cause)
	require.Equal(t, "invalid config", cause.Error())
}
