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

package rpcutil_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCError(t *testing.T) {
	t.Parallel()

	// nil
	require.NoError(t, rpcutil.ToGRPCError(nil))

	// already a gRPC error
	err := status.New(codes.NotFound, "not found").Err()
	require.Equal(t, err, rpcutil.ToGRPCError(err))

	// unknown error
	err = errors.New("unknown error")
	gerr := rpcutil.ToGRPCError(err)
	require.Equal(t, codes.Unknown, status.Code(gerr))
	st, ok := status.FromError(gerr)
	require.True(t, ok)
	require.Equal(t, err.Error(), st.Message())
	require.Len(t, st.Details(), 1)
	errInfo := st.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, errors.ErrUnknown.RFCCode(), perrors.RFCErrorCode(errInfo.Reason))

	// job not found
	err = errors.ErrJobNotFound.GenWithStackByArgs("job-1")
	gerr = rpcutil.ToGRPCError(err)
	require.Equal(t, codes.NotFound, status.Code(gerr))
	st, ok = status.FromError(gerr)
	require.True(t, ok)
	require.Equal(t, "job job-1 is not found", st.Message())
	require.Len(t, st.Details(), 1)
	errInfo = st.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, errors.ErrJobNotFound.RFCCode(), perrors.RFCErrorCode(errInfo.Reason))

	// create worker terminated
	err = errors.ErrCreateWorkerTerminate.Wrap(perrors.New("invalid config")).GenWithStackByArgs()
	gerr = rpcutil.ToGRPCError(err)
	st, ok = status.FromError(gerr)
	require.True(t, ok)
	require.Equal(t, "create worker is terminated", st.Message())
	require.Len(t, st.Details(), 1)
	errInfo = st.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, errors.ErrCreateWorkerTerminate.RFCCode(), perrors.RFCErrorCode(errInfo.Reason))
	require.Equal(t, "invalid config", errInfo.Metadata["cause"])
}

func TestFromGRPCError(t *testing.T) {
	t.Parallel()

	// nil
	require.NoError(t, rpcutil.FromGRPCError(nil))

	// not a gRPC error
	err := errors.New("unknown error")
	require.Equal(t, err, rpcutil.FromGRPCError(err))

	// gRPC error
	srvErr := errors.ErrJobNotFound.GenWithStackByArgs("job-1")
	clientErr := rpcutil.FromGRPCError(rpcutil.ToGRPCError(srvErr))
	require.True(t, errors.Is(clientErr, errors.ErrJobNotFound))
	require.Equal(t, srvErr.Error(), clientErr.Error())

	// create worker terminated
	srvErr = errors.ErrCreateWorkerTerminate.Wrap(perrors.New("invalid config")).GenWithStackByArgs()
	clientErr = rpcutil.FromGRPCError(rpcutil.ToGRPCError(srvErr))
	require.True(t, errors.Is(clientErr, errors.ErrCreateWorkerTerminate))
	require.Equal(t, srvErr.Error(), clientErr.Error())
	cause := errors.Cause(clientErr)
	require.Error(t, cause)
	require.Equal(t, "invalid config", cause.Error())
}

type leaderClient struct {
	heartbeat func(ctx context.Context, req any) (any, error)
	status    func(ctx context.Context, req any) (any, error)
}

func (lc *leaderClient) Heartbeat(ctx context.Context, req any) (any, error) {
	return lc.heartbeat(ctx, req)
}

func (lc *leaderClient) Status(ctx context.Context, req any) (any, error) {
	return lc.status(ctx, req)
}

func TestForwardToLeader(t *testing.T) {
	t.Parallel()

	fc := mock.NewMockForwardChecker[*leaderClient](gomock.NewController(t))
	mw := rpcutil.ForwardToLeader[*leaderClient](fc)

	var (
		local   atomic.Bool
		forward atomic.Bool
	)
	fc.EXPECT().LeaderOnly("Heartbeat").AnyTimes().Return(true)
	fc.EXPECT().LeaderOnly("Status").AnyTimes().Return(false)

	handler := func(ctx context.Context, req any) (any, error) {
		local.Store(true)
		return nil, nil
	}

	// Current node is leader.
	fc.EXPECT().IsLeader().Times(1).Return(true)
	_, err := mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "Heartbeat"}, handler)
	require.NoError(t, err)
	require.True(t, local.Load())
	require.False(t, forward.Load())

	// Method is not leader only.
	fc.EXPECT().IsLeader().Times(1).Return(false)
	local.Store(false)
	forward.Store(false)
	_, err = mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "Status"}, handler)
	require.NoError(t, err)
	require.True(t, local.Load())
	require.False(t, forward.Load())

	// Forward to leader.
	lc := &leaderClient{
		heartbeat: func(ctx context.Context, req any) (any, error) {
			forward.Store(true)
			return nil, nil
		},
		status: func(ctx context.Context, req any) (any, error) {
			forward.Store(true)
			return nil, nil
		},
	}
	fc.EXPECT().IsLeader().Times(1).Return(false)
	fc.EXPECT().LeaderClient().Times(1).Return(lc, nil)
	local.Store(false)
	forward.Store(false)
	_, err = mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "Heartbeat"}, handler)
	require.NoError(t, err)
	require.False(t, local.Load())
	require.True(t, forward.Load())

	// Wait for leader.
	const leaderDelay = time.Millisecond * 500
	start := time.Now()
	fc.EXPECT().IsLeader().Times(1).Return(false)
	fc.EXPECT().LeaderClient().AnyTimes().DoAndReturn(func() (*leaderClient, error) {
		if time.Since(start) < leaderDelay {
			return nil, errors.ErrMasterNoLeader.GenWithStackByArgs()
		}
		return lc, nil
	})
	local.Store(false)
	forward.Store(false)
	_, err = mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "Heartbeat"}, handler)
	require.NoError(t, err)
	require.False(t, local.Load())
	require.True(t, forward.Load())
}

func TestCheckAvailable(t *testing.T) {
	t.Parallel()

	fc := mock.NewMockFeatureChecker(gomock.NewController(t))
	mw := rpcutil.CheckAvailable(fc)

	var handled atomic.Bool
	handler := func(ctx context.Context, req any) (any, error) {
		handled.Store(true)
		return nil, nil
	}

	fc.EXPECT().Available("Heartbeat").Times(1).Return(false)
	_, err := mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "Heartbeat"}, handler)
	err = rpcutil.FromGRPCError(err)
	require.True(t, errors.Is(err, errors.ErrMasterNotReady))
	require.False(t, handled.Load())

	fc.EXPECT().Available("Heartbeat").Times(1).Return(true)
	_, err = mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "Heartbeat"}, handler)
	require.NoError(t, err)
	require.True(t, handled.Load())
}

func TestNormalizeError(t *testing.T) {
	t.Parallel()

	mw := rpcutil.NormalizeError()
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, errors.ErrJobNotFound.GenWithStackByArgs("job-1")
	}
	_, err := mw(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "GetJob"}, handler)
	gerr, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, gerr.Code())
	clientErr := rpcutil.FromGRPCError(err)
	require.True(t, errors.Is(clientErr, errors.ErrJobNotFound))
}
