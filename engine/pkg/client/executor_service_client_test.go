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

package client

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/enginepb"
	pbMock "github.com/pingcap/tiflow/engine/enginepb/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDispatchTaskNormal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := pbMock.NewMockExecutorServiceClient(ctrl)
	serviceCli := NewExecutorServiceClient(client)

	var (
		requestID           string
		preDispatchComplete atomic.Bool
		cbCalled            atomic.Bool
	)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	client.EXPECT().PreDispatchTask(gomock.Any(), matchPreDispatchArgs(args)).
		Do(func(_ context.Context, arg1 *enginepb.PreDispatchTaskRequest, _ ...grpc.CallOption) {
			requestID = arg1.RequestId
			preDispatchComplete.Store(true)
		}).Return(&enginepb.PreDispatchTaskResponse{}, nil).Times(1)

	client.EXPECT().ConfirmDispatchTask(gomock.Any(), matchConfirmDispatch(&requestID, "worker-1")).
		Return(&enginepb.ConfirmDispatchTaskResponse{}, nil).Do(
		func(_ context.Context, _ *enginepb.ConfirmDispatchTaskRequest, _ ...grpc.CallOption) {
			require.True(t, preDispatchComplete.Load())
			require.True(t, cbCalled.Load())
		}).Times(1)

	err := serviceCli.DispatchTask(context.Background(), args, func() {
		require.True(t, preDispatchComplete.Load())
		require.False(t, cbCalled.Swap(true))
	})
	require.NoError(t, err)
}

func TestPreDispatchAborted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := pbMock.NewMockExecutorServiceClient(ctrl)
	serviceCli := NewExecutorServiceClient(client)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	unknownRPCError := status.Error(codes.Unknown, "fake error")
	client.EXPECT().PreDispatchTask(gomock.Any(), matchPreDispatchArgs(args)).
		Return((*enginepb.PreDispatchTaskResponse)(nil), unknownRPCError).Times(1)

	err := serviceCli.DispatchTask(context.Background(), args, func() {
		t.Fatalf("unexpected callback")
	})
	require.Error(t, err)
	require.Regexp(t, "fake error", err)
}

func TestConfirmDispatchErrorFailFast(t *testing.T) {
	t.Parallel()

	// Only those errors that indicates a server-side failure can
	// make DispatchTask fail fast. Otherwise, no error should be
	// reported and at least a timeout should be waited for.
	testCases := []struct {
		err        error
		isFailFast bool
	}{
		{
			err:        errors.ErrRuntimeIncomingQueueFull.FastGenByArgs(),
			isFailFast: true,
		},
		{
			err:        errors.ErrDispatchTaskRequestIDNotFound.FastGenByArgs(),
			isFailFast: true,
		},
		{
			err:        errors.ErrInvalidArgument.FastGenByArgs("request-id"),
			isFailFast: false,
		},
		{
			err:        errors.ErrUnknown.FastGenByArgs(),
			isFailFast: false,
		},
	}

	ctrl := gomock.NewController(t)
	client := pbMock.NewMockExecutorServiceClient(ctrl)
	serviceCli := NewExecutorServiceClient(client)

	for _, tc := range testCases {
		var (
			requestID           string
			preDispatchComplete atomic.Bool
			timerStarted        atomic.Bool
		)

		args := &DispatchTaskArgs{
			WorkerID:     "worker-1",
			MasterID:     "master-1",
			WorkerType:   1,
			WorkerConfig: []byte("testtest"),
		}

		client.EXPECT().PreDispatchTask(gomock.Any(), matchPreDispatchArgs(args)).
			Do(func(_ context.Context, arg1 *enginepb.PreDispatchTaskRequest, _ ...grpc.CallOption) {
				requestID = arg1.RequestId
				preDispatchComplete.Store(true)
			}).Return(&enginepb.PreDispatchTaskResponse{}, nil).Times(1)

		client.EXPECT().ConfirmDispatchTask(gomock.Any(), matchConfirmDispatch(&requestID, "worker-1")).
			Return((*enginepb.ConfirmDispatchTaskResponse)(nil), tc.err).Do(
			func(_ context.Context, _ *enginepb.ConfirmDispatchTaskRequest, _ ...grpc.CallOption) {
				require.True(t, preDispatchComplete.Load())
				require.True(t, timerStarted.Load())
			}).Times(1)

		err := serviceCli.DispatchTask(context.Background(), args, func() {
			require.True(t, preDispatchComplete.Load())
			require.False(t, timerStarted.Swap(true))
		})

		if tc.isFailFast {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
