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

package internal

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockRequest struct {
	val int
}

type mockResponse struct {
	val int
}

func TestCallNormal(t *testing.T) {
	t.Parallel()

	mockCallFn := func(ctx context.Context, req *mockRequest, opts ...grpc.CallOption) (*mockResponse, error) {
		return &mockResponse{
			val: req.val,
		}, nil
	}

	call := NewCall(mockCallFn, &mockRequest{val: 1})
	resp, err := call.Do(context.Background())
	require.NoError(t, err)
	require.Equal(t, &mockResponse{val: 1}, resp)
}

func TestCallFuncCancel(t *testing.T) {
	t.Parallel()

	mockCallFn := func(ctx context.Context, req *mockRequest, opts ...grpc.CallOption) (*mockResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	call := NewCall(mockCallFn, &mockRequest{val: 1})
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		resp, err := call.Do(ctx)
		require.Nil(t, resp)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	}()

	cancel()
	wg.Wait()
}

func TestCallRetryCancel(t *testing.T) {
	t.Parallel()

	mockCallFn := func(ctx context.Context, req *mockRequest, opts ...grpc.CallOption) (*mockResponse, error) {
		return nil, status.Error(codes.Unavailable, "")
	}

	call := NewCall(mockCallFn, &mockRequest{val: 1})
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		resp, err := call.Do(ctx)
		require.Nil(t, resp)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	}()

	cancel()
	wg.Wait()
}

func TestCallRetrySucceed(t *testing.T) {
	t.Parallel()

	retryCount := 0
	mockCallFn := func(ctx context.Context, req *mockRequest, opts ...grpc.CallOption) (*mockResponse, error) {
		if retryCount < 5 {
			retryCount++
		} else {
			return &mockResponse{val: req.val}, nil
		}
		return nil, status.Error(codes.Unavailable, "")
	}

	call := NewCall(mockCallFn, &mockRequest{val: 1})
	resp, err := call.Do(context.Background())
	require.NoError(t, err)
	require.Equal(t, &mockResponse{val: 1}, resp)
}

func TestCallForceNoRetry(t *testing.T) {
	t.Parallel()

	retryCount := 0
	mockCallFn := func(ctx context.Context, req *mockRequest, opts ...grpc.CallOption) (*mockResponse, error) {
		if retryCount < 5 {
			retryCount++
		} else {
			return &mockResponse{val: req.val}, nil
		}
		return nil, status.Error(codes.Unavailable, "should not retry")
	}

	call := NewCall(mockCallFn, &mockRequest{val: 1}, WithForceNoRetry())
	_, err := call.Do(context.Background())
	require.Error(t, err)
}
