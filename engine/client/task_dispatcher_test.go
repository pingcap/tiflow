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
	"sync"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/engine/pb"
)

func TestDispatchTaskNormal(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	var (
		requestID           string
		preDispatchComplete atomic.Bool
		cbCalled            atomic.Bool
	)
	mockExecClient.On("Send", mock.Anything, mock.MatchedBy(func(req *ExecutorRequest) bool {
		if req.Cmd != CmdPreDispatchTask {
			return false
		}
		preDispatchReq := req.Req.(*pb.PreDispatchTaskRequest)
		checkReqMatchesArgs(t, preDispatchReq, args)
		requestID = preDispatchReq.RequestId
		preDispatchComplete.Store(true)
		return true
	})).Return(&ExecutorResponse{Resp: &pb.PreDispatchTaskResponse{}}, nil)
	mockExecClient.On("Send", mock.Anything, mock.MatchedBy(func(req *ExecutorRequest) bool {
		if req.Cmd != CmdConfirmDispatchTask {
			return false
		}
		confirmDispatchReq := req.Req.(*pb.ConfirmDispatchTaskRequest)
		require.Equal(t, requestID, confirmDispatchReq.GetRequestId())
		require.Equal(t, args.WorkerID, confirmDispatchReq.GetWorkerId())
		return true
	})).Return(&ExecutorResponse{Resp: &pb.ConfirmDispatchTaskResponse{}}, nil)

	err := dispatcher.DispatchTask(context.Background(), args, func() {
		require.True(t, preDispatchComplete.Load())
		require.False(t, cbCalled.Swap(true))
	}, func(error) {
		require.Fail(t, "not expected")
	})
	require.NoError(t, err)
	mockExecClient.AssertExpectations(t)
}

func TestPreDispatchAborted(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}
	mockExecClient.On("Send", mock.Anything, mock.Anything).
		Return((*ExecutorResponse)(nil), status.Error(codes.Aborted, "aborted error")).
		Once() // Aborted calls should NOT be retried.

	err := dispatcher.DispatchTask(context.Background(), args, func() {
		require.Fail(t, "the callback should never be called")
	}, func(error) {
		require.Fail(t, "not expected")
	})
	require.Error(t, err)
	require.Regexp(t, ".*aborted error.*", err)
	mockExecClient.AssertExpectations(t)
}

func TestAlreadyExistsPanics(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}
	mockExecClient.On("Send", mock.Anything, mock.Anything).
		Return((*ExecutorResponse)(nil), status.Error(codes.AlreadyExists, "already exists error")).
		Once()

	require.Panics(t, func() {
		_ = dispatcher.DispatchTask(context.Background(), args, func() {
			require.Fail(t, "the callback should never be called")
		}, func(error) {
			require.Fail(t, "not expected")
		})
	})
	mockExecClient.AssertExpectations(t)
}

func TestDispatchRetryCanceled(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)
	// Resets the retryInterval to accelerate testing.
	dispatcher.retryInterval = time.Millisecond * 1

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	var (
		retryCount atomic.Int64
		wg         sync.WaitGroup
	)
	mockExecClient.On("Send", mock.Anything, mock.Anything).
		Return((*ExecutorResponse)(nil), status.Error(codes.Unknown, "should retry")).Run(
		func(args mock.Arguments) {
			retryCount.Add(1)
		})

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Eventually(t, func() bool {
			return retryCount.Load() > 10
		}, 1*time.Second, 1*time.Millisecond)
		cancel()
	}()

	err := dispatcher.DispatchTask(cancelCtx, args, func() {
		require.Fail(t, "the callback should never be called")
	}, func(error) {
		require.Fail(t, "not expected")
	})
	require.Error(t, err)
	require.Regexp(t, ".*ErrExecutorPreDispatchFailed.*", err)

	wg.Wait()
}

func TestDispatchRetrySucceed(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)
	// Resets the retryInterval to accelerate testing.
	dispatcher.retryInterval = time.Millisecond * 1

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	mockExecClient.On("Send", mock.Anything, mock.Anything).
		Return((*ExecutorResponse)(nil), status.Error(codes.Unknown, "should retry")).Twice()
	mockExecClient.On("Send", mock.Anything, mock.Anything).
		Return(&ExecutorResponse{Resp: &pb.ConfirmDispatchTaskResponse{}}, nil)
	err := dispatcher.DispatchTask(context.Background(), args, func() {}, func(error) {
		require.Fail(t, "not expected")
	})
	require.NoError(t, err)
	mockExecClient.AssertExpectations(t)
}

func TestConfirmDispatchFailsUndertermined(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	var (
		preDispatchComplete atomic.Bool
		cbCalled            atomic.Bool
	)
	mockExecClient.On("Send", mock.Anything, mock.MatchedBy(func(req *ExecutorRequest) bool {
		if req.Cmd != CmdPreDispatchTask {
			return false
		}
		preDispatchReq := req.Req.(*pb.PreDispatchTaskRequest)
		checkReqMatchesArgs(t, preDispatchReq, args)
		preDispatchComplete.Store(true)
		return true
	})).Return(&ExecutorResponse{Resp: &pb.PreDispatchTaskResponse{}}, nil)
	mockExecClient.On("Send", mock.Anything, mock.MatchedBy(func(req *ExecutorRequest) bool {
		return req.Cmd == CmdConfirmDispatchTask
	})).Return((*ExecutorResponse)(nil), status.Error(codes.Unknown, "no retry"))

	err := dispatcher.DispatchTask(context.Background(), args, func() {
		require.True(t, preDispatchComplete.Load())
		require.False(t, cbCalled.Swap(true))
	}, func(error) {
		require.Fail(t, "not expected")
	})
	require.NoError(t, err)
	mockExecClient.AssertExpectations(t)
}

func TestConfirmDispatchFailsGuaranteed(t *testing.T) {
	t.Parallel()

	mockExecClient := &MockExecutorClient{}
	dispatcher := newTaskDispatcher(mockExecClient)

	args := &DispatchTaskArgs{
		WorkerID:     "worker-1",
		MasterID:     "master-1",
		WorkerType:   1,
		WorkerConfig: []byte("testtest"),
	}

	var (
		preDispatchComplete atomic.Bool
		startTimerCalled    atomic.Bool
		abortWorkerCalled   atomic.Bool
	)
	mockExecClient.On("Send", mock.Anything, mock.MatchedBy(func(req *ExecutorRequest) bool {
		if req.Cmd != CmdPreDispatchTask {
			return false
		}
		preDispatchReq := req.Req.(*pb.PreDispatchTaskRequest)
		checkReqMatchesArgs(t, preDispatchReq, args)
		preDispatchComplete.Store(true)
		return true
	})).Return(&ExecutorResponse{Resp: &pb.PreDispatchTaskResponse{}}, nil)
	mockExecClient.On("Send", mock.Anything, mock.MatchedBy(func(req *ExecutorRequest) bool {
		return req.Cmd == CmdConfirmDispatchTask
	})).Return((*ExecutorResponse)(nil), status.Error(codes.Aborted, "server end failure"))

	err := dispatcher.DispatchTask(context.Background(), args, func() {
		require.True(t, preDispatchComplete.Load())
		require.False(t, startTimerCalled.Swap(true))
	}, func(err error) {
		require.Error(t, err)
		require.Regexp(t, ".*server end failure.*", err)

		require.True(t, startTimerCalled.Load())
		require.False(t, abortWorkerCalled.Swap(true))
	})
	require.Error(t, err)
	require.Regexp(t, ".*server end failure.*", err)

	require.True(t, abortWorkerCalled.Load())
	mockExecClient.AssertExpectations(t)
}

func checkReqMatchesArgs(t *testing.T, req *pb.PreDispatchTaskRequest, args *DispatchTaskArgs) {
	require.Equal(t, args.WorkerID, req.GetWorkerId())
	require.Equal(t, args.MasterID, req.GetMasterId())
	require.Equal(t, args.WorkerType, req.GetTaskTypeId())
	require.Equal(t, args.WorkerConfig, req.GetTaskConfig())
}
