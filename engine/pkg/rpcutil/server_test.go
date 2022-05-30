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
	"context"
	gerrors "errors"
	"sync"
	"testing"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

func TestCheckLeaderAndNeedForward(t *testing.T) {
	t.Parallel()

	serverID := "server1"
	ctx := context.Background()
	h := &PreRPCHook[int]{
		id:        serverID,
		leader:    &atomic.Value{},
		leaderCli: &LeaderClientWithLock[int]{},
	}

	isLeader, needForward := h.isLeaderAndNeedForward(ctx)
	require.False(t, isLeader)
	require.False(t, needForward)

	var wg sync.WaitGroup
	cctx, cancel := context.WithCancel(ctx)
	startTime := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		isLeader, needForward := h.isLeaderAndNeedForward(cctx)
		require.False(t, isLeader)
		require.False(t, needForward)
	}()
	cancel()
	wg.Wait()
	// should not wait too long time
	require.Less(t, time.Since(startTime), time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		isLeader, needForward := h.isLeaderAndNeedForward(ctx)
		require.True(t, isLeader)
		require.True(t, needForward)
	}()
	time.Sleep(time.Second)
	h.leaderCli.Set(&FailoverRPCClients[int]{})
	h.leader.Store(&Member{Name: serverID})
	wg.Wait()
}

type (
	mockRPCReq  struct{}
	mockRPCResp struct {
		id int
	}
)

type mockRPCRespWithErrField struct {
	Err *pb.Error
}

type mockRPCClientIface interface {
	MockRPC(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCResp, error)
	MockRPCWithErrField(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCRespWithErrField, error)
}

type mockRPCClientImpl struct {
	fail bool
}

func (m *mockRPCClientImpl) MockRPC(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCResp, error) {
	if m.fail {
		return nil, gerrors.New("mock failed")
	}
	return &mockRPCResp{2}, nil
}

func (m *mockRPCClientImpl) MockRPCWithErrField(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCRespWithErrField, error) {
	if m.fail {
		return nil, gerrors.New("mock failed")
	}
	return &mockRPCRespWithErrField{}, nil
}

type mockRPCServer struct {
	hook *PreRPCHook[mockRPCClientIface]
}

// MockRPC is an example usage for PreRPCHook.PreRPC.
func (s *mockRPCServer) MockRPC(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCResp, error) {
	resp2 := &mockRPCResp{}
	shouldRet, err := s.hook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return &mockRPCResp{1}, nil
}

func (s *mockRPCServer) MockRPCWithErrField(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCRespWithErrField, error) {
	resp2 := &mockRPCRespWithErrField{}
	shouldRet, err := s.hook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return &mockRPCRespWithErrField{}, nil
}

// newMockRPCServer returns a mockRPCServer that is ready to use.
func newMockRPCServer() *mockRPCServer {
	serverID := "server1"
	h := &PreRPCHook[mockRPCClientIface]{
		id:          serverID,
		leader:      &atomic.Value{},
		leaderCli:   &LeaderClientWithLock[mockRPCClientIface]{},
		initialized: atomic.NewBool(true),
		limiter:     rate.NewLimiter(rate.Every(time.Second*5), 3),
	}
	h.leader.Store(&Member{Name: serverID})
	return &mockRPCServer{hook: h}
}

func TestForwardToLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newMockRPCServer()
	req := &mockRPCReq{}
	resp, err := s.MockRPC(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, resp.id)

	// the server is not leader, and cluster has another leader

	s.hook.leader.Store(&Member{Name: "another"})
	s.hook.leaderCli.Set(NewFailoverRPCClientsForTest[mockRPCClientIface](
		&mockRPCClientImpl{},
	))
	resp, err = s.MockRPC(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, resp.id)

	// the server is not leader, and leader info is not initialized

	s.hook.leader = &atomic.Value{}
	resp, err = s.MockRPC(ctx, req)
	require.True(t, errors.ErrMasterRPCNotForward.Equal(err))

	// the server is not leader, and cluster has no leader

	s.hook.leader.Store(&Member{})
	resp, err = s.MockRPC(ctx, req)
	require.True(t, errors.ErrMasterRPCNotForward.Equal(err))

	// the server is not leader, cluster has a leader but the forwarding client is empty due to network problems

	s.hook.leader.Store(&Member{Name: "another"})
	s.hook.leaderCli.inner = nil
	resp, err = s.MockRPC(ctx, req)
	require.True(t, errors.ErrMasterRPCNotForward.Equal(err))

	// forwarding returns error

	s.hook.leaderCli.Set(NewFailoverRPCClientsForTest[mockRPCClientIface](
		&mockRPCClientImpl{fail: true},
	))
	resp, err = s.MockRPC(ctx, req)
	require.ErrorContains(t, err, "mock failed")
}

func TestCheckInitialized(t *testing.T) {
	t.Parallel()

	s := newMockRPCServer()
	ctx := context.Background()
	req := &mockRPCReq{}

	s.hook.initialized.Store(false)
	_, err := s.MockRPC(ctx, req)
	require.True(t, errors.ErrMasterNotInitialized.Equal(err))

	resp, err := s.MockRPCWithErrField(ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.ErrorCode_MasterNotReady, resp.Err.Code)
}
