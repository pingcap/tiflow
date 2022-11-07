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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

func TestCheckLeaderAndNeedForward(t *testing.T) {
	t.Parallel()

	serverID := "server1"
	ctx := context.Background()
	h := &preRPCHookImpl[*FailoverRPCClients[int]]{
		id:        serverID,
		leader:    &atomic.Value{},
		leaderCli: &LeaderClientWithLock[*FailoverRPCClients[int]]{},
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
	cli := &FailoverRPCClients[int]{}
	h.leaderCli.Set(cli, func() {
		_ = cli.Close()
	})
	h.leader.Store(&Member{Name: serverID})
	wg.Wait()
}

type (
	mockRPCReq  struct{}
	mockRPCResp struct {
		id int
	}
)

type mockRPCClientIface interface {
	MockRPC(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCResp, error)
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

type mockRPCServer struct {
	hook *preRPCHookImpl[mockRPCClientIface]
}

// MockRPC is an example usage for preRPCHookImpl.PreRPC.
func (s *mockRPCServer) MockRPC(ctx context.Context, req *mockRPCReq, opts ...grpc.CallOption) (*mockRPCResp, error) {
	resp2 := &mockRPCResp{}
	shouldRet, err := s.hook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return &mockRPCResp{1}, nil
}

// newMockRPCServer returns a mockRPCServer that is ready to use.
func newMockRPCServer(t *testing.T) (*mockRPCServer, *mock.MockFeatureChecker) {
	serverID := "server1"
	rpcLim := newRPCLimiter(rate.NewLimiter(rate.Every(time.Second*5), 3), nil)
	mockFeatureChecker := mock.NewMockFeatureChecker(gomock.NewController(t))
	h := &preRPCHookImpl[mockRPCClientIface]{
		id:             serverID,
		leader:         &atomic.Value{},
		leaderCli:      &LeaderClientWithLock[mockRPCClientIface]{},
		featureChecker: mockFeatureChecker,
		limiter:        rpcLim,
	}
	h.leader.Store(&Member{Name: serverID})
	return &mockRPCServer{hook: h}, mockFeatureChecker
}

func TestForwardToLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, mockFeatureChecker := newMockRPCServer(t)
	mockFeatureChecker.EXPECT().Available(gomock.Any()).Return(true).AnyTimes()
	req := &mockRPCReq{}
	resp, err := s.MockRPC(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, resp.id)

	// the server is not leader, and cluster has another leader

	s.hook.leader.Store(&Member{Name: "another"})
	cli := &mockRPCClientImpl{}
	s.hook.leaderCli.Set(cli, func() {})
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
	cli = &mockRPCClientImpl{fail: true}
	s.hook.leaderCli.Set(cli, func() {})
	resp, err = s.MockRPC(ctx, req)
	require.ErrorContains(t, err, "mock failed")
}

func TestFeatureChecker(t *testing.T) {
	t.Parallel()

	s, _ := newMockRPCServer(t)
	ctx := context.Background()
	req := &mockRPCReq{}

	mockFeatureChecker, ok := s.hook.featureChecker.(*mock.MockFeatureChecker)
	require.True(t, ok)

	mockFeatureChecker.EXPECT().Available(gomock.Any()).Return(false)
	_, err := s.MockRPC(ctx, req)
	require.True(t, ErrMasterNotReady.Is(err))

	mockFeatureChecker.EXPECT().Available(gomock.Any()).Return(true)
	_, err = s.MockRPC(ctx, req)
	require.NoError(t, err)
}

func TestRPCLimiter(t *testing.T) {
	t.Parallel()

	allowList := []string{"submit", "cancel"}
	rl := rate.NewLimiter(rate.Every(time.Minute*10), 1)
	rpcLim := newRPCLimiter(rl, allowList)
	require.True(t, rpcLim.Allow(allowList[0]))
	require.True(t, rpcLim.Allow(allowList[1]))
	require.True(t, rpcLim.Allow("query"))
	require.False(t, rpcLim.Allow("query"))
	require.True(t, rpcLim.Allow(allowList[0]))
	require.True(t, rpcLim.Allow(allowList[1]))
	require.False(t, rpcLim.Allow("query"))
	require.False(t, rpcLim.Allow("query"))
}
