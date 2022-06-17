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

package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
)

func TestExecutorManager(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	heartbeatTTL := time.Millisecond * 100
	checkInterval := time.Millisecond * 10
	mgr := NewExecutorManagerImpl(heartbeatTTL, checkInterval, nil)

	// register an executor server
	executorAddr := "127.0.0.1:10001"
	registerReq := &pb.RegisterExecutorRequest{
		Address:    executorAddr,
		Capability: 2,
	}
	info, err := mgr.AllocateNewExec(registerReq)
	require.Nil(t, err)

	addr, ok := mgr.GetAddr(info.ID)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:10001", addr)

	require.Equal(t, 1, mgr.ExecutorCount(model.Initing))
	require.Equal(t, 0, mgr.ExecutorCount(model.Running))
	mgr.mu.Lock()
	require.Contains(t, mgr.executors, info.ID)
	mgr.mu.Unlock()

	newHeartbeatReq := func() *pb.HeartbeatRequest {
		return &pb.HeartbeatRequest{
			ExecutorId: string(info.ID),
			Status:     int32(model.Running),
			Timestamp:  uint64(time.Now().Unix()),
			Ttl:        uint64(10), // 10ms ttl
		}
	}

	// test executor heartbeat
	resp, err := mgr.HandleHeartbeat(newHeartbeatReq())
	require.Nil(t, err)
	require.Nil(t, resp.Err)

	mgr.Start(ctx)

	require.Eventually(t, func() bool {
		return mgr.ExecutorCount(model.Running) == 0
	}, time.Second*2, time.Millisecond*50)

	// test late heartbeat request after executor is offline
	resp, err = mgr.HandleHeartbeat(newHeartbeatReq())
	require.Nil(t, err)
	require.NotNil(t, resp.Err)
	require.Equal(t, pb.ErrorCode_UnknownExecutor, resp.Err.GetCode())

	cancel()
	mgr.Stop()
}

func TestExecutorManagerWatch(t *testing.T) {
	t.Parallel()

	heartbeatTTL := time.Millisecond * 400
	checkInterval := time.Millisecond * 10
	ctx, cancel := context.WithCancel(context.Background())
	mgr := NewExecutorManagerImpl(heartbeatTTL, checkInterval, nil)
	mgr.Start(ctx)

	// register an executor server
	executorAddr := "127.0.0.1:10001"
	registerReq := &pb.RegisterExecutorRequest{
		Address:    executorAddr,
		Capability: 2,
	}
	info, err := mgr.AllocateNewExec(registerReq)
	require.Nil(t, err)

	executorID1 := info.ID
	snap, stream, err := mgr.WatchExecutors(context.Background())
	require.NoError(t, err)
	require.Equal(t, []model.ExecutorID{executorID1}, snap)

	// register another executor server
	executorAddr = "127.0.0.1:10002"
	registerReq = &pb.RegisterExecutorRequest{
		Address:    executorAddr,
		Capability: 2,
	}
	info, err = mgr.AllocateNewExec(registerReq)
	require.Nil(t, err)

	executorID2 := info.ID
	event := <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID: executorID2,
		Tp: model.EventExecutorOnline,
	}, event)

	newHeartbeatReq := func(executorID model.ExecutorID) *pb.HeartbeatRequest {
		return &pb.HeartbeatRequest{
			ExecutorId: string(executorID),
			Status:     int32(model.Running),
			Timestamp:  uint64(time.Now().Unix()),
			Ttl:        uint64(100), // 10ms ttl
		}
	}

	_, err = mgr.HandleHeartbeat(newHeartbeatReq(executorID1))
	require.NoError(t, err)
	_, err = mgr.HandleHeartbeat(newHeartbeatReq(executorID2))
	require.NoError(t, err)

	require.Equal(t, 2, mgr.ExecutorCount(model.Running))

	require.Eventually(t, func() bool {
		resp, err := mgr.HandleHeartbeat(newHeartbeatReq(executorID2))
		require.NoError(t, err)
		require.Nil(t, resp.Err)
		return mgr.ExecutorCount(model.Running) == 1
	}, time.Second*2, time.Millisecond*5)

	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID: executorID1,
		Tp: model.EventExecutorOffline,
	}, event)

	cancel()
	mgr.Stop()
}
