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
	"sync"
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
	checkInterval := time.Millisecond * 50
	ctx, cancel := context.WithCancel(context.Background())
	mgr := NewExecutorManagerImpl(heartbeatTTL, checkInterval, nil)

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
			Ttl:        uint64(10), // 10ms ttl
		}
	}

	bgExecutorHeartbeat := func(
		ctx context.Context, wg *sync.WaitGroup, executorID model.DeployNodeID,
	) context.CancelFunc {
		// send a blocking heartbeat first in order to check online count takes
		// effect immediately.
		resp, err := mgr.HandleHeartbeat(newHeartbeatReq(executorID))
		require.NoError(t, err)
		require.Nil(t, resp.Err)

		ctxIn, cancelIn := context.WithCancel(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctxIn.Done():
					return
				case <-ticker.C:
					resp, err := mgr.HandleHeartbeat(newHeartbeatReq(executorID))
					require.NoError(t, err)
					require.Nil(t, resp.Err)
				}
			}
		}()
		return cancelIn
	}

	mgr.Start(ctx)
	require.Equal(t, 0, mgr.ExecutorCount(model.Running))
	var wg sync.WaitGroup
	cancel1 := bgExecutorHeartbeat(ctx, &wg, executorID1)
	cancel2 := bgExecutorHeartbeat(ctx, &wg, executorID2)
	require.Equal(t, 2, mgr.ExecutorCount(model.Running))

	// executor-1 will timeout
	cancel1()
	require.Eventually(t, func() bool {
		return mgr.ExecutorCount(model.Running) == 1
	}, time.Second*2, time.Millisecond*5)

	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID: executorID1,
		Tp: model.EventExecutorOffline,
	}, event)

	// executor-2 will timeout
	cancel2()
	wg.Wait()
	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID: executorID2,
		Tp: model.EventExecutorOffline,
	}, event)

	cancel()
	mgr.Stop()
}
