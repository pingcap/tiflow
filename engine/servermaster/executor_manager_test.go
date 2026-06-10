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

	"github.com/golang/mock/gomock"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/orm/mock"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestExecutorManager(t *testing.T) {
	t.Parallel()

	metaClient := mock.NewMockClient(gomock.NewController(t))
	ctx, cancel := context.WithCancel(context.Background())
	heartbeatTTL := time.Millisecond * 100
	checkInterval := time.Millisecond * 10
	mgr := NewExecutorManagerImpl(metaClient, heartbeatTTL, checkInterval)

	// register an executor server
	executorAddr := "127.0.0.1:10001"
	registerReq := &pb.RegisterExecutorRequest{
		Executor: &pb.Executor{Address: executorAddr},
	}
	metaClient.EXPECT().
		CreateExecutor(gomock.Any(), gomock.Any()).Times(1).
		DoAndReturn(func(ctx context.Context, executor *ormModel.Executor) error {
			require.NotEmpty(t, executor.ID)
			require.Equal(t, executorAddr, executor.Address)
			return nil
		})

	executor, err := mgr.AllocateNewExec(ctx, registerReq)
	require.Nil(t, err)

	addr, ok := mgr.GetAddr(executor.ID)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:10001", addr)

	require.Equal(t, 1, mgr.ExecutorCount(model.Initing))
	require.Equal(t, 0, mgr.ExecutorCount(model.Running))
	mgr.mu.Lock()
	require.Contains(t, mgr.executors, executor.ID)
	mgr.mu.Unlock()

	newHeartbeatReq := func() *pb.HeartbeatRequest {
		return &pb.HeartbeatRequest{
			ExecutorId: string(executor.ID),
			Timestamp:  uint64(time.Now().Unix()),
			Ttl:        uint64(10), // 10ms ttl
		}
	}

	// test executor heartbeat
	_, err = mgr.HandleHeartbeat(newHeartbeatReq())
	require.NoError(t, err)

	metaClient.EXPECT().QueryExecutors(gomock.Any()).Times(1).Return([]*ormModel.Executor{}, nil)
	metaClient.EXPECT().DeleteExecutor(gomock.Any(), executor.ID).Times(1).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return mgr.ExecutorCount(model.Running) == 0
	}, time.Second*2, time.Millisecond*50)

	// test late heartbeat request after executor is offline
	_, err = mgr.HandleHeartbeat(newHeartbeatReq())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrUnknownExecutor))

	cancel()
	wg.Wait()
}

func TestExecutorManagerWatch(t *testing.T) {
	t.Parallel()

	metaClient := mock.NewMockClient(gomock.NewController(t))

	heartbeatTTL := time.Millisecond * 400
	checkInterval := time.Millisecond * 50
	ctx, cancel := context.WithCancel(context.Background())
	mgr := NewExecutorManagerImpl(metaClient, heartbeatTTL, checkInterval)

	// register an executor server
	executorAddr := "127.0.0.1:10001"
	registerReq := &pb.RegisterExecutorRequest{
		Executor: &pb.Executor{Address: executorAddr},
	}
	metaClient.EXPECT().
		CreateExecutor(gomock.Any(), gomock.Any()).Times(1).
		DoAndReturn(func(ctx context.Context, executor *ormModel.Executor) error {
			require.NotEmpty(t, executor.ID)
			require.Equal(t, executorAddr, executor.Address)
			return nil
		})
	executor, err := mgr.AllocateNewExec(ctx, registerReq)
	require.Nil(t, err)

	executorID1 := executor.ID
	snap, stream, err := mgr.WatchExecutors(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[model.ExecutorID]string{
		executorID1: executor.Address,
	}, snap)

	// register another executor server
	executorAddr = "127.0.0.1:10002"
	registerReq = &pb.RegisterExecutorRequest{
		Executor: &pb.Executor{Address: executorAddr},
	}
	metaClient.EXPECT().
		CreateExecutor(gomock.Any(), gomock.Any()).Times(1).
		DoAndReturn(func(ctx context.Context, executor *ormModel.Executor) error {
			require.NotEmpty(t, executor.ID)
			require.Equal(t, executorAddr, executor.Address)
			return nil
		})
	executor, err = mgr.AllocateNewExec(ctx, registerReq)
	require.Nil(t, err)

	executorID2 := executor.ID
	event := <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID:   executorID2,
		Tp:   model.EventExecutorOnline,
		Addr: "127.0.0.1:10002",
	}, event)

	newHeartbeatReq := func(executorID model.ExecutorID) *pb.HeartbeatRequest {
		return &pb.HeartbeatRequest{
			ExecutorId: string(executorID),
			Timestamp:  uint64(time.Now().Unix()),
			Ttl:        uint64(100), // 10ms ttl
		}
	}

	bgExecutorHeartbeat := func(
		ctx context.Context, wg *sync.WaitGroup, executorID model.DeployNodeID,
	) context.CancelFunc {
		// send a synchronous heartbeat first in order to ensure the online
		// count of this executor takes effect immediately.
		_, err := mgr.HandleHeartbeat(newHeartbeatReq(executorID))
		require.NoError(t, err)

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
					_, err := mgr.HandleHeartbeat(newHeartbeatReq(executorID))
					require.NoError(t, err)
				}
			}
		}()
		return cancelIn
	}

	metaClient.EXPECT().QueryExecutors(gomock.Any()).Times(1).
		Return([]*ormModel.Executor{
			{ID: executorID1, Address: "127.0.0.1:10001"},
			{ID: executorID2, Address: "127.0.0.1:10002"},
		}, nil)
	metaClient.EXPECT().DeleteExecutor(gomock.Any(), executorID1).Times(1).Return(nil)
	metaClient.EXPECT().DeleteExecutor(gomock.Any(), executorID2).Times(1).Return(nil)

	var mgrWg sync.WaitGroup
	mgrWg.Add(1)
	go func() {
		defer mgrWg.Done()
		mgr.Run(ctx)
	}()

	// mgr.Start will reset executors first, so there will be two online events.
	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID:   executorID1,
		Tp:   model.EventExecutorOnline,
		Addr: "127.0.0.1:10001",
	}, event)
	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID:   executorID2,
		Tp:   model.EventExecutorOnline,
		Addr: "127.0.0.1:10002",
	}, event)

	var wg sync.WaitGroup
	require.Equal(t, 0, mgr.ExecutorCount(model.Running))
	cancel1 := bgExecutorHeartbeat(ctx, &wg, executorID1)
	cancel2 := bgExecutorHeartbeat(ctx, &wg, executorID2)
	require.Equal(t, 2, mgr.ExecutorCount(model.Running))

	// executor-1 will time out
	cancel1()
	require.Eventually(t, func() bool {
		return mgr.ExecutorCount(model.Running) == 1
	}, time.Second*2, time.Millisecond*5)

	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID:   executorID1,
		Tp:   model.EventExecutorOffline,
		Addr: "127.0.0.1:10001",
	}, event)

	// executor-2 will time out
	cancel2()
	wg.Wait()
	event = <-stream.C
	require.Equal(t, model.ExecutorStatusChange{
		ID:   executorID2,
		Tp:   model.EventExecutorOffline,
		Addr: "127.0.0.1:10002",
	}, event)

	cancel()
	mgrWg.Wait()
}
