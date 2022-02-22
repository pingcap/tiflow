package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/stretchr/testify/require"
)

func TestExecutorManager(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	// test allocate resource to given task request
	tasks := []*pb.ScheduleTask{
		{
			Task: &pb.TaskRequest{Id: 1, OpTp: int32(model.JobMasterType)},
			Cost: 1,
		},
	}
	allocated, allocResp := mgr.Allocate(tasks)
	require.True(t, allocated)
	require.Equal(t, 1, len(allocResp.GetSchedule()))
	require.Equal(t, map[int64]*pb.ScheduleResult{
		1: {
			ExecutorId: string(info.ID),
			Addr:       executorAddr,
		},
	}, allocResp.GetSchedule())

	mgr.Start(ctx)

	require.Eventually(t, func() bool {
		return mgr.ExecutorCount(model.Running) == 0
	}, time.Second*2, time.Millisecond*50)

	// test late heartbeat request after executor is offline
	resp, err = mgr.HandleHeartbeat(newHeartbeatReq())
	require.Nil(t, err)
	require.NotNil(t, resp.Err)
	require.Equal(t, pb.ErrorCode_UnknownExecutor, resp.Err.GetCode())
}
