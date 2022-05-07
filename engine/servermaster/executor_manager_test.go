package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
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
}
