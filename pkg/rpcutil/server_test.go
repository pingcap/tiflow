package rpcutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCheckLeaderAndNeedForward(t *testing.T) {
	t.Parallel()

	serverID := "server1"
	ctx := context.Background()
	h := &PreRPCHooker[int]{
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
	h.leaderCli.Lock()
	h.leaderCli.Inner = &FailoverRPCClients[int]{}
	h.leaderCli.Unlock()
	h.leader.Store(&Member{Name: serverID})
	wg.Wait()
}
