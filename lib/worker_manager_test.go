package lib

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	derror "github.com/hanfei1991/microcosm/pkg/errors"

	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/dig"

	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func fastMarshalDummyStatus(t *testing.T, val int) []byte {
	dummySt := &dummyStatus{Val: val}
	bytes, err := dummySt.Marshal()
	require.NoError(t, err)
	return bytes
}

type digParamList struct {
	dig.Out

	MessageSender         p2p.MessageSender
	MessageHandlerManager p2p.MessageHandlerManager
	MetaClient            metadata.MetaKV
	Pool                  workerpool.AsyncPool
}

type workerManagerTestSuite struct {
	digParamList

	wg     *sync.WaitGroup
	cancel context.CancelFunc
}

func newWorkerManagerTestSuite(ctx context.Context) *workerManagerTestSuite {
	pool := workerpool.NewDefaultAsyncPool(1)
	poolCtx, cancel := context.WithCancel(ctx)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(poolCtx)
	}()

	return &workerManagerTestSuite{
		digParamList: digParamList{
			MessageSender:         p2p.NewMockMessageSender(),
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MetaClient:            metadata.NewMetaMock(),
			Pool:                  pool,
		},
		wg:     wg,
		cancel: cancel,
	}
}

func (s *workerManagerTestSuite) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *workerManagerTestSuite) BuildDepsContext(t *testing.T) *dcontext.Context {
	dp := deps.NewDeps()
	err := dp.Provide(func() digParamList {
		return s.digParamList
	})
	require.NoError(t, err)
	return dcontext.Background().WithDeps(dp)
}

func TestHeartBeatPingPongAfterCreateWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		false,
		1,
		&defaultTimeoutConfig,
	).(*workerManagerImpl)

	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := manager.OnWorkerCreated(ctx, workerID1, executorNodeID1)
	require.NoError(t, err)

	sendTime := clock.MonoNow()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	replyTime := time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)

	var offlined, onlined []*WorkerInfo
	require.Eventually(t, func() bool {
		offlined, onlined = manager.Tick(ctx)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, 1*time.Second, 10*time.Millisecond)

	msg, ok := manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	cancel()
	suite.Close()
}

func TestHeartBeatPingPongAfterFailover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig).(*workerManagerImpl)

	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	require.Eventually(t, func() bool {
		_, _ = manager.Tick(ctx)
		return manager.fsmState.Load() == workerManagerWaitingHeartbeats
	}, 1*time.Second, 10*time.Millisecond)

	sendTime := clock.MonoNow()
	err := manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	replyTime := time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)

	require.Eventually(t, func() bool {
		offlined, onlined := manager.Tick(ctx)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, 1*time.Second, 10*time.Millisecond)

	msg, ok := manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	cancel()
	suite.Close()
}

func TestMultiplePendingHeartbeats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig).(*workerManagerImpl)

	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	require.Eventually(t, func() bool {
		_, _ = manager.Tick(ctx)
		return manager.fsmState.Load() == workerManagerWaitingHeartbeats
	}, 1*time.Second, 10*time.Millisecond)

	err := safeAddWorker(manager, workerID1, executorNodeID1)
	require.NoError(t, err)
	err = safeAddWorker(manager, workerID2, executorNodeID2)
	require.NoError(t, err)
	err = safeAddWorker(manager, workerID3, executorNodeID3)
	require.NoError(t, err)

	offlined, onlined := manager.Tick(ctx)
	require.Empty(t, offlined)
	require.Empty(t, onlined)

	sendTime1 := clock.MonoNow()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime1,
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	sendTime2 := clock.MonoNow()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime2,
		FromWorkerID: workerID2,
		Epoch:        1,
	}, executorNodeID2)
	require.NoError(t, err)

	replyTime := time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)

	var totalOnlined int
	require.Eventually(t, func() bool {
		offlined, onlined = manager.Tick(ctx)
		totalOnlined += len(onlined)
		require.Empty(t, offlined)
		return totalOnlined == 2
	}, 1*time.Second, 10*time.Millisecond)

	msg, ok := manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime1,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	msg, ok = manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID2, HeartbeatPongTopic(masterName, workerID2))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime2,
		ReplyTime:  replyTime,
		ToWorkerID: workerID2,
		Epoch:      1,
	}, msg)

	_, ok = manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID3, HeartbeatPongTopic(masterName, workerID3))
	require.False(t, ok)

	sendTime3 := clock.MonoNow()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime3,
		FromWorkerID: workerID3,
		Epoch:        1,
	}, executorNodeID3)
	require.NoError(t, err)

	replyTime = time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)
	offlined, onlined = manager.Tick(ctx)
	require.Empty(t, offlined)
	require.Len(t, onlined, 1)

	_, ok = manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.False(t, ok)

	_, ok = manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID2, HeartbeatPongTopic(masterName, workerID2))
	require.False(t, ok)

	msg, ok = manager.messageSender.(*p2p.MockMessageSender).
		TryPop(executorNodeID3, HeartbeatPongTopic(masterName, workerID3))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime3,
		ReplyTime:  replyTime,
		ToWorkerID: workerID3,
		Epoch:      1,
	}, msg)

	cancel()
	suite.Close()
}

func TestWorkerManagerInitialization(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig).(*workerManagerImpl)

	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	ok := manager.IsInitialized()
	require.False(t, ok)

	manager.Tick(ctx)
	ok = manager.IsInitialized()
	require.False(t, ok)

	err := manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     clock.MonoNow(),
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	manager.Tick(ctx)
	ok = manager.IsInitialized()
	require.False(t, ok)

	require.Eventually(t, func() bool {
		_, _ = manager.Tick(ctx)
		return manager.fsmState.Load() == workerManagerWaitingHeartbeats
	}, 1*time.Second, 10*time.Millisecond)

	clockDelta :=
		manager.timeoutConfig.workerTimeoutDuration +
			manager.timeoutConfig.workerTimeoutGracefulDuration +
			10*time.Second
	manager.clock.(*clock.Mock).Set(time.Now().Add(clockDelta))

	require.Eventually(t, func() bool {
		manager.Tick(ctx)
		return manager.IsInitialized()
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	suite.Close()
}

func safeAddWorker(manager *workerManagerImpl, id WorkerID, executorNodeID p2p.NodeID) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.addWorker(id, executorNodeID)
}

func TestUpdateStatus(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	workerMetaClient := NewWorkerMetadataClient(masterName, suite.MetaClient)
	err := workerMetaClient.Store(ctx, workerID1,
		&WorkerStatus{
			Code:         WorkerStatusInit,
			ErrorMessage: "fake",
			ExtBytes:     fastMarshalDummyStatus(t, 7),
		})
	require.NoError(t, err)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig).(*workerManagerImpl)

	err = safeAddWorker(manager, workerID1, executorNodeID1)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		info, ok := manager.GetWorkerInfo(workerID1)
		if !ok {
			return false
		}
		return info.statusInitialized.Load()
	}, time.Second, time.Millisecond*10)

	status, ok := manager.GetStatus(workerID1)
	require.True(t, ok)
	require.Equal(t, WorkerStatusInit, status.Code)

	err = workerMetaClient.Store(ctx, workerID1,
		&WorkerStatus{
			Code:         WorkerStatusError,
			ErrorMessage: "fake",
			ExtBytes:     fastMarshalDummyStatus(t, 7),
		})
	require.NoError(t, err)

	manager.OnWorkerStatusUpdated(&WorkerStatusUpdatedMessage{
		FromWorkerID: workerID1,
		Epoch:        1,
	})

	require.Eventually(t, func() bool {
		err := manager.CheckStatusUpdate(ctx)
		require.NoError(t, err)
		return manager.GetWorkerHandle(workerID1).Status().Code == WorkerStatusError
	}, time.Second, 10*time.Millisecond)

	handle := manager.GetWorkerHandle(workerID1)
	require.NotNil(t, handle)
	require.False(t, handle.IsTombStone())
	require.Equal(t, &WorkerStatus{
		Code:         WorkerStatusError,
		ErrorMessage: "fake",
		ExtBytes:     fastMarshalDummyStatus(t, 7),
	}, handle.Status())

	cancel()
	suite.Close()
}

func TestWorkerTimedOut(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig,
	).(*workerManagerImpl)

	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := safeAddWorker(manager, workerID1, executorNodeID1)
	require.NoError(t, err)

	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     manager.clock.Mono(),
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		offlined, onlined := manager.Tick(ctx)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, time.Second, 10*time.Millisecond)

	manager.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration * 2)
	offlined, onlined := manager.Tick(ctx)
	require.Len(t, offlined, 1)
	require.Empty(t, onlined)
	require.Len(t, manager.tombstones, 1)

	workers := manager.GetWorkers()
	require.Len(t, workers, 1)
	require.Contains(t, workers, workerID1)
	handle := workers[workerID1]
	require.True(t, handle.IsTombStone())

	cancel()
	suite.Close()
}

func TestWorkerTimedOutWithPendingHeartbeat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := safeAddWorker(manager, workerID1, executorNodeID1)
	require.NoError(t, err)

	offlined, onlined := manager.Tick(ctx)
	require.Empty(t, offlined)
	require.Empty(t, onlined)

	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     manager.clock.Mono(),
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	info, ok := manager.GetWorkerInfo(workerID1)
	require.True(t, ok)
	require.True(t, info.hasPendingHeartbeat)

	suite.MessageSender.(*p2p.MockMessageSender).SetBlocked(true)
	require.Eventually(t, func() bool {
		offlined, onlined := manager.Tick(ctx)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, time.Second, 10*time.Millisecond)

	manager.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration * 2)
	offlined, onlined = manager.Tick(ctx)
	require.Len(t, offlined, 1)
	require.Empty(t, onlined)
	require.Len(t, manager.tombstones, 1)

	workers := manager.GetWorkers()
	require.Len(t, workers, 1)
	require.Contains(t, workers, workerID1)
	handle := workers[workerID1]
	require.True(t, handle.IsTombStone())

	cancel()
	suite.Close()
}

func TestWorkerTombstones(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	suite := newWorkerManagerTestSuite(ctx)

	manager := newWorkerManager(
		suite.BuildDepsContext(t),
		masterName,
		true,
		1,
		&defaultTimeoutConfig).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	workerMetaClient := NewWorkerMetadataClient(masterName, suite.MetaClient)
	for i := 0; i < 10; i++ {
		err := workerMetaClient.Store(ctx, fmt.Sprintf("worker-%d", i), &WorkerStatus{Code: WorkerStatusNormal})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		_, _ = manager.Tick(ctx)
		return manager.fsmState.Load() == workerManagerWaitingHeartbeats
	}, 1*time.Second, 10*time.Millisecond)

	manager.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration / 2)

	err := manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     clock.MonoNow(),
		FromWorkerID: "worker-0",
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     clock.MonoNow(),
		FromWorkerID: "worker-1",
		Epoch:        1,
	}, executorNodeID2)
	require.NoError(t, err)

	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     clock.MonoNow(),
		FromWorkerID: "worker-2",
		Epoch:        1,
	}, executorNodeID3)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		manager.clock.(*clock.Mock).Add(1 * time.Second)
		_, _ = manager.Tick(ctx)
		return manager.fsmState.Load() == workerManagerNormal
	}, 1*time.Second, 10*time.Millisecond)

	handles := manager.GetWorkers()
	require.Len(t, handles, 10)

	for i := 0; i < 10; i++ {
		handle := manager.GetWorkerHandle(fmt.Sprintf("worker-%d", i))
		require.NotNil(t, handle)
		if i < 3 {
			require.False(t, handle.IsTombStone(), "worker-%d", i)
		} else {
			require.True(t, handle.IsTombStone(), "worker-%d", i)
		}
	}

	handle := manager.GetWorkerHandle("worker-5")
	ok, err := handle.DeleteTombStone(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	require.Eventually(t, func() bool {
		workerMetaClient := NewWorkerMetadataClient(masterName, suite.MetaClient)
		_, err := workerMetaClient.Load(ctx, "worker-5")
		return derror.ErrWorkerNoMeta.Equal(err)
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	suite.Close()
}
