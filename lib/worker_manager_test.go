package lib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func fastMarshalDummyStatus(t *testing.T, val int) []byte {
	dummySt := &dummyStatus{Val: val}
	bytes, err := dummySt.Marshal()
	require.NoError(t, err)
	return bytes
}

func TestHeartBeatPingPongAfterCreateWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		false,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
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
		offlined, onlined = manager.Tick(ctx, msgSender)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, 1*time.Second, 10*time.Millisecond)

	msg, ok := msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	cancel()
	wg.Wait()
}

func TestHeartBeatPingPongAfterFailover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		true,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
		&defaultTimeoutConfig).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

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
		offlined, onlined := manager.Tick(ctx, msgSender)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, 1*time.Second, 10*time.Millisecond)

	msg, ok := msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	cancel()
	wg.Wait()
}

func TestMultiplePendingHeartbeats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		true,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
		&defaultTimeoutConfig).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := safeAddWorker(manager, workerID1, executorNodeID1)
	require.NoError(t, err)
	err = safeAddWorker(manager, workerID2, executorNodeID2)
	require.NoError(t, err)
	err = safeAddWorker(manager, workerID3, executorNodeID3)
	require.NoError(t, err)

	offlined, onlined := manager.Tick(ctx, msgSender)
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
		offlined, onlined = manager.Tick(ctx, msgSender)
		totalOnlined += len(onlined)
		require.Empty(t, offlined)
		return totalOnlined == 2
	}, 1*time.Second, 10*time.Millisecond)

	msg, ok := msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime1,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	msg, ok = msgSender.TryPop(executorNodeID2, HeartbeatPongTopic(masterName, workerID2))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime2,
		ReplyTime:  replyTime,
		ToWorkerID: workerID2,
		Epoch:      1,
	}, msg)

	_, ok = msgSender.TryPop(executorNodeID3, HeartbeatPongTopic(masterName, workerID3))
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
	offlined, onlined = manager.Tick(ctx, msgSender)
	require.Empty(t, offlined)
	require.Len(t, onlined, 1)

	_, ok = msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName, workerID1))
	require.False(t, ok)

	_, ok = msgSender.TryPop(executorNodeID2, HeartbeatPongTopic(masterName, workerID2))
	require.False(t, ok)

	msg, ok = msgSender.TryPop(executorNodeID3, HeartbeatPongTopic(masterName, workerID3))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime3,
		ReplyTime:  replyTime,
		ToWorkerID: workerID3,
		Epoch:      1,
	}, msg)

	cancel()
	wg.Wait()
}

func TestWorkerManagerInitialization(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		true,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
		&defaultTimeoutConfig).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	ok, err := manager.IsInitialized(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     clock.MonoNow(),
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	clockDelta :=
		manager.timeoutConfig.workerTimeoutDuration +
			manager.timeoutConfig.workerTimeoutGracefulDuration +
			10*time.Second
	manager.clock.(*clock.Mock).Set(time.Now().Add(clockDelta))

	ok, err = manager.IsInitialized(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = manager.IsInitialized(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	cancel()
	wg.Wait()
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

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	workerMetaClient := NewWorkerMetadataClient(masterName, mockMeta)
	err := workerMetaClient.Store(ctx, workerID1,
		&WorkerStatus{
			Code:         WorkerStatusInit,
			ErrorMessage: "fake",
			ExtBytes:     fastMarshalDummyStatus(t, 7),
		})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		true,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
		&defaultTimeoutConfig,
	).(*workerManagerImpl)

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

	err = msgHandlerManager.InvokeHandler(
		t,
		workerStatusUpdatedTopic(masterName, workerID1),
		executorNodeID1,
		&workerStatusUpdatedMessage{Epoch: 1})
	require.NoError(t, err)

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
	wg.Wait()
}

func TestWorkerTimedOut(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		true,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
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
		offlined, onlined := manager.Tick(ctx, msgSender)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, time.Second, 10*time.Millisecond)

	manager.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration * 2)
	offlined, onlined := manager.Tick(ctx, msgSender)
	require.Len(t, offlined, 1)
	require.Empty(t, onlined)
	require.Len(t, manager.tombstones, 1)

	workers := manager.GetWorkers()
	require.Len(t, workers, 1)
	require.Contains(t, workers, workerID1)
	handle := workers[workerID1]
	require.True(t, handle.IsTombStone())

	cancel()
	wg.Wait()
}

func TestWorkerTimedOutWithPendingHeartbeat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()
	msgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockMeta := metadata.NewMetaMock()
	pool := workerpool.NewDefaultAsyncPool(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	manager := newWorkerManager(
		masterName,
		true,
		1,
		msgSender,
		msgHandlerManager,
		mockMeta,
		pool,
		&defaultTimeoutConfig).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := safeAddWorker(manager, workerID1, executorNodeID1)
	require.NoError(t, err)

	offlined, onlined := manager.Tick(ctx, msgSender)
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

	msgSender.SetBlocked(true)
	require.Eventually(t, func() bool {
		offlined, onlined := manager.Tick(ctx, msgSender)
		require.Empty(t, offlined)
		return len(onlined) == 1
	}, time.Second, 10*time.Millisecond)

	manager.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration * 2)
	offlined, onlined = manager.Tick(ctx, msgSender)
	require.Len(t, offlined, 1)
	require.Empty(t, onlined)
	require.Len(t, manager.tombstones, 1)

	workers := manager.GetWorkers()
	require.Len(t, workers, 1)
	require.Contains(t, workers, workerID1)
	handle := workers[workerID1]
	require.True(t, handle.IsTombStone())

	cancel()
	wg.Wait()
}
