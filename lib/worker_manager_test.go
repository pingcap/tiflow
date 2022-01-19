package lib

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/gavv/monotime"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/stretchr/testify/require"
)

func TestHeartBeatPingPongAfterCreateWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()

	manager := newWorkerManager(masterName, false, 1).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := manager.AddWorker(workerID1, executorNodeID1, WorkerStatusCreated)
	require.NoError(t, err)

	sendTime := monotime.Now()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	replyTime := time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)
	offlined, onlined := manager.Tick(ctx, msgSender)
	require.Empty(t, offlined)
	require.Len(t, onlined, 1)

	msg, ok := msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)
}

func TestHeartBeatPingPongAfterFailover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()

	manager := newWorkerManager(masterName, true, 1).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	sendTime := monotime.Now()
	err := manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	replyTime := time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)
	offlined, onlined := manager.Tick(ctx, msgSender)
	require.Empty(t, offlined)
	require.Len(t, onlined, 1)

	msg, ok := msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)
}

func TestMultiplePendingHeartbeats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()

	manager := newWorkerManager(masterName, true, 1).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := manager.AddWorker(workerID1, executorNodeID1, WorkerStatusCreated)
	require.NoError(t, err)
	err = manager.AddWorker(workerID2, executorNodeID2, WorkerStatusCreated)
	require.NoError(t, err)
	err = manager.AddWorker(workerID3, executorNodeID3, WorkerStatusCreated)
	require.NoError(t, err)

	offlined, onlined := manager.Tick(ctx, msgSender)
	require.Empty(t, offlined)
	require.Empty(t, onlined)

	sendTime1 := monotime.Now()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime1,
		FromWorkerID: workerID1,
		Epoch:        1,
	}, executorNodeID1)
	require.NoError(t, err)

	sendTime2 := monotime.Now()
	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     sendTime2,
		FromWorkerID: workerID2,
		Epoch:        1,
	}, executorNodeID2)
	require.NoError(t, err)

	replyTime := time.Now()
	manager.clock.(*clock.Mock).Set(replyTime)
	offlined, onlined = manager.Tick(ctx, msgSender)
	require.Empty(t, offlined)
	require.Len(t, onlined, 2)

	msg, ok := msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime1,
		ReplyTime:  replyTime,
		ToWorkerID: workerID1,
		Epoch:      1,
	}, msg)

	msg, ok = msgSender.TryPop(executorNodeID2, HeartbeatPongTopic(masterName))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime2,
		ReplyTime:  replyTime,
		ToWorkerID: workerID2,
		Epoch:      1,
	}, msg)

	_, ok = msgSender.TryPop(executorNodeID3, HeartbeatPongTopic(masterName))
	require.False(t, ok)

	sendTime3 := monotime.Now()
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

	_, ok = msgSender.TryPop(executorNodeID1, HeartbeatPongTopic(masterName))
	require.False(t, ok)

	_, ok = msgSender.TryPop(executorNodeID2, HeartbeatPongTopic(masterName))
	require.False(t, ok)

	msg, ok = msgSender.TryPop(executorNodeID3, HeartbeatPongTopic(masterName))
	require.True(t, ok)
	require.Equal(t, &HeartbeatPongMessage{
		SendTime:   sendTime3,
		ReplyTime:  replyTime,
		ToWorkerID: workerID3,
		Epoch:      1,
	}, msg)
}

func TestWorkerManagerInitialization(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	manager := newWorkerManager(masterName, true, 1).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	ok, err := manager.IsInitialized(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	err = manager.HandleHeartbeat(&HeartbeatPingMessage{
		SendTime:     monotime.Now(),
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
}

func TestUpdateStatus(t *testing.T) {
	t.Parallel()

	manager := newWorkerManager(masterName, true, 1)
	err := manager.AddWorker(workerID1, executorNodeID1, WorkerStatusInit)
	require.NoError(t, err)

	info, ok := manager.GetWorkerInfo(workerID1)
	require.True(t, ok)
	require.Equal(t, WorkerStatusInit, info.status.Code)

	manager.UpdateStatus(&StatusUpdateMessage{
		WorkerID: workerID1,
		Status: WorkerStatus{
			Code:         WorkerStatusError,
			ErrorMessage: "fake",
			Ext:          "fake ext",
		},
	})

	handle := manager.GetWorkerHandle(workerID1)
	require.NotNil(t, handle)
	require.False(t, handle.IsTombStone())
	require.Equal(t, &WorkerStatus{
		Code:         WorkerStatusError,
		ErrorMessage: "fake",
		Ext:          "fake ext",
	}, handle.Status())
}

func TestWorkerTimedOut(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	msgSender := p2p.NewMockMessageSender()

	manager := newWorkerManager(masterName, true, 1).(*workerManagerImpl)
	manager.clock = clock.NewMock()
	manager.clock.(*clock.Mock).Set(time.Now())

	err := manager.AddWorker(workerID1, executorNodeID1, WorkerStatusInit)
	require.NoError(t, err)

	offlined, onlined := manager.Tick(ctx, msgSender)
	require.Empty(t, offlined)
	require.Empty(t, onlined)

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
	require.Equal(t, &WorkerStatus{
		Code: WorkerStatusError,
	}, handle.Status())
}
