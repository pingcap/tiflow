package lib

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func putMasterMeta(ctx context.Context, t *testing.T, metaclient metadata.MetaKV, metaData *MasterMetaKVData) {
	masterKey := adapter.MasterMetaKey.Encode(masterName)
	masterInfoBytes, err := json.Marshal(metaData)
	require.NoError(t, err)
	_, err = metaclient.Put(ctx, masterKey, string(masterInfoBytes))
	require.NoError(t, err)
}

func TestWorkerInitAndClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:          masterName,
		NodeID:      masterNodeName,
		Epoch:       1,
		Initialized: true,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(WorkerStatus{
		Code: WorkerStatusNormal,
	}, nil)
	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval + 1*time.Second)
	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval + 1*time.Second)

	var hbMsg *HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, HeartbeatPingTopic(masterName))
		if ok {
			hbMsg = rawMsg.(*HeartbeatPingMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)
	require.Conditionf(t, func() (success bool) {
		return hbMsg.FromWorkerID == workerID1 && hbMsg.Epoch == 1
	}, "unexpected heartbeat %v", hbMsg)

	var statusMsg *StatusUpdateMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, StatusUpdateTopic(masterName))
		if ok {
			statusMsg = rawMsg.(*StatusUpdateMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)
	require.Equal(t, &StatusUpdateMessage{
		WorkerID: workerID1,
		Status: WorkerStatus{
			Code: WorkerStatusNormal,
		},
	}, statusMsg)

	worker.On("CloseImpl").Return(nil)
	err = worker.Close(ctx)
	require.NoError(t, err)
}

const (
	heartbeatPingPongTestRepeatTimes = 100
)

func TestWorkerHeartbeatPingPong(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:          masterName,
		NodeID:      masterNodeName,
		Epoch:       1,
		Initialized: true,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(WorkerStatus{
		Code: WorkerStatusNormal,
	}, nil)
	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval)

	worker.On("Tick", mock.Anything).Return(nil)
	var lastHeartbeatSendTime clock.MonotonicTime
	for i := 0; i < heartbeatPingPongTestRepeatTimes; i++ {
		err := worker.Poll(ctx)
		require.NoError(t, err)

		worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval)
		var hbMsg *HeartbeatPingMessage
		require.Eventually(t, func() bool {
			rawMsg, ok := worker.messageSender.TryPop(masterNodeName, HeartbeatPingTopic(masterName))
			if ok {
				hbMsg = rawMsg.(*HeartbeatPingMessage)
			}
			return ok
		}, time.Second, time.Millisecond*10)

		require.Conditionf(t, func() (success bool) {
			return hbMsg.SendTime.Sub(lastHeartbeatSendTime) >= defaultTimeoutConfig.workerHeartbeatInterval
		}, "last-send-time %s, cur-send-time %s", lastHeartbeatSendTime, hbMsg.SendTime)
		lastHeartbeatSendTime = hbMsg.SendTime

		pongMsg := &HeartbeatPongMessage{
			SendTime:   hbMsg.SendTime,
			ReplyTime:  time.Now(),
			ToWorkerID: workerID1,
			Epoch:      1,
		}
		err = worker.messageHandlerManager.InvokeHandler(t, HeartbeatPongTopic(masterName), masterNodeName, pongMsg)
		require.NoError(t, err)
	}
}

func TestWorkerMasterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:          masterName,
		NodeID:      masterNodeName,
		Epoch:       1,
		Initialized: true,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(WorkerStatus{
		Code: WorkerStatusNormal,
	}, nil)
	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval)
	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval)
	var hbMsg *HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, HeartbeatPingTopic(masterName))
		if ok {
			hbMsg = rawMsg.(*HeartbeatPingMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)

	pongMsg := &HeartbeatPongMessage{
		SendTime:   hbMsg.SendTime,
		ReplyTime:  time.Now(),
		ToWorkerID: workerID1,
		Epoch:      1,
	}
	err = worker.messageHandlerManager.InvokeHandler(t, HeartbeatPongTopic(masterName), masterNodeName, pongMsg)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(time.Second * 1)
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:          masterName,
		NodeID:      executorNodeID3,
		Epoch:       2,
		Initialized: true,
	})

	worker.On("OnMasterFailover", mock.Anything).Return(nil)
	// Trigger a pull from Meta for the latest master's info.
	worker.clock.(*clock.Mock).Add(3 * defaultTimeoutConfig.workerHeartbeatInterval)

	require.Eventually(t, func() bool {
		return worker.failoverCount.Load() == 1
	}, time.Second*1, time.Millisecond*10)
}

func TestWorkerSuicide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:          masterName,
		NodeID:      masterNodeName,
		Epoch:       1,
		Initialized: true,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(WorkerStatus{
		Code: WorkerStatusNormal,
	}, nil)
	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.On("Tick", mock.Anything).Return(nil)
	err = worker.Poll(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration)
	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerTimeoutDuration)

	var exitErr error
	require.Eventually(t, func() bool {
		exitErr = worker.Poll(ctx)
		return exitErr != nil
	}, time.Second*1, time.Millisecond*10)

	require.Regexp(t, ".*Suicide.*", exitErr.Error())
}
