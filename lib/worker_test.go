package lib

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	runtime "github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/metadata"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	_ Worker           = (*DefaultBaseWorker)(nil)
	_ runtime.Runnable = (Worker)(nil)
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
	worker.On("Tick", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval + 1*time.Second)
	worker.clock.(*clock.Mock).Add(defaultTimeoutConfig.workerHeartbeatInterval + 1*time.Second)

	var hbMsg *HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, HeartbeatPingTopic(masterName, workerID1))
		if ok {
			hbMsg = rawMsg.(*HeartbeatPingMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)
	require.Conditionf(t, func() (success bool) {
		return hbMsg.FromWorkerID == workerID1 && hbMsg.Epoch == 1
	}, "unexpected heartbeat %v", hbMsg)

	err = worker.UpdateStatus(ctx, WorkerStatus{Code: WorkerStatusNormal})
	require.NoError(t, err)

	var statusMsg *workerStatusUpdatedMessage
	require.Eventually(t, func() bool {
		err := worker.Poll(ctx)
		require.NoError(t, err)
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, workerStatusUpdatedTopic(masterName, workerID1))
		if ok {
			statusMsg = rawMsg.(*workerStatusUpdatedMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)
	require.Equal(t, &workerStatusUpdatedMessage{
		Epoch: 1,
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
			rawMsg, ok := worker.messageSender.TryPop(masterNodeName, HeartbeatPingTopic(masterName, workerID1))
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
		err = worker.messageHandlerManager.InvokeHandler(t, HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
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
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, HeartbeatPingTopic(masterName, workerID1))
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
	err = worker.messageHandlerManager.InvokeHandler(t, HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
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

func TestWorkerStatus(t *testing.T) {
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
		Code:     WorkerStatusNormal,
		ExtBytes: fastMarshalDummyStatus(t, 1),
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)
	worker.On("CloseImpl", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		err := worker.UpdateStatus(ctx, WorkerStatus{
			Code:     WorkerStatusNormal,
			ExtBytes: fastMarshalDummyStatus(t, 6),
		})
		if err == nil {
			return true
		}
		require.Regexp(t, ".*ErrWorkerUpdateStatusTryAgain.*", err.Error())
		return false
	}, 1*time.Second, 10*time.Millisecond)

	var status *workerStatusUpdatedMessage
	require.Eventually(t, func() bool {
		err := worker.Poll(ctx)
		require.NoError(t, err)
		rawStatus, ok := worker.messageSender.TryPop(masterNodeName, workerStatusUpdatedTopic(masterName, workerID1))
		if ok {
			status = rawStatus.(*workerStatusUpdatedMessage)
		}
		return ok
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, &workerStatusUpdatedMessage{
		Epoch: 1,
	}, status)

	workerMetaClient := NewWorkerMetadataClient(masterName, worker.metaKVClient)
	actualStatus, err := workerMetaClient.Load(ctx, workerID1)
	require.NoError(t, err)
	require.Equal(t, &WorkerStatus{
		Code:     WorkerStatusNormal,
		ExtBytes: fastMarshalDummyStatus(t, 6),
	},
		actualStatus)

	err = worker.Close(ctx)
	require.NoError(t, err)
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
