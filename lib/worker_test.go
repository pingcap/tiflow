package lib

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	runtime "github.com/hanfei1991/microcosm/executor/worker"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	_ Worker           = (*DefaultBaseWorker)(nil)
	_ runtime.Runnable = (Worker)(nil)
)

func putMasterMeta(ctx context.Context, t *testing.T, metaclient metaclient.KVClient, metaData *MasterMetaKVData) {
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
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)

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

	err = worker.UpdateStatus(ctx, libModel.WorkerStatus{Code: libModel.WorkerStatusNormal})
	require.NoError(t, err)

	var statusMsg *statusutil.WorkerStatusMessage
	require.Eventually(t, func() bool {
		err := worker.Poll(ctx)
		require.NoError(t, err)
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, statusutil.WorkerStatusTopic(masterName))
		if ok {
			statusMsg = rawMsg.(*statusutil.WorkerStatusMessage)
		}
		return !ok
	}, time.Second, time.Millisecond*10)
	require.Equal(t, &statusutil.WorkerStatusMessage{
		Worker:      workerID1,
		MasterEpoch: 1,
		Status:      &libModel.WorkerStatus{Code: libModel.WorkerStatusNormal},
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
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
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
			return hbMsg.SendTime.Sub(lastHeartbeatSendTime) >= defaultTimeoutConfig.workerHeartbeatInterval &&
				hbMsg.FromWorkerID == workerID1
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
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
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
	require.Equal(t, workerID1, hbMsg.FromWorkerID)

	pongMsg := &HeartbeatPongMessage{
		SendTime:   hbMsg.SendTime,
		ReplyTime:  time.Now(),
		ToWorkerID: workerID1,
		Epoch:      1,
	}
	err = worker.messageHandlerManager.InvokeHandler(t, HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
	require.NoError(t, err)
	masterAckedTimeAfterPing := worker.masterClient.getLastMasterAckedPingTime()

	worker.clock.(*clock.Mock).Add(time.Second * 1)
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:         masterName,
		NodeID:     executorNodeID3,
		Epoch:      2,
		StatusCode: MasterStatusInit,
	})

	worker.On("OnMasterFailover", mock.Anything).Return(nil)
	// Trigger a pull from Meta for the latest master's info.
	worker.clock.(*clock.Mock).Add(3 * defaultTimeoutConfig.workerHeartbeatInterval)

	require.Eventually(t, func() bool {
		return worker.failoverCount.Load() == 1
	}, time.Second*1, time.Millisecond*10)

	masterAckedTimeAfterFailover := worker.masterClient.getLastMasterAckedPingTime()
	require.Greater(t, masterAckedTimeAfterFailover, masterAckedTimeAfterPing)
}

func TestWorkerStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaKVClient, &MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code:     libModel.WorkerStatusNormal,
		ExtBytes: fastMarshalDummyStatus(t, 1),
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)
	worker.On("CloseImpl", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	rawStatus, ok := worker.messageSender.TryPop(masterNodeName, statusutil.WorkerStatusTopic(masterName))
	require.True(t, ok)
	msg := rawStatus.(*statusutil.WorkerStatusMessage)
	require.Equal(t, &statusutil.WorkerStatusMessage{
		Worker:      workerID1,
		MasterEpoch: 1,
		Status: &libModel.WorkerStatus{
			Code: libModel.WorkerStatusInit,
		},
	}, msg)

	err = worker.UpdateStatus(ctx, libModel.WorkerStatus{
		Code:     libModel.WorkerStatusNormal,
		ExtBytes: fastMarshalDummyStatus(t, 6),
	})
	require.NoError(t, err)

	rawStatus, ok = worker.messageSender.TryPop(masterNodeName, statusutil.WorkerStatusTopic(masterName))
	require.True(t, ok)
	msg = rawStatus.(*statusutil.WorkerStatusMessage)
	require.Equal(t, &statusutil.WorkerStatusMessage{
		Worker:      workerID1,
		MasterEpoch: 1,
		Status: &libModel.WorkerStatus{
			Code:     libModel.WorkerStatusNormal,
			ExtBytes: fastMarshalDummyStatus(t, 6),
		},
	}, msg)

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
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
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

func TestCloseBeforeInit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)

	worker.On("CloseImpl").Return(nil)
	err := worker.Close(ctx)
	require.NoError(t, err)
}
