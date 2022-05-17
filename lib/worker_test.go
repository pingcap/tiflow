package lib

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	runtime "github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib/config"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/pkg/clock"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
)

var (
	_ Worker           = (*DefaultBaseWorker)(nil)
	_ runtime.Runnable = (Worker)(nil)
)

func putMasterMeta(ctx context.Context, t *testing.T, metaclient pkgOrm.Client, metaData *libModel.MasterMetaKVData) {
	// FIXME: current backend mock db is not support unique index
	if _, err := metaclient.GetJobByID(ctx, metaData.ID); err != nil {
		err := metaclient.UpsertJob(ctx, metaData)
		require.NoError(t, err)
		return
	}

	err := metaclient.UpdateJob(ctx, metaData)
	require.NoError(t, err)
}

func fastMarshalDummyStatus(t *testing.T, val int) []byte {
	dummySt := &dummyStatus{Val: val}
	bytes, err := dummySt.Marshal()
	require.NoError(t, err)
	return bytes
}

func TestWorkerInitAndClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval + 1*time.Second)
	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval + 1*time.Second)

	var hbMsg *libModel.HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, libModel.HeartbeatPingTopic(masterName))
		if ok {
			hbMsg = rawMsg.(*libModel.HeartbeatPingMessage)
		}
		return ok
	}, time.Second*3, time.Millisecond*10)
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
	checkWorkerStatusMsg(t, &statusutil.WorkerStatusMessage{
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
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval)

	worker.On("Tick", mock.Anything).Return(nil)
	var lastHeartbeatSendTime clock.MonotonicTime
	for i := 0; i < heartbeatPingPongTestRepeatTimes; i++ {
		err := worker.Poll(ctx)
		require.NoError(t, err)

		worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval)
		var hbMsg *libModel.HeartbeatPingMessage
		require.Eventually(t, func() bool {
			rawMsg, ok := worker.messageSender.TryPop(masterNodeName, libModel.HeartbeatPingTopic(masterName))
			if ok {
				hbMsg = rawMsg.(*libModel.HeartbeatPingMessage)
			}
			return ok
		}, time.Second*3, time.Millisecond*10)

		require.Conditionf(t, func() (success bool) {
			return hbMsg.SendTime.Sub(lastHeartbeatSendTime) >= config.DefaultTimeoutConfig().WorkerHeartbeatInterval &&
				hbMsg.FromWorkerID == workerID1
		}, "last-send-time %s, cur-send-time %s", lastHeartbeatSendTime, hbMsg.SendTime)
		lastHeartbeatSendTime = hbMsg.SendTime

		pongMsg := &libModel.HeartbeatPongMessage{
			SendTime:   hbMsg.SendTime,
			ReplyTime:  time.Now(),
			ToWorkerID: workerID1,
			Epoch:      1,
		}
		err = worker.messageHandlerManager.InvokeHandler(
			t, libModel.HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
		require.NoError(t, err)
	}
}

func TestWorkerMasterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)
	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval)
	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval)
	var hbMsg *libModel.HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, libModel.HeartbeatPingTopic(masterName))
		if ok {
			hbMsg = rawMsg.(*libModel.HeartbeatPingMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)
	require.Equal(t, workerID1, hbMsg.FromWorkerID)

	pongMsg := &libModel.HeartbeatPongMessage{
		SendTime:   hbMsg.SendTime,
		ReplyTime:  time.Now(),
		ToWorkerID: workerID1,
		Epoch:      1,
	}
	err = worker.messageHandlerManager.InvokeHandler(t,
		libModel.HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(time.Second * 1)
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     executorNodeID3,
		Epoch:      2,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("OnMasterFailover", mock.Anything).Return(nil)
	// Trigger a pull from Meta for the latest master's info.
	worker.clock.(*clock.Mock).Add(3 * config.DefaultTimeoutConfig().WorkerHeartbeatInterval)

	require.Eventually(t, func() bool {
		return worker.failoverCount.Load() == 1
	}, time.Second*3, time.Millisecond*10)
}

func TestWorkerStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
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
	checkWorkerStatusMsg(t, &statusutil.WorkerStatusMessage{
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
	checkWorkerStatusMsg(t, &statusutil.WorkerStatusMessage{
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
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
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

	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerTimeoutDuration)
	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerTimeoutDuration)

	var exitErr error
	require.Eventually(t, func() bool {
		exitErr = worker.Poll(ctx)
		return exitErr != nil
	}, time.Second*1, time.Millisecond*10)

	require.Regexp(t, ".*Suicide.*", exitErr.Error())
}

func TestWorkerSuicideAfterRuntimeDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	submitTime := time.Now()
	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(submitTime.Add(worker.timeoutConfig.WorkerTimeoutDuration * 2))

	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)

	ctx = runtime.NewRuntimeCtxWithSubmitTime(ctx, submitTime)
	err := worker.Init(ctx)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	worker.clock.(*clock.Mock).Add(worker.timeoutConfig.WorkerHeartbeatInterval)
	worker.clock.(*clock.Mock).Add(1 * time.Second)

	var pollErr error
	require.Eventually(t, func() bool {
		pollErr = worker.Poll(ctx)
		return pollErr != nil
	}, 2*time.Second, 10*time.Millisecond)
	require.Error(t, pollErr)
	require.Regexp(t, ".*Suicide.*", pollErr)
}

func TestWorkerGracefulExit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.On("Tick", mock.Anything).
		Return(errors.New("fake error")).Once()

	for {
		err := worker.Poll(ctx)
		require.NoError(t, err)

		// Make the heartbeat worker tick.
		worker.clock.(*clock.Mock).Add(time.Second)

		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, libModel.HeartbeatPingTopic(masterName))
		if !ok {
			continue
		}
		msg := rawMsg.(*libModel.HeartbeatPingMessage)
		if msg.IsFinished {
			pongMsg := &libModel.HeartbeatPongMessage{
				SendTime:   msg.SendTime,
				ReplyTime:  time.Now(),
				ToWorkerID: workerID1,
				Epoch:      1,
				IsFinished: true,
			}

			err := worker.messageHandlerManager.InvokeHandler(
				t,
				libModel.HeartbeatPongTopic(masterName, workerID1),
				masterNodeName,
				pongMsg,
			)
			require.NoError(t, err)
			break
		}
	}

	err = worker.Poll(ctx)
	require.Error(t, err)
	require.Regexp(t, ".*fake error.*", err)
}

func TestWorkerGracefulExitWhileTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &libModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}, nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.On("Tick", mock.Anything).
		Return(errors.New("fake error")).Once()

	for {
		err := worker.Poll(ctx)
		require.NoError(t, err)

		// Make the heartbeat worker tick.
		worker.clock.(*clock.Mock).Add(time.Second)

		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, libModel.HeartbeatPingTopic(masterName))
		if !ok {
			continue
		}
		msg := rawMsg.(*libModel.HeartbeatPingMessage)
		if msg.IsFinished {
			break
		}
	}

	for {
		err := worker.Poll(ctx)
		worker.clock.(*clock.Mock).Add(time.Second)

		if err != nil {
			require.Regexp(t, ".*fake error.*", err)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCloseBeforeInit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)

	worker.On("CloseImpl").Return(nil)
	err := worker.Close(ctx)
	require.NoError(t, err)
}

func checkWorkerStatusMsg(t *testing.T, expect, msg *statusutil.WorkerStatusMessage) {
	require.Equal(t, expect.Worker, msg.Worker)
	require.Equal(t, expect.MasterEpoch, msg.MasterEpoch)
	require.Equal(t, expect.Status.Code, expect.Status.Code)
	require.Equal(t, expect.Status.ErrorMessage, expect.Status.ErrorMessage)
	require.Equal(t, expect.Status.ExtBytes, expect.Status.ExtBytes)
}
