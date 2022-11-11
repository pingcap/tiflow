// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package framework

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework/config"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var _ Worker = (*DefaultBaseWorker)(nil) // _ runtime.Runnable = (Worker)(nil)

func putMasterMeta(ctx context.Context, t *testing.T, metaclient pkgOrm.Client, metaData *frameModel.MasterMeta) {
	err := metaclient.UpsertJob(ctx, metaData)
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
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval + 1*time.Second)
	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval + 1*time.Second)

	var hbMsg *frameModel.HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, frameModel.HeartbeatPingTopic(masterName))
		if ok {
			hbMsg = rawMsg.(*frameModel.HeartbeatPingMessage)
		}
		return ok
	}, time.Second*3, time.Millisecond*10)
	require.Conditionf(t, func() (success bool) {
		return hbMsg.FromWorkerID == workerID1 && hbMsg.Epoch == 1
	}, "unexpected heartbeat %v", hbMsg)

	err = worker.UpdateStatus(ctx, frameModel.WorkerStatus{State: frameModel.WorkerStateNormal})
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
		Status:      &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal},
	}, statusMsg)

	worker.On("CloseImpl").Return().Once()
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
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
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
		var hbMsg *frameModel.HeartbeatPingMessage
		require.Eventually(t, func() bool {
			rawMsg, ok := worker.messageSender.TryPop(masterNodeName, frameModel.HeartbeatPingTopic(masterName))
			if ok {
				hbMsg = rawMsg.(*frameModel.HeartbeatPingMessage)
			}
			return ok
		}, time.Second*3, time.Millisecond*10)

		require.Conditionf(t, func() (success bool) {
			return hbMsg.SendTime.Sub(lastHeartbeatSendTime) >= config.DefaultTimeoutConfig().WorkerHeartbeatInterval &&
				hbMsg.FromWorkerID == workerID1
		}, "last-send-time %s, cur-send-time %s", lastHeartbeatSendTime, hbMsg.SendTime)
		lastHeartbeatSendTime = hbMsg.SendTime

		pongMsg := &frameModel.HeartbeatPongMessage{
			SendTime:   hbMsg.SendTime,
			ReplyTime:  time.Now(),
			ToWorkerID: workerID1,
			Epoch:      1,
		}
		err = worker.messageHandlerManager.InvokeHandler(
			t, frameModel.HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
		require.NoError(t, err)
	}
}

func TestWorkerMasterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
	}, nil)
	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval)
	worker.clock.(*clock.Mock).Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval)
	var hbMsg *frameModel.HeartbeatPingMessage
	require.Eventually(t, func() bool {
		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, frameModel.HeartbeatPingTopic(masterName))
		if ok {
			hbMsg = rawMsg.(*frameModel.HeartbeatPingMessage)
		}
		return ok
	}, time.Second, time.Millisecond*10)
	require.Equal(t, workerID1, hbMsg.FromWorkerID)

	pongMsg := &frameModel.HeartbeatPongMessage{
		SendTime:   hbMsg.SendTime,
		ReplyTime:  time.Now(),
		ToWorkerID: workerID1,
		Epoch:      1,
	}
	err = worker.messageHandlerManager.InvokeHandler(t,
		frameModel.HeartbeatPongTopic(masterName, workerID1), masterNodeName, pongMsg)
	require.NoError(t, err)

	worker.clock.(*clock.Mock).Add(time.Second * 1)
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: executorNodeID3,
		Epoch:  2,
		State:  frameModel.MasterStateInit,
	})

	// Trigger a pull from Meta for the latest master's info.
	worker.clock.(*clock.Mock).Add(3 * config.DefaultTimeoutConfig().WorkerHeartbeatInterval)

	require.Eventually(t, func() bool {
		return worker.masterClient.MasterNode() == executorNodeID3
	}, time.Second*3, time.Millisecond*10)
}

func TestWorkerState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State:    frameModel.WorkerStateNormal,
		ExtBytes: fastMarshalDummyStatus(t, 1),
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)
	worker.On("CloseImpl", mock.Anything).Return()

	err := worker.Init(ctx)
	require.NoError(t, err)

	rawStatus, ok := worker.messageSender.TryPop(masterNodeName, statusutil.WorkerStatusTopic(masterName))
	require.True(t, ok)
	msg := rawStatus.(*statusutil.WorkerStatusMessage)
	checkWorkerStatusMsg(t, &statusutil.WorkerStatusMessage{
		Worker:      workerID1,
		MasterEpoch: 1,
		Status: &frameModel.WorkerStatus{
			State: frameModel.WorkerStateInit,
		},
	}, msg)

	err = worker.UpdateStatus(ctx, frameModel.WorkerStatus{
		State:    frameModel.WorkerStateNormal,
		ExtBytes: fastMarshalDummyStatus(t, 6),
	})
	require.NoError(t, err)

	rawStatus, ok = worker.messageSender.TryPop(masterNodeName, statusutil.WorkerStatusTopic(masterName))
	require.True(t, ok)
	msg = rawStatus.(*statusutil.WorkerStatusMessage)
	checkWorkerStatusMsg(t, &statusutil.WorkerStatusMessage{
		Worker:      workerID1,
		MasterEpoch: 1,
		Status: &frameModel.WorkerStatus{
			State:    frameModel.WorkerStateNormal,
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
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
	}, nil)
	worker.On("CloseImpl", mock.Anything).Return()

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

	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
	}, nil)
	worker.On("Tick", mock.Anything).Return(nil)
	worker.On("CloseImpl", mock.Anything).Return()

	ctx = runtime.NewRuntimeCtxWithSubmitTime(ctx, clock.ToMono(submitTime))
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
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.On("Tick", mock.Anything).
		Return(errors.New("fake error")).Once()
	worker.On("CloseImpl", mock.Anything).Return().Once()

	err = worker.Poll(ctx)
	require.Error(t, err)
	require.Regexp(t, ".*fake error.*", err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, worker.NotifyExit(ctx, err))
	}()

	for {
		// Make the heartbeat worker tick.
		worker.clock.(*clock.Mock).Add(time.Second)

		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, frameModel.HeartbeatPingTopic(masterName))
		if !ok {
			continue
		}
		msg := rawMsg.(*frameModel.HeartbeatPingMessage)
		if msg.IsFinished {
			pongMsg := &frameModel.HeartbeatPongMessage{
				SendTime:   msg.SendTime,
				ReplyTime:  time.Now(),
				ToWorkerID: workerID1,
				Epoch:      1,
				IsFinished: true,
			}

			err := worker.messageHandlerManager.InvokeHandler(
				t,
				frameModel.HeartbeatPongTopic(masterName, workerID1),
				masterNodeName,
				pongMsg,
			)
			require.NoError(t, err)
			break
		}
	}

	wg.Wait()
	worker.AssertExpectations(t)
}

func TestWorkerGracefulExitWhileTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.On("Tick", mock.Anything).
		Return(errors.New("fake error")).Once()
	worker.On("CloseImpl", mock.Anything).Return().Once()

	err = worker.Poll(ctx)
	require.Error(t, err)
	require.Regexp(t, ".*fake error.*", err)

	var (
		done atomic.Bool
		wg   sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer done.Store(true)
		err := worker.NotifyExit(ctx, err)
		require.Error(t, err)
		require.Regexp(t, "context deadline exceeded", err)
	}()

	for {
		// Make the heartbeat worker tick.
		worker.clock.(*clock.Mock).Add(time.Second)

		rawMsg, ok := worker.messageSender.TryPop(masterNodeName, frameModel.HeartbeatPingTopic(masterName))
		if !ok {
			continue
		}
		msg := rawMsg.(*frameModel.HeartbeatPingMessage)
		if msg.IsFinished {
			break
		}
	}

	for !done.Load() {
		worker.clock.(*clock.Mock).Add(time.Second)
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
}

func TestCloseBeforeInit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)

	worker.On("CloseImpl").Return()
	err := worker.Close(ctx)
	require.NoError(t, err)
}

func TestExitWithoutReturn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker := newMockWorkerImpl(workerID1, masterName)
	worker.clock = clock.NewMock()
	worker.clock.(*clock.Mock).Set(time.Now())
	putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
		ID:     masterName,
		NodeID: masterNodeName,
		Epoch:  1,
		State:  frameModel.MasterStateInit,
	})

	worker.On("InitImpl", mock.Anything).Return(nil)
	worker.On("Status").Return(frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
	}, nil)

	err := worker.Init(ctx)
	require.NoError(t, err)

	worker.On("Tick", mock.Anything).Return(nil)
	worker.On("CloseImpl", mock.Anything).Return().Once()

	_ = worker.DefaultBaseWorker.Exit(ctx, ExitReasonFailed, errors.New("Exit error"), nil)

	err = worker.Poll(ctx)
	require.Error(t, err)
	require.Regexp(t, "Exit error", err)
}

func checkWorkerStatusMsg(t *testing.T, expect, msg *statusutil.WorkerStatusMessage) {
	require.Equal(t, expect.Worker, msg.Worker)
	require.Equal(t, expect.MasterEpoch, msg.MasterEpoch)
	require.Equal(t, expect.Status.State, expect.Status.State)
	require.Equal(t, expect.Status.ErrorMsg, expect.Status.ErrorMsg)
	require.Equal(t, expect.Status.ExtBytes, expect.Status.ExtBytes)
}

func TestWorkerExit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		exitReason       ExitReason
		err              error
		extMsg           []byte
		expectedState    frameModel.WorkerState
		expectedErrorMsg string
		expectedExtMsg   []byte
	}{
		{
			exitReason:       ExitReasonFinished,
			err:              nil,
			extMsg:           []byte("test finished"),
			expectedState:    frameModel.WorkerStateFinished,
			expectedErrorMsg: "",
			expectedExtMsg:   []byte("test finished"),
		},
		{
			exitReason:       ExitReasonFinished,
			err:              errors.New("test finished with error"),
			extMsg:           []byte("test finished"),
			expectedState:    frameModel.WorkerStateFinished,
			expectedErrorMsg: "test finished with error",
			expectedExtMsg:   []byte("test finished"),
		},
		{
			exitReason:       ExitReasonCanceled,
			err:              nil,
			extMsg:           []byte("test canceled"),
			expectedState:    frameModel.WorkerStateStopped,
			expectedErrorMsg: "",
			expectedExtMsg:   []byte("test canceled"),
		},
		{
			exitReason:       ExitReasonCanceled,
			err:              errors.New("test canceled with error"),
			extMsg:           []byte("test canceled"),
			expectedState:    frameModel.WorkerStateStopped,
			expectedErrorMsg: "test canceled with error",
			expectedExtMsg:   []byte("test canceled"),
		},
		{
			exitReason:       ExitReasonFailed,
			err:              nil,
			extMsg:           []byte("test failed"),
			expectedState:    frameModel.WorkerStateError,
			expectedErrorMsg: "",
			expectedExtMsg:   []byte("test failed"),
		},
		{
			exitReason:       ExitReasonFailed,
			err:              errors.New("test failed with error"),
			extMsg:           []byte("test failed"),
			expectedState:    frameModel.WorkerStateError,
			expectedErrorMsg: "test failed with error",
			expectedExtMsg:   []byte("test failed"),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i, cs := range cases {
		worker := newMockWorkerImpl(fmt.Sprintf("worker-%d", i), masterName)
		worker.clock = clock.NewMock()
		worker.clock.(*clock.Mock).Set(time.Now())
		putMasterMeta(ctx, t, worker.metaClient, &frameModel.MasterMeta{
			ID:     masterName,
			NodeID: masterNodeName,
			Epoch:  1,
			State:  frameModel.MasterStateInit,
		})

		worker.On("InitImpl", mock.Anything).Return(nil)
		worker.On("Status").Return(frameModel.WorkerStatus{
			State: frameModel.WorkerStateNormal,
		}, nil)

		err := worker.Init(ctx)
		require.NoError(t, err)

		worker.On("Tick", mock.Anything).Return(nil)
		worker.On("CloseImpl", mock.Anything).Return().Once()

		err = worker.DefaultBaseWorker.Exit(ctx, cs.exitReason, cs.err, cs.extMsg)
		require.NoError(t, err)

		meta, err := worker.metaClient.GetWorkerByID(ctx, masterName, worker.ID())
		require.NoError(t, err)
		require.Equal(t, cs.expectedState, meta.State)
		require.Equal(t, cs.expectedErrorMsg, meta.ErrorMsg)
		require.Equal(t, cs.expectedExtMsg, meta.ExtBytes)

		require.NoError(t, worker.Close(ctx))
	}
}
