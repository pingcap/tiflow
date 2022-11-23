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

package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/framework/config"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	ormMock "github.com/pingcap/tiflow/engine/pkg/orm/mock"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

type masterClientTestHelper struct {
	Client        *MasterClient
	Meta          pkgOrm.Client
	MessageSender *p2p.MockMessageSender
	InitTime      time.Time
	Clk           *clock.Mock

	wg         sync.WaitGroup
	closeErrCh chan error
}

func newMasterClientTestHelper(
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
) *masterClientTestHelper {
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	epoch, err := meta.GenEpoch(ctx)
	if err != nil {
		panic(err)
	}

	initTime := time.Now()
	msgSender := p2p.NewMockMessageSender()
	clk := clock.NewMock()
	masterCli := NewMasterClient(
		masterID,
		workerID,
		msgSender,
		meta,
		clock.ToMono(initTime),
		clk,
		epoch,
	)

	return &masterClientTestHelper{
		Client:        masterCli,
		Meta:          meta,
		MessageSender: msgSender,
		InitTime:      initTime,
		Clk:           clk,
		closeErrCh:    make(chan error, 1),
	}
}

func (h *masterClientTestHelper) PopHeartbeat(t *testing.T) *frameModel.HeartbeatPingMessage {
	node, _ := h.Client.getMasterInfo()
	masterID := h.Client.MasterID()

	msg, ok := h.MessageSender.TryPop(node, frameModel.HeartbeatPingTopic(masterID))
	require.True(t, ok)

	return msg.(*frameModel.HeartbeatPingMessage)
}

func (h *masterClientTestHelper) SimulateWorkerClose(timeout time.Duration) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		h.closeErrCh <- h.Client.WaitClosed(ctx)
	}()
}

func (h *masterClientTestHelper) WaitWorkerClosed(t *testing.T) error {
	h.wg.Wait()

	// The timeout is for terminating the test when there is a serious problem,
	// so that the test does not run for 10 minutes and then times out.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.FailNow()
	case err := <-h.closeErrCh:
		return err
	}
	return nil
}

func TestMasterClientRefreshInfo(t *testing.T) {
	masterID := "master-refresh-info"
	workerID := "worker-refresh-info"
	helper := newMasterClientTestHelper(masterID, workerID)
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     masterID,
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	require.Equal(t, "executor-1", helper.Client.MasterNode())
	require.Equal(t, frameModel.Epoch(1), helper.Client.Epoch())

	err = helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     masterID,
		State:  frameModel.MasterStateInit,
		NodeID: "executor-2",
		Addr:   "192.168.0.2:1234",
		Epoch:  2,
	})
	require.NoError(t, err)

	err = helper.Client.SyncRefreshMasterInfo(context.Background())
	require.NoError(t, err)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, frameModel.Epoch(2), epoch)
}

func TestMasterClientHeartbeat(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	sendTime1 := helper.InitTime.Add(1 * time.Second)
	helper.Clk.Set(sendTime1)

	err = helper.Client.SendHeartBeat(context.Background())
	require.NoError(t, err)

	ping := helper.PopHeartbeat(t)
	require.Equal(t, &frameModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime1),
		FromWorkerID: "worker-1",
		Epoch:        1,
		WorkerEpoch:  helper.Client.WorkerEpoch(),
		IsFinished:   false,
	}, ping)
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
		IsFinished: false,
	})
	require.Equal(t, clock.ToMono(sendTime1), helper.Client.getLastMasterAckedPingTime())

	sendTime2 := helper.InitTime.Add(2 * time.Second)
	helper.Clk.Set(sendTime2)

	helper.SimulateWorkerClose(1 * time.Second)

	// Wait for the goroutine running WaitClose() to launch.
	time.Sleep(100 * time.Millisecond)

	// Send a heartbeat with IsFinished set to true
	err = helper.Client.SendHeartBeat(context.Background())
	require.NoError(t, err)
	ping = helper.PopHeartbeat(t)
	require.Equal(t, &frameModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime2),
		FromWorkerID: "worker-1",
		Epoch:        1,
		WorkerEpoch:  helper.Client.WorkerEpoch(),
		IsFinished:   true,
	}, ping)
	// The pong has IsFinished == false.
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime2),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
		IsFinished: false,
	})
	require.False(t, helper.Client.IsMasterSideClosed())

	// The master has acknowledged the IsFinished message.
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime2),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
		IsFinished: true,
	})
	require.True(t, helper.Client.IsMasterSideClosed())
	require.NoError(t, helper.WaitWorkerClosed(t))
}

func TestMasterClientHeartbeatMismatch(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  2,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	sendTime1 := helper.InitTime.Add(1 * time.Second)
	helper.Clk.Set(sendTime1)

	err = helper.Client.SendHeartBeat(context.Background())
	require.NoError(t, err)

	ping := helper.PopHeartbeat(t)
	require.Equal(t, &frameModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime1),
		FromWorkerID: "worker-1",
		Epoch:        2,
		WorkerEpoch:  helper.Client.WorkerEpoch(),
		IsFinished:   false,
	}, ping)

	// Epoch mismatch
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1, // mismatch
		IsFinished: false,
	})
	require.NotEqual(t, clock.ToMono(sendTime1), helper.Client.getLastMasterAckedPingTime())

	// WorkerID mismatch
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-2", // mismatch
		Epoch:      2,
		IsFinished: false,
	})
	require.NotEqual(t, clock.ToMono(sendTime1), helper.Client.getLastMasterAckedPingTime())
}

func TestMasterClientHeartbeatLargerEpoch(t *testing.T) {
	// In this test case, we test the ability for the MasterClient to
	// update its maintained masterInfo automatically, if it receives a heartbeat
	// with a larger Epoch.

	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-1", nodeID)
	require.Equal(t, frameModel.Epoch(1), epoch)

	sendTime1 := helper.InitTime.Add(1 * time.Second)
	helper.Clk.Set(sendTime1)

	helper.Client.HandleHeartbeat("executor-2", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1", // mismatch
		Epoch:      2,
		IsFinished: false,
	})

	nodeID, epoch = helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, frameModel.Epoch(2), epoch)
}

func TestMasterClientTimeoutFromInit(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	// Add a delta more than the timeout threshould
	currentTime := helper.InitTime.Add(time.Minute)
	helper.Clk.Set(currentTime)

	ok, err := helper.Client.CheckMasterTimeout()
	// False indicates having timed out.
	require.False(t, ok)
	require.NoError(t, err)
}

func TestMasterClientCheckTimeoutRefreshMaster(t *testing.T) {
	// This tests asserts that when the master has not responded for more
	// than two heartbeat intervals, the MasterClient should try to refresh
	// master info automatically.

	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	// Add a delta more than two heartbeat intervals,
	// but less than the timeout threshold.
	currentTime := helper.InitTime.Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval * 3)
	helper.Clk.Set(currentTime)

	// Simulates a master failover
	err = helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-2",
		Addr:   "192.168.0.2:1234",
		Epoch:  2,
	})
	require.NoError(t, err)

	ok, err := helper.Client.CheckMasterTimeout()
	require.True(t, ok) // not timed out
	require.NoError(t, err)

	// Since the refreshing happens asynchronously,
	// we need to wait for a while.
	require.Eventually(t, func() bool {
		_, epoch := helper.Client.getMasterInfo()
		return epoch > 1
	}, 1*time.Second, 5*time.Millisecond)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, frameModel.Epoch(2), epoch)
}

func TestMasterClientSendHeartbeatRefreshMaster(t *testing.T) {
	// Tests that if an executor is not found when trying to send
	// a heartbeat, a master info refresh is triggered.

	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	helper.MessageSender.MarkNodeOffline("executor-1")
	err = helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-2",
		Addr:   "192.168.0.2:1234",
		Epoch:  2,
	})
	require.NoError(t, err)

	err = helper.Client.SendHeartBeat(context.Background())
	require.NoError(t, err)

	// Since the refreshing happens asynchronously,
	// we need to wait for a while.
	require.Eventually(t, func() bool {
		_, epoch := helper.Client.getMasterInfo()
		return epoch > 1
	}, 1*time.Second, 5*time.Millisecond)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, frameModel.Epoch(2), epoch)
}

func TestMasterClientHeartbeatStalePong(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	defer helper.Meta.Close()
	err := helper.Meta.UpsertJob(context.Background(), &frameModel.MasterMeta{
		ID:     "master-1",
		State:  frameModel.MasterStateInit,
		NodeID: "executor-1",
		Addr:   "192.168.0.1:1234",
		Epoch:  1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	clk := helper.Clk
	sendTime1 := helper.InitTime.Add(1 * time.Second)
	clk.Set(sendTime1)

	err = helper.Client.SendHeartBeat(context.Background())
	require.NoError(t, err)

	ping := helper.PopHeartbeat(t)
	require.Equal(t, &frameModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime1),
		FromWorkerID: "worker-1",
		Epoch:        1,
		WorkerEpoch:  helper.Client.WorkerEpoch(),
		IsFinished:   false,
	}, ping)

	sendTimeOld := helper.InitTime.Add(-30 * time.Second)
	// master_client receives a stale pong heartbeat
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTimeOld),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
	})
	require.Equal(t,
		time.Duration(clock.ToMono(helper.InitTime)),
		helper.Client.lastMasterAckedPingTime.Load())

	// master_client receives a fresh pong heartbeat
	helper.Client.HandleHeartbeat("executor-1", &frameModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
	})
	require.Equal(t,
		time.Duration(clock.ToMono(sendTime1)),
		helper.Client.lastMasterAckedPingTime.Load())
}

func TestAsyncReloadMasterInfoFailed(t *testing.T) {
	t.Parallel()

	mockframeMetaClient := ormMock.NewMockClient(gomock.NewController(t))
	masterID := "test-async-reload-failed"
	mc := &MasterClient{
		masterID:        masterID,
		frameMetaClient: mockframeMetaClient,
	}

	ctx := context.Background()
	mockErr := errors.New("mock get job error")
	mockframeMetaClient.EXPECT().
		GetJobByID(gomock.Any(), masterID).Times(1).
		Return(nil, mockErr)
	ch := mc.asyncReloadMasterInfo(ctx)
	err := <-ch
	require.EqualError(t, err, mockErr.Error())
}
