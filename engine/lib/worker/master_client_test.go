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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/lib/config"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

type masterClientTestHelper struct {
	Client        *MasterClient
	Meta          pkgOrm.Client
	MessageSender *p2p.MockMessageSender
	InitTime      time.Time
}

func newMasterClientTestHelper(
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
) *masterClientTestHelper {
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}

	initTime := time.Now()
	msgSender := p2p.NewMockMessageSender()
	masterCli := NewMasterClient(
		masterID,
		workerID,
		msgSender,
		meta,
		clock.ToMono(initTime))

	return &masterClientTestHelper{
		Client:        masterCli,
		Meta:          meta,
		MessageSender: msgSender,
		InitTime:      initTime,
	}
}

func (h *masterClientTestHelper) PopHeartbeat(t *testing.T) *libModel.HeartbeatPingMessage {
	node, _ := h.Client.getMasterInfo()
	masterID := h.Client.MasterID()

	msg, ok := h.MessageSender.TryPop(node, libModel.HeartbeatPingTopic(masterID))
	require.True(t, ok)

	return msg.(*libModel.HeartbeatPingMessage)
}

func TestMasterClientRefreshInfo(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	require.Equal(t, "executor-1", helper.Client.MasterNode())
	require.Equal(t, libModel.Epoch(1), helper.Client.Epoch())

	err = helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-2",
		Addr:       "192.168.0.2:1234",
		Epoch:      2,
	})
	require.NoError(t, err)

	err = helper.Client.SyncRefreshMasterInfo(context.Background())
	require.NoError(t, err)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, libModel.Epoch(2), epoch)
}

func TestMasterClientHeartbeat(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	clk := clock.NewMock()
	sendTime1 := helper.InitTime.Add(1 * time.Second)
	clk.Set(sendTime1)

	err = helper.Client.SendHeartBeat(context.Background(), clk, false)
	require.NoError(t, err)

	ping := helper.PopHeartbeat(t)
	require.Equal(t, &libModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime1),
		FromWorkerID: "worker-1",
		Epoch:        1,
		IsFinished:   false,
	}, ping)
	helper.Client.HandleHeartbeat("executor-1", &libModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
		IsFinished: false,
	})
	require.Equal(t, clock.ToMono(sendTime1), helper.Client.getLastMasterAckedPingTime())

	sendTime2 := helper.InitTime.Add(2 * time.Second)
	clk.Set(sendTime2)

	// Send a heartbeat with IsFinished set to true
	err = helper.Client.SendHeartBeat(context.Background(), clk, true)
	require.NoError(t, err)
	ping = helper.PopHeartbeat(t)
	require.Equal(t, &libModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime2),
		FromWorkerID: "worker-1",
		Epoch:        1,
		IsFinished:   true,
	}, ping)
	// The pong has IsFinished == false.
	helper.Client.HandleHeartbeat("executor-1", &libModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime2),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
		IsFinished: false,
	})
	require.False(t, helper.Client.IsMasterSideClosed())

	// The master has acknowledged the IsFinished message.
	helper.Client.HandleHeartbeat("executor-1", &libModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime2),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1,
		IsFinished: true,
	})
	require.True(t, helper.Client.IsMasterSideClosed())
}

func TestMasterClientHeartbeatMismatch(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      2,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	clk := clock.NewMock()
	sendTime1 := helper.InitTime.Add(1 * time.Second)
	clk.Set(sendTime1)

	err = helper.Client.SendHeartBeat(context.Background(), clk, false)
	require.NoError(t, err)

	ping := helper.PopHeartbeat(t)
	require.Equal(t, &libModel.HeartbeatPingMessage{
		SendTime:     clock.ToMono(sendTime1),
		FromWorkerID: "worker-1",
		Epoch:        2,
		IsFinished:   false,
	}, ping)

	// Epoch mismatch
	helper.Client.HandleHeartbeat("executor-1", &libModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1",
		Epoch:      1, // mismatch
		IsFinished: false,
	})
	require.NotEqual(t, clock.ToMono(sendTime1), helper.Client.getLastMasterAckedPingTime())

	// WorkerID mismatch
	helper.Client.HandleHeartbeat("executor-1", &libModel.HeartbeatPongMessage{
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
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-1", nodeID)
	require.Equal(t, libModel.Epoch(1), epoch)

	clk := clock.NewMock()
	sendTime1 := helper.InitTime.Add(1 * time.Second)
	clk.Set(sendTime1)

	helper.Client.HandleHeartbeat("executor-2", &libModel.HeartbeatPongMessage{
		SendTime:   clock.ToMono(sendTime1),
		ReplyTime:  time.Now(),
		ToWorkerID: "worker-1", // mismatch
		Epoch:      2,
		IsFinished: false,
	})

	nodeID, epoch = helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, libModel.Epoch(2), epoch)
}

func TestMasterClientTimeoutFromInit(t *testing.T) {
	helper := newMasterClientTestHelper("master-1", "worker-1")
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	// Add a delta more than the timeout threshould
	currentTime := helper.InitTime.Add(time.Minute)
	clk := clock.NewMock()
	clk.Set(currentTime)

	ok, err := helper.Client.CheckMasterTimeout(clk)
	// False indicates having timed out.
	require.False(t, ok)
	require.NoError(t, err)
}

func TestMasterClientCheckTimeoutRefreshMaster(t *testing.T) {
	// This tests asserts that when the master has not responded for more
	// than two heartbeat intervals, the MasterClient should try to refresh
	// master info automatically.

	helper := newMasterClientTestHelper("master-1", "worker-1")
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	// Add a delta more than two heartbeat intervals,
	// but less than the timeout threshold.
	currentTime := helper.InitTime.Add(config.DefaultTimeoutConfig().WorkerHeartbeatInterval * 3)
	clk := clock.NewMock()
	clk.Set(currentTime)

	// Simulates a master failover
	err = helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-2",
		Addr:       "192.168.0.2:1234",
		Epoch:      2,
	})
	require.NoError(t, err)

	ok, err := helper.Client.CheckMasterTimeout(clk)
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
	require.Equal(t, libModel.Epoch(2), epoch)
}

func TestMasterClientSendHeartbeatRefreshMaster(t *testing.T) {
	// Tests that if an executor is not found when trying to send
	// a heartbeat, a master info refresh is triggered.

	helper := newMasterClientTestHelper("master-1", "worker-1")
	err := helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	err = helper.Client.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	helper.MessageSender.MarkNodeOffline("executor-1")
	err = helper.Meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-2",
		Addr:       "192.168.0.2:1234",
		Epoch:      2,
	})
	require.NoError(t, err)

	err = helper.Client.SendHeartBeat(context.Background(), clock.New(), false)
	require.NoError(t, err)

	// Since the refreshing happens asynchronously,
	// we need to wait for a while.
	require.Eventually(t, func() bool {
		_, epoch := helper.Client.getMasterInfo()
		return epoch > 1
	}, 1*time.Second, 5*time.Millisecond)

	nodeID, epoch := helper.Client.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, libModel.Epoch(2), epoch)
}
