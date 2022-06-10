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

package tp

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

type mockTrans struct {
	sendBuffer []*schedulepb.Message
	recvBuffer []*schedulepb.Message

	keepRecvBuffer bool
}

func newMockTrans() *mockTrans {
	return &mockTrans{
		sendBuffer: make([]*schedulepb.Message, 0),
		recvBuffer: make([]*schedulepb.Message, 0),
	}
}

func (m *mockTrans) Close() error {
	return nil
}

func (m *mockTrans) Send(ctx context.Context, msgs []*schedulepb.Message) error {
	m.sendBuffer = append(m.sendBuffer, msgs...)
	return nil
}

func (m *mockTrans) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
	if m.keepRecvBuffer {
		return m.recvBuffer, nil
	}
	messages := m.recvBuffer[:len(m.recvBuffer)]
	m.recvBuffer = make([]*schedulepb.Message, 0)
	return messages, nil
}

func TestCoordinatorSendMsgs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trans := newMockTrans()
	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "0",
		trans:     trans,
	}
	coord.captureM = newCaptureManager(model.ChangeFeedID{}, coord.revision, 0)
	coord.sendMsgs(
		ctx, []*schedulepb.Message{{To: "1", MsgType: schedulepb.MsgDispatchTableRequest}})

	coord.captureM.Captures["1"] = &CaptureStatus{Epoch: schedulepb.ProcessorEpoch{Epoch: "epoch"}}
	coord.sendMsgs(
		ctx, []*schedulepb.Message{{To: "1", MsgType: schedulepb.MsgDispatchTableRequest}})

	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:       coord.version,
			OwnerRevision: coord.revision,
		},
		From: "0", To: "1", MsgType: schedulepb.MsgDispatchTableRequest,
	}, {
		Header: &schedulepb.Message_Header{
			Version:        coord.version,
			OwnerRevision:  coord.revision,
			ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "epoch"},
		},
		From: "0", To: "1", MsgType: schedulepb.MsgDispatchTableRequest,
	}}, trans.sendBuffer)
}

func TestCoordinatorRecvMsgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	trans := &mockTrans{}
	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "0",
		trans:     trans,
	}

	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: coord.revision,
			},
			From: "1", To: coord.captureID, MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: schedulepb.OwnerRevision{Revision: 4},
			},
			From: "2", To: coord.captureID, MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: coord.revision,
			},
			From: "3", To: "lost", MsgType: schedulepb.MsgDispatchTableResponse,
		})

	msgs, err := coord.recvMsgs(ctx)
	require.NoError(t, err)
	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			OwnerRevision: coord.revision,
		},
		From: "1", To: "0", MsgType: schedulepb.MsgDispatchTableResponse,
	}}, msgs)
}

func TestCoordinatorHeartbeat(t *testing.T) {
	t.Parallel()

	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		MaxTaskConcurrency: 1,
	})
	trans := &mockTrans{}
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Two captures "a", "b".
	// Three tables 1 2 3.
	ctx := context.Background()
	currentTables := []model.TableID{1, 2, 3}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err := coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs := trans.sendBuffer
	require.Len(t, msgs, 2)
	require.NotNil(t, msgs[0].Heartbeat, msgs[0])
	require.NotNil(t, msgs[1].Heartbeat, msgs[1])
	require.False(t, coord.captureM.CheckAllCaptureInitialized())

	trans.recvBuffer = append(trans.recvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:                "a",
		From:              "b",
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{},
	})
	trans.recvBuffer = append(trans.recvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:      "a",
		From:    "a",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []schedulepb.TableStatus{
				{TableID: 1, State: schedulepb.TableStateReplicating},
				{TableID: 2, State: schedulepb.TableStateReplicating},
			},
		},
	})
	trans.sendBuffer = []*schedulepb.Message{}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	msgs = trans.sendBuffer
	require.Len(t, msgs, 1)
	require.EqualValues(t, 3, msgs[0].DispatchTableRequest.GetAddTable().TableID)
	require.Len(t, coord.replicationM.tables, 3)
}

func TestCoordinatorAddCapture(t *testing.T) {
	t.Parallel()
	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		MaxTaskConcurrency: 1,
	})
	trans := &mockTrans{}
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Two captures "a".
	// Three tables 1 2 3.
	coord.captureM.Captures["a"] = &CaptureStatus{State: CaptureStateInitialized}
	coord.captureM.initialized = true
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	msgs, err := coord.replicationM.HandleCaptureChanges(&captureChanges{
		Init: map[string][]schedulepb.TableStatus{
			"a": {
				{TableID: 1, State: schedulepb.TableStateReplicating},
				{TableID: 2, State: schedulepb.TableStateReplicating},
				{TableID: 3, State: schedulepb.TableStateReplicating},
			},
		},
	}, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, coord.replicationM.tables, 3)

	// Capture "b" is online, heartbeat, and then move one table to capture "b".
	ctx := context.Background()
	currentTables := []model.TableID{1, 2, 3}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs = trans.sendBuffer
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].Heartbeat, msgs[0])

	trans.recvBuffer = append(trans.recvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:                "a",
		From:              "b",
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{},
	})
	trans.sendBuffer = []*schedulepb.Message{}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs = trans.sendBuffer
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].DispatchTableRequest.GetAddTable(), msgs[0])
	require.True(t, msgs[0].DispatchTableRequest.GetAddTable().IsSecondary)
}

func TestCoordinatorRemoveCapture(t *testing.T) {
	t.Parallel()

	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		MaxTaskConcurrency: 1,
	})
	trans := &mockTrans{}
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Three captures "a" "b" "c".
	// Three tables 1 2 3.
	coord.captureM.Captures["a"] = &CaptureStatus{State: CaptureStateInitialized}
	coord.captureM.Captures["b"] = &CaptureStatus{State: CaptureStateInitialized}
	coord.captureM.Captures["c"] = &CaptureStatus{State: CaptureStateInitialized}
	coord.captureM.initialized = true
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	msgs, err := coord.replicationM.HandleCaptureChanges(&captureChanges{
		Init: map[string][]schedulepb.TableStatus{
			"a": {{TableID: 1, State: schedulepb.TableStateReplicating}},
			"b": {{TableID: 2, State: schedulepb.TableStateReplicating}},
			"c": {{TableID: 3, State: schedulepb.TableStateReplicating}},
		},
	}, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, coord.replicationM.tables, 3)

	// Capture "c" is removed, add table 3 to another capture.
	ctx := context.Background()
	currentTables := []model.TableID{1, 2, 3}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs = trans.sendBuffer
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].DispatchTableRequest.GetAddTable(), msgs[0])
	require.EqualValues(t, 3, msgs[0].DispatchTableRequest.GetAddTable().TableID)
}

func benchmarkCoordinator(
	b *testing.B,
	factory func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	),
) {
	log.SetLevel(zapcore.DPanicLevel)
	ctx := context.Background()
	size := 16384
	for total := 1; total <= size; total *= 2 {
		name, coord, currentTables, captures := factory(total)
		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				coord.poll(ctx, 0, currentTables, captures)
			}
		})
		b.StopTimer()
	}
}

func BenchmarkCoordinatorInit(b *testing.B) {
	benchmarkCoordinator(b, func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		schedulers := make(map[schedulerType]scheduler)
		schedulers[schedulerTypeBurstBalance] = newBurstBalanceScheduler()
		coord = &coordinator{
			trans:        &mockTrans{},
			schedulers:   schedulers,
			replicationM: newReplicationManager(10, model.ChangeFeedID{}),
			// Disable heartbeat.
			captureM: newCaptureManager(
				model.ChangeFeedID{}, schedulepb.OwnerRevision{}, math.MaxInt),
			tasksCounter: make(map[struct {
				scheduler string
				task      string
			}]int),
		}
		name = fmt.Sprintf("InitTable %d", total)
		return name, coord, currentTables, captures
	})
}

func BenchmarkCoordinatorHeartbeat(b *testing.B) {
	benchmarkCoordinator(b, func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		// Always heartbeat.
		captureM := newCaptureManager(
			model.ChangeFeedID{}, schedulepb.OwnerRevision{}, 0)
		captureM.initialized = true
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
			captureM.Captures[fmt.Sprint(i)] = &CaptureStatus{State: CaptureStateInitialized}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		schedulers := make(map[schedulerType]scheduler)
		schedulers[schedulerTypeBurstBalance] = newBurstBalanceScheduler()
		coord = &coordinator{
			trans:        &mockTrans{},
			schedulers:   schedulers,
			replicationM: newReplicationManager(10, model.ChangeFeedID{}),
			captureM:     captureM,
			tasksCounter: make(map[struct {
				scheduler string
				task      string
			}]int),
		}
		name = fmt.Sprintf("Heartbeat %d", total)
		return name, coord, currentTables, captures
	})
}

func BenchmarkCoordinatorHeartbeatResponse(b *testing.B) {
	benchmarkCoordinator(b, func(total int) (
		name string,
		coord *coordinator,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		// Disable heartbeat.
		captureM := newCaptureManager(
			model.ChangeFeedID{}, schedulepb.OwnerRevision{}, math.MaxInt)
		captureM.initialized = true
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
			captureM.Captures[fmt.Sprint(i)] = &CaptureStatus{State: CaptureStateInitialized}
		}
		replicationM := newReplicationManager(10, model.ChangeFeedID{})
		currentTables = make([]model.TableID, 0, total)
		heartbeatResp := make(map[model.CaptureID]*schedulepb.Message)
		for i := 0; i < total; i++ {
			tableID := int64(10000 + i)
			currentTables = append(currentTables, tableID)
			captureID := fmt.Sprint(i % captureCount)
			rep, err := newReplicationSet(tableID, 0, map[string]*schedulepb.TableStatus{
				captureID: {
					TableID: tableID,
					State:   schedulepb.TableStateReplicating,
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			replicationM.tables[tableID] = rep
			_, ok := heartbeatResp[captureID]
			if !ok {
				heartbeatResp[captureID] = &schedulepb.Message{
					Header:            &schedulepb.Message_Header{},
					From:              captureID,
					MsgType:           schedulepb.MsgHeartbeatResponse,
					HeartbeatResponse: &schedulepb.HeartbeatResponse{},
				}
			}
			heartbeatResp[captureID].HeartbeatResponse.Tables = append(
				heartbeatResp[captureID].HeartbeatResponse.Tables,
				schedulepb.TableStatus{
					TableID: tableID,
					State:   schedulepb.TableStateReplicating,
				})
		}
		recvMsgs := make([]*schedulepb.Message, 0, len(heartbeatResp))
		for _, resp := range heartbeatResp {
			recvMsgs = append(recvMsgs, resp)
		}
		trans := &mockTrans{
			recvBuffer:     recvMsgs,
			keepRecvBuffer: true,
		}
		schedulers := make(map[schedulerType]scheduler)
		schedulers[schedulerTypeBurstBalance] = newBurstBalanceScheduler()
		coord = &coordinator{
			trans:        trans,
			schedulers:   schedulers,
			replicationM: replicationM,
			captureM:     captureM,
			tasksCounter: make(map[struct {
				scheduler string
				task      string
			}]int),
		}
		name = fmt.Sprintf("HeartbeatResponse %d", total)
		return name, coord, currentTables, captures
	})
}
