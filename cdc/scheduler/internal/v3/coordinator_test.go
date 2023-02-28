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

package v3

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/transport"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

func TestCoordinatorSendMsgs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trans := transport.NewMockTrans()
	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "0",
		trans:     trans,
	}
	cfg := config.NewDefaultSchedulerConfig()
	coord.captureM = member.NewCaptureManager("", model.ChangeFeedID{}, coord.revision, cfg)
	coord.sendMsgs(
		ctx, []*schedulepb.Message{{To: "1", MsgType: schedulepb.MsgDispatchTableRequest}})

	coord.captureM.Captures["1"] = &member.CaptureStatus{
		Epoch: schedulepb.ProcessorEpoch{Epoch: "epoch"},
	}
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
	}}, trans.SendBuffer)
}

func TestCoordinatorRecvMsgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	trans := transport.NewMockTrans()
	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "0",
		trans:     trans,
	}

	trans.RecvBuffer = append(trans.RecvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: coord.revision,
			},
			From: "1", To: coord.captureID, MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.RecvBuffer = append(trans.RecvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: schedulepb.OwnerRevision{Revision: 4},
			},
			From: "2", To: coord.captureID, MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.RecvBuffer = append(trans.RecvBuffer,
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
		CollectStatsTick:   math.MaxInt,
		MaxTaskConcurrency: 1,
		AddTableBatchSize:  50,
	})
	trans := transport.NewMockTrans()
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Two captures "a", "b".
	// Three tables 1 2 3.
	ctx := context.Background()
	currentTables := []model.TableID{1, 2, 3}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err := coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs := trans.SendBuffer
	require.Len(t, msgs, 2)
	require.NotNil(t, msgs[0].Heartbeat, msgs[0])
	require.NotNil(t, msgs[1].Heartbeat, msgs[1])
	require.False(t, coord.captureM.CheckAllCaptureInitialized())

	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:                "a",
		From:              "b",
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{},
	})
	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:      "a",
		From:    "a",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{
				{TableID: 1, State: tablepb.TableStateReplicating},
				{TableID: 2, State: tablepb.TableStateReplicating},
			},
		},
	})
	trans.SendBuffer = []*schedulepb.Message{}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	msgs = trans.SendBuffer
	require.Len(t, msgs, 1)
	// Basic scheduler, make sure all tables get replicated.
	require.EqualValues(t, 3, msgs[0].DispatchTableRequest.GetAddTable().TableID)
	require.Len(t, coord.replicationM.GetReplicationSetForTests(), 3)
}

func TestCoordinatorAddCapture(t *testing.T) {
	t.Parallel()
	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		CollectStatsTick:   math.MaxInt,
		MaxTaskConcurrency: 1,
	})
	trans := transport.NewMockTrans()
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Two captures "a".
	// Three tables 1 2 3.
	coord.captureM.Captures["a"] = &member.CaptureStatus{State: member.CaptureStateInitialized}
	coord.captureM.SetInitializedForTests(true)
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	init := map[string][]tablepb.TableStatus{
		"a": {
			{TableID: 1, State: tablepb.TableStateReplicating},
			{TableID: 2, State: tablepb.TableStateReplicating},
			{TableID: 3, State: tablepb.TableStateReplicating},
		},
	}
	msgs, err := coord.replicationM.HandleCaptureChanges(init, nil, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, coord.replicationM.GetReplicationSetForTests(), 3)

	// Capture "b" is online, heartbeat, and then move one table to capture "b".
	ctx := context.Background()
	currentTables := []model.TableID{1, 2, 3}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs = trans.SendBuffer
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].Heartbeat, msgs[0])

	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:                "a",
		From:              "b",
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{},
	})
	trans.SendBuffer = []*schedulepb.Message{}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs = trans.SendBuffer
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].DispatchTableRequest.GetAddTable(), msgs[0])
	require.True(t, msgs[0].DispatchTableRequest.GetAddTable().IsSecondary)
}

func TestCoordinatorRemoveCapture(t *testing.T) {
	t.Parallel()

	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		CollectStatsTick:   math.MaxInt,
		MaxTaskConcurrency: 1,
		AddTableBatchSize:  50,
	})
	trans := transport.NewMockTrans()
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Three captures "a" "b" "c".
	// Three tables 1 2 3.
	coord.captureM.Captures["a"] = &member.CaptureStatus{State: member.CaptureStateInitialized}
	coord.captureM.Captures["b"] = &member.CaptureStatus{State: member.CaptureStateInitialized}
	coord.captureM.Captures["c"] = &member.CaptureStatus{State: member.CaptureStateInitialized}
	coord.captureM.SetInitializedForTests(true)
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	init := map[string][]tablepb.TableStatus{
		"a": {{TableID: 1, State: tablepb.TableStateReplicating}},
		"b": {{TableID: 2, State: tablepb.TableStateReplicating}},
		"c": {{TableID: 3, State: tablepb.TableStateReplicating}},
	}
	msgs, err := coord.replicationM.HandleCaptureChanges(init, nil, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, coord.replicationM.GetReplicationSetForTests(), 3)

	// Capture "c" is removed, add table 3 to another capture.
	ctx := context.Background()
	currentTables := []model.TableID{1, 2, 3}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	msgs = trans.SendBuffer
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].DispatchTableRequest.GetAddTable(), msgs[0])
	require.EqualValues(t, 3, msgs[0].DispatchTableRequest.GetAddTable().TableID)
}

func TestCoordinatorDrainCapture(t *testing.T) {
	t.Parallel()

	coord := coordinator{
		version:   "6.2.0",
		revision:  schedulepb.OwnerRevision{Revision: 3},
		captureID: "a",
	}
	cfg := config.NewDefaultSchedulerConfig()
	coord.captureM = member.NewCaptureManager("", model.ChangeFeedID{}, coord.revision, cfg)

	coord.captureM.SetInitializedForTests(true)
	coord.captureM.Captures["a"] = &member.CaptureStatus{State: member.CaptureStateUninitialized}
	count, err := coord.DrainCapture("a")
	require.ErrorIs(t, err, cerror.ErrSchedulerRequestFailed)
	require.Equal(t, 0, count)

	coord.captureM.Captures["a"] = &member.CaptureStatus{State: member.CaptureStateInitialized}
	coord.replicationM = replication.NewReplicationManager(10, model.ChangeFeedID{})
	count, err = coord.DrainCapture("a")
	require.NoError(t, err)
	require.Equal(t, 0, count)

	coord.replicationM.SetReplicationSetForTests(&replication.ReplicationSet{
		TableID: 1,
		State:   replication.ReplicationSetStateReplicating,
		Primary: "a",
	})

	count, err = coord.DrainCapture("a")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	coord.captureM.Captures["b"] = &member.CaptureStatus{State: member.CaptureStateInitialized}
	coord.replicationM.SetReplicationSetForTests(&replication.ReplicationSet{
		TableID: 2,
		State:   replication.ReplicationSetStateReplicating,
		Primary: "b",
	})

	count, err = coord.DrainCapture("a")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	coord.schedulerM = scheduler.NewSchedulerManager(
		model.ChangeFeedID{}, config.NewDefaultSchedulerConfig())
	count, err = coord.DrainCapture("b")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestCoordinatorAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		CollectStatsTick:   math.MaxInt,
		MaxTaskConcurrency: 1,
	})
	trans := transport.NewMockTrans()
	coord.trans = trans

	// Prepare captureM and replicationM.
	// Two captures "a", "b".
	// Three tables 1 2.
	ctx := context.Background()
	currentTables := []model.TableID{1, 2}
	aliveCaptures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	_, _, err := coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)

	// Initialize captures.
	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:                "a",
		From:              "b",
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{},
	})
	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:      "a",
		From:    "a",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{
				{
					TableID: 1, State: tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 2, ResolvedTs: 4,
					},
				},
				{
					TableID: 2, State: tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 2, ResolvedTs: 4,
					},
				},
			},
		},
	})
	cts, rts, err := coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	require.True(t, coord.captureM.CheckAllCaptureInitialized())
	require.EqualValues(t, 2, cts)
	require.EqualValues(t, 4, rts)

	// Checkpoint should be advanced even if there is an uninitialized capture.
	aliveCaptures["c"] = &model.CaptureInfo{}
	trans.RecvBuffer = nil
	trans.RecvBuffer = append(trans.RecvBuffer, &schedulepb.Message{
		Header: &schedulepb.Message_Header{
			OwnerRevision: schedulepb.OwnerRevision{Revision: 1},
		},
		To:      "a",
		From:    "a",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{
				{
					TableID: 1, State: tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 3, ResolvedTs: 5,
					},
				},
				{
					TableID: 2, State: tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 4, ResolvedTs: 5,
					},
				},
			},
		},
	})
	cts, rts, err = coord.poll(ctx, 0, currentTables, aliveCaptures)
	require.Nil(t, err)
	require.False(t, coord.captureM.CheckAllCaptureInitialized())
	require.EqualValues(t, 3, cts)
	require.EqualValues(t, 5, rts)
}
