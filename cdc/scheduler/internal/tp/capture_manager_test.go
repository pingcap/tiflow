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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/require"
)

func TestCaptureStatusHandleHeartbeatResponse(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{Revision: 1}
	epoch := schedulepb.ProcessorEpoch{Epoch: "test"}
	c := newCaptureStatus(rev)
	require.Equal(t, CaptureStateUninitialize, c.State)

	// Uninitialize -> Initialized
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{}, epoch)
	require.Equal(t, CaptureStateInitialized, c.State)
	require.Equal(t, epoch, c.Epoch)

	// Processor epoch mismatch
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{
		IsStopping: true,
	}, schedulepb.ProcessorEpoch{Epoch: "unknown"})
	require.Equal(t, CaptureStateInitialized, c.State)

	// Initialized -> Stopping
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{IsStopping: true}, epoch)
	require.Equal(t, CaptureStateStopping, c.State)
	require.Equal(t, epoch, c.Epoch)
}

func TestCaptureManagerHandleAliveCaptureUpdate(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{}
	cm := newCaptureManager(rev, 2)
	ms := map[model.CaptureID]*model.CaptureInfo{
		"1": {}, "2": {}, "3": {},
	}

	// Initial handle alive captures.
	msgs := cm.HandleAliveCaptureUpdate(ms)
	require.ElementsMatch(t, []*schedulepb.Message{
		{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		{To: "3", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
	}, msgs)
	require.False(t, cm.CheckAllCaptureInitialized())
	require.Nil(t, cm.TakeChanges())
	require.Contains(t, cm.Captures, "1")
	require.Contains(t, cm.Captures, "2")
	require.Contains(t, cm.Captures, "3")

	// Remove one capture before init.
	delete(ms, "1")
	msgs = cm.HandleAliveCaptureUpdate(ms)
	require.Len(t, msgs, 0)
	require.Nil(t, cm.TakeChanges())
	require.NotContains(t, cm.Captures, "1")
	require.Contains(t, cm.Captures, "2")
	require.Contains(t, cm.Captures, "3")

	// Init
	cm.HandleMessage([]*schedulepb.Message{{
		Header: &schedulepb.Message_Header{}, From: "2",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []schedulepb.TableStatus{{TableID: 1}},
		},
	}, {
		Header: &schedulepb.Message_Header{}, From: "3",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []schedulepb.TableStatus{{TableID: 2}},
		},
	}})
	require.False(t, cm.CheckAllCaptureInitialized())
	msgs = cm.HandleAliveCaptureUpdate(ms)
	require.Len(t, msgs, 0)
	require.True(t, cm.CheckAllCaptureInitialized())
	require.EqualValues(t, &captureChanges{
		Init: map[string][]schedulepb.TableStatus{"2": {{TableID: 1}}, "3": {{TableID: 2}}},
	}, cm.TakeChanges())

	// Add a new node and remove an old node.
	ms["4"] = &model.CaptureInfo{}
	delete(ms, "2")
	msgs = cm.HandleAliveCaptureUpdate(ms)
	require.ElementsMatch(t, []*schedulepb.Message{
		{To: "4", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
	}, msgs)
	require.Equal(t, &captureChanges{
		Removed: map[string][]schedulepb.TableStatus{"2": {{TableID: 1}}},
	}, cm.TakeChanges())
	require.False(t, cm.CheckAllCaptureInitialized())
}

func TestCaptureManagerHandleMessages(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{}
	ms := map[model.CaptureID]*model.CaptureInfo{
		"1": {},
		"2": {},
	}
	cm := newCaptureManager(rev, 2)
	require.False(t, cm.CheckAllCaptureInitialized())

	// Initial handle alive captures.
	msgs := cm.HandleAliveCaptureUpdate(ms)
	require.ElementsMatch(t, []*schedulepb.Message{
		{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
	}, msgs)
	require.False(t, cm.CheckAllCaptureInitialized())
	require.Contains(t, cm.Captures, "1")
	require.Contains(t, cm.Captures, "2")

	// Handle one response
	cm.HandleMessage([]*schedulepb.Message{
		{
			Header: &schedulepb.Message_Header{}, From: "1",
			MsgType:           schedulepb.MsgHeartbeatResponse,
			HeartbeatResponse: &schedulepb.HeartbeatResponse{},
		},
	})
	require.False(t, cm.CheckAllCaptureInitialized())

	// Handle another response
	cm.HandleMessage([]*schedulepb.Message{
		{
			Header: &schedulepb.Message_Header{}, From: "2",
			MsgType:           schedulepb.MsgHeartbeatResponse,
			HeartbeatResponse: &schedulepb.HeartbeatResponse{},
		},
	})
	require.False(t, cm.CheckAllCaptureInitialized(), "%v %v", cm.Captures["1"], cm.Captures["2"])

	// Handle unknown capture response
	cm.HandleMessage([]*schedulepb.Message{
		{
			Header: &schedulepb.Message_Header{}, From: "unknown",
			MsgType:           schedulepb.MsgHeartbeatResponse,
			HeartbeatResponse: &schedulepb.HeartbeatResponse{},
		},
	})
	require.False(t, cm.CheckAllCaptureInitialized())
}

func TestCaptureManagerTick(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{}
	cm := newCaptureManager(rev, 2)

	// No heartbeat if there is no capture.
	msgs := cm.Tick(nil)
	require.Empty(t, msgs)
	msgs = cm.Tick(nil)
	require.Empty(t, msgs)

	ms := map[model.CaptureID]*model.CaptureInfo{
		"1": {},
		"2": {},
	}
	cm.HandleAliveCaptureUpdate(ms)

	// Heartbeat even if capture is uninitialized.
	msgs = cm.Tick(nil)
	require.Empty(t, msgs)
	msgs = cm.Tick(nil)
	require.ElementsMatch(t, []*schedulepb.Message{
		{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
	}, msgs)

	// Heartbeat even if capture is initialized or stopping.
	for _, s := range []CaptureState{CaptureStateInitialized, CaptureStateStopping} {
		cm.Captures["1"].State = s
		cm.Captures["2"].State = s
		msgs = cm.Tick(nil)
		require.Empty(t, msgs)
		msgs = cm.Tick(nil)
		require.ElementsMatch(t, []*schedulepb.Message{
			{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
			{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		}, msgs)
	}

	// TableID in heartbeat.
	msgs = cm.Tick(nil)
	require.Empty(t, msgs)
	tables := map[model.TableID]*ReplicationSet{
		1: {Primary: "1"},
		2: {Primary: "1", Secondary: "2"},
		3: {Secondary: "2"},
		4: {},
	}
	msgs = cm.Tick(tables)
	require.Len(t, msgs, 2)
	if msgs[0].To == "1" {
		require.ElementsMatch(t, []model.TableID{1, 2}, msgs[0].Heartbeat.TableIDs)
		require.ElementsMatch(t, []model.TableID{2, 3}, msgs[1].Heartbeat.TableIDs)
	} else {
		require.ElementsMatch(t, []model.TableID{2, 3}, msgs[0].Heartbeat.TableIDs)
		require.ElementsMatch(t, []model.TableID{1, 2}, msgs[1].Heartbeat.TableIDs)
	}
}
