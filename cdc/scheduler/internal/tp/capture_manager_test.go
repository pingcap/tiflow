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

func TestCaptureManagerPoll(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{}
	ms := map[model.CaptureID]*model.CaptureInfo{
		"1": {},
		"2": {},
	}
	cm := newCaptureManager(rev, 2)

	// Initial poll for alive captures.
	msgs := cm.Poll(ms, nil)
	require.ElementsMatch(t, []*schedulepb.Message{
		{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
	}, msgs)
	require.False(t, cm.CheckAllCaptureInitialized())

	// Poll one response
	msgs = cm.Poll(ms, []*schedulepb.Message{
		{
			Header: &schedulepb.Message_Header{}, From: "1",
			MsgType:           schedulepb.MsgHeartbeatResponse,
			HeartbeatResponse: &schedulepb.HeartbeatResponse{},
		},
	})
	require.Empty(t, msgs)
	require.False(t, cm.CheckAllCaptureInitialized())

	// Poll another response
	msgs = cm.Poll(ms, []*schedulepb.Message{
		{
			Header: &schedulepb.Message_Header{}, From: "2",
			MsgType:           schedulepb.MsgHeartbeatResponse,
			HeartbeatResponse: &schedulepb.HeartbeatResponse{},
		},
	})
	require.Empty(t, msgs)
	require.True(t, cm.CheckAllCaptureInitialized(), "%v %v", cm.Captures["1"], cm.Captures["2"])

	// Poll unknown capture response
	msgs = cm.Poll(ms, []*schedulepb.Message{
		{
			Header: &schedulepb.Message_Header{}, From: "unknown",
			MsgType:           schedulepb.MsgHeartbeatResponse,
			HeartbeatResponse: &schedulepb.HeartbeatResponse{},
		},
	})
	require.Empty(t, msgs)
	require.True(t, cm.CheckAllCaptureInitialized())
}

func TestCaptureManagerTick(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{}
	cm := newCaptureManager(rev, 2)

	// No heartbeat if there is no capture.
	msgs := cm.Tick()
	require.Empty(t, msgs)
	msgs = cm.Tick()
	require.Empty(t, msgs)

	ms := map[model.CaptureID]*model.CaptureInfo{
		"1": {},
		"2": {},
	}
	cm.Poll(ms, nil)
	require.False(t, cm.CheckAllCaptureInitialized())

	// Heartbeat even if capture is uninitialize.
	msgs = cm.Tick()
	require.Empty(t, msgs)
	msgs = cm.Tick()
	require.ElementsMatch(t, []*schedulepb.Message{
		{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
	}, msgs)

	// Heartbeat even if capture is initialized or stopping.
	for _, s := range []CaptureState{CaptureStateInitialized, CaptureStateStopping} {
		cm.Captures["1"].State = s
		cm.Captures["2"].State = s
		require.True(t, cm.CheckAllCaptureInitialized())
		msgs = cm.Tick()
		require.Empty(t, msgs)
		msgs = cm.Tick()
		require.ElementsMatch(t, []*schedulepb.Message{
			{To: "1", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
			{To: "2", MsgType: schedulepb.MsgHeartbeat, Heartbeat: &schedulepb.Heartbeat{}},
		}, msgs)
	}
}
