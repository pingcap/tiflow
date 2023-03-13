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

package compat

import (
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestCheckSpanReplicationEnabled(t *testing.T) {
	t.Parallel()

	c := New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}, map[string]*model.CaptureInfo{})

	// Add 1 supported capture.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
	}))
	require.True(t, c.CheckSpanReplicationEnabled())

	// Add 1 supported capture again.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
		"b": {Version: SpanReplicationMinVersion.String()},
	}))
	require.True(t, c.CheckSpanReplicationEnabled())

	// Rolling upgrade 3 nodes cluster.
	unsupportedVersion := semver.New("4.0.0")
	require.True(t, unsupportedVersion.LessThan(*SpanReplicationMinVersion))
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
		"b": {Version: unsupportedVersion.String()},
		"c": {Version: unsupportedVersion.String()},
	}))
	require.False(t, c.CheckSpanReplicationEnabled())
	// Restart b.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
		"c": {Version: unsupportedVersion.String()},
	}))
	require.False(t, c.CheckSpanReplicationEnabled())
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
		"b": {Version: SpanReplicationMinVersion.String()},
		"c": {Version: unsupportedVersion.String()},
	}))
	require.False(t, c.CheckSpanReplicationEnabled())
	// Restart c.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
		"b": {Version: unsupportedVersion.String()},
	}))
	require.False(t, c.CheckSpanReplicationEnabled())
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
		"b": {Version: SpanReplicationMinVersion.String()},
		"c": {Version: SpanReplicationMinVersion.String()},
	}))
	require.True(t, c.CheckSpanReplicationEnabled())

	// Disable in config
	c = New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			RegionThreshold: 0,
		},
	}, map[string]*model.CaptureInfo{
		"a": {Version: SpanReplicationMinVersion.String()},
	})
	require.False(t, c.CheckSpanReplicationEnabled())
}

func TestBeforeTransportSend(t *testing.T) {
	t.Parallel()

	c := New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			RegionThreshold: 0, // Disable span replication.
		},
	}, map[string]*model.CaptureInfo{})
	require.False(t, c.CheckSpanReplicationEnabled())

	addTableReq := &schedulepb.AddTableRequest{
		Span: spanz.TableIDToComparableSpan(1),
	}
	c.BeforeTransportSend([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: addTableReq,
			},
		},
	}})
	require.EqualValues(t, 1, addTableReq.TableID)

	removeTableReq := &schedulepb.RemoveTableRequest{
		Span: spanz.TableIDToComparableSpan(1),
	}
	c.BeforeTransportSend([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: removeTableReq,
			},
		},
	}})
	require.EqualValues(t, 1, removeTableReq.TableID)

	addTableResp := &schedulepb.AddTableResponse{
		Status: &tablepb.TableStatus{
			Span: spanz.TableIDToComparableSpan(1),
		},
	}
	c.BeforeTransportSend([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: addTableResp,
			},
		},
	}})
	require.EqualValues(t, 1, addTableResp.Status.TableID)

	removeTableResp := &schedulepb.RemoveTableResponse{
		Status: &tablepb.TableStatus{
			Span: spanz.TableIDToComparableSpan(1),
		},
	}
	c.BeforeTransportSend([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: removeTableResp,
			},
		},
	}})
	require.EqualValues(t, 1, removeTableResp.Status.TableID)

	heartbeat := &schedulepb.Heartbeat{
		Spans: []tablepb.Span{spanz.TableIDToComparableSpan(1)},
	}
	c.BeforeTransportSend([]*schedulepb.Message{{
		MsgType:   schedulepb.MsgHeartbeat,
		Heartbeat: heartbeat,
	}})
	require.EqualValues(t, 1, heartbeat.TableIDs[0])
	heartbeatResp := &schedulepb.HeartbeatResponse{
		Tables: []tablepb.TableStatus{{Span: spanz.TableIDToComparableSpan(1)}},
	}
	c.BeforeTransportSend([]*schedulepb.Message{{
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: heartbeatResp,
	}})
	require.EqualValues(t, 1, heartbeatResp.Tables[0].TableID)
}

func TestAfterTransportReceive(t *testing.T) {
	t.Parallel()

	c := New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			RegionThreshold: 0, // Disable span replication.
		},
	}, map[string]*model.CaptureInfo{})
	require.False(t, c.CheckSpanReplicationEnabled())

	addTableReq := &schedulepb.AddTableRequest{
		TableID: 1,
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: addTableReq,
			},
		},
	}})
	require.EqualValues(t, spanz.TableIDToComparableSpan(1), addTableReq.Span)

	addTableReq1 := &schedulepb.AddTableRequest{
		TableID: 1,
		Span:    tablepb.Span{TableID: 1},
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: addTableReq1,
			},
		},
	}})
	require.EqualValues(t, tablepb.Span{TableID: 1}, addTableReq1.Span)

	removeTableReq := &schedulepb.RemoveTableRequest{
		TableID: 1,
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: removeTableReq,
			},
		},
	}})
	require.EqualValues(t, spanz.TableIDToComparableSpan(1), removeTableReq.Span)

	addTableResp := &schedulepb.AddTableResponse{
		Status: &tablepb.TableStatus{
			TableID: 1,
		},
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: addTableResp,
			},
		},
	}})
	require.EqualValues(t, spanz.TableIDToComparableSpan(1), addTableResp.Status.Span)

	removeTableResp := &schedulepb.RemoveTableResponse{
		Status: &tablepb.TableStatus{
			TableID: 1,
		},
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: removeTableResp,
			},
		},
	}})
	require.EqualValues(t, spanz.TableIDToComparableSpan(1), removeTableResp.Status.Span)

	heartbeat := &schedulepb.Heartbeat{
		TableIDs: []model.TableID{1},
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType:   schedulepb.MsgHeartbeat,
		Heartbeat: heartbeat,
	}})
	require.EqualValues(t, 1, heartbeat.TableIDs[0])
	heartbeatResp := &schedulepb.HeartbeatResponse{
		Tables: []tablepb.TableStatus{{TableID: 1}},
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: heartbeatResp,
	}})
	require.EqualValues(t, spanz.TableIDToComparableSpan(1), heartbeatResp.Tables[0].Span)

	heartbeatResp1 := &schedulepb.HeartbeatResponse{
		Tables: []tablepb.TableStatus{{
			TableID: 1,
			Span:    tablepb.Span{TableID: 1},
		}},
	}
	c.AfterTransportReceive([]*schedulepb.Message{{
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: heartbeatResp1,
	}})
	require.EqualValues(t, tablepb.Span{TableID: 1}, heartbeatResp1.Tables[0].Span)
}

func TestCheckChangefeedEpochEnabled(t *testing.T) {
	t.Parallel()

	c := New(&config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}, map[string]*model.CaptureInfo{})

	// Unknown capture always return false
	require.False(t, c.CheckChangefeedEpochEnabled("unknown"))

	// Add 1 supported capture.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: ChangefeedEpochMinVersion.String()},
	}))
	require.True(t, c.CheckChangefeedEpochEnabled("a"))
	// Check again.
	require.True(t, c.CheckChangefeedEpochEnabled("a"))

	// Add 1 supported capture again.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: ChangefeedEpochMinVersion.String()},
		"b": {Version: ChangefeedEpochMinVersion.String()},
	}))
	require.True(t, c.CheckChangefeedEpochEnabled("a"))
	require.True(t, c.CheckChangefeedEpochEnabled("b"))

	// Replace 1 unsupported capture.
	unsupported := *ChangefeedEpochMinVersion
	unsupported.Major--
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: ChangefeedEpochMinVersion.String()},
		"c": {Version: unsupported.String()},
	}))
	require.True(t, c.CheckChangefeedEpochEnabled("a"))
	require.False(t, c.CheckChangefeedEpochEnabled("b"))
	require.False(t, c.CheckChangefeedEpochEnabled("c"))
}
