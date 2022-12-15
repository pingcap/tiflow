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
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/version"
)

// Compat is a compatibility layer between span replication and table replication.
type Compat struct {
	enableSpanReplication bool

	hasChecked    bool
	regionPerSpan int
	captureInfo   map[model.CaptureID]*model.CaptureInfo
}

// New returns a new Compat.
func New(
	config *config.SchedulerConfig,
	captureInfo map[model.CaptureID]*model.CaptureInfo,
) *Compat {
	return &Compat{
		regionPerSpan: config.RegionPerSpan,
		captureInfo:   captureInfo,
	}
}

// UpdateCaptureInfo update the latest alive capture info.
// Returns true if capture info has changed.
func (c *Compat) UpdateCaptureInfo(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) bool {
	if len(aliveCaptures) != len(c.captureInfo) {
		c.captureInfo = aliveCaptures
		c.hasChecked = false
		return true
	}
	for id := range aliveCaptures {
		_, ok := c.captureInfo[id]
		if !ok {
			c.captureInfo = aliveCaptures
			c.hasChecked = false
			return true
		}
	}
	return false
}

// SpanReplicationMinVersion is the min version that allows span replication.
// For now, we ASSUME it is `v6.6.0-alpha`.
// FIXME: correct the min version before we release the feature.
var SpanReplicationMinVersion = semver.New("6.6.0-alpha")

// CheckSpanReplicationEnabled check if the changefeed can enable span replication.
func (c *Compat) CheckSpanReplicationEnabled() bool {
	if c.hasChecked {
		return c.enableSpanReplication
	}
	c.hasChecked = true

	c.enableSpanReplication = c.regionPerSpan != 0
	for _, capture := range c.captureInfo {
		if len(capture.Version) == 0 {
			c.enableSpanReplication = false
			break
		}
		captureVer := semver.New(version.SanitizeVersion(capture.Version))
		if captureVer.Compare(*SpanReplicationMinVersion) < 0 {
			c.enableSpanReplication = false
			break
		}
	}

	return c.enableSpanReplication
}

// BeforeTransportSend modifies messages in place before sending messages,
// makes messages compatible with other end.
func (c *Compat) BeforeTransportSend(msgs []*schedulepb.Message) {
	if c.CheckSpanReplicationEnabled() {
		return
	}

	// - span agent -> table scheduler
	//   - tableID = span.TableID
	// - span scheduler -> table agent
	//   - tableID = span.TableID
	for i := range msgs {
		switch msgs[i].MsgType {
		case schedulepb.MessageType_MsgDispatchTableRequest:
			switch req := msgs[i].DispatchTableRequest.Request.(type) {
			case *schedulepb.DispatchTableRequest_AddTable:
				req.AddTable.TableId = req.AddTable.Span.TableId
			case *schedulepb.DispatchTableRequest_RemoveTable:
				req.RemoveTable.TableId = req.RemoveTable.Span.TableId
			}
		case schedulepb.MessageType_MsgDispatchTableResponse:
			switch resp := msgs[i].DispatchTableResponse.Response.(type) {
			case *schedulepb.DispatchTableResponse_AddTable:
				resp.AddTable.Status.TableId = resp.AddTable.Status.Span.TableId
			case *schedulepb.DispatchTableResponse_RemoveTable:
				resp.RemoveTable.Status.TableId = resp.RemoveTable.Status.Span.TableId
			}
		case schedulepb.MessageType_MsgHeartbeat:
			tableIDs := make([]model.TableID, 0, len(msgs[i].Heartbeat.Spans))
			for _, span := range msgs[i].Heartbeat.Spans {
				tableIDs = append(tableIDs, span.TableId)
			}
			msgs[i].Heartbeat.TableIds = tableIDs
		case schedulepb.MessageType_MsgHeartbeatResponse:
			resp := msgs[i].HeartbeatResponse
			for i := range resp.Tables {
				resp.Tables[i].TableId = resp.Tables[i].Span.TableId
			}
		}
	}
}

// AfterTransportReceive modifies messages in place after receiving messages,
// makes messages compatible with other end.
func (c *Compat) AfterTransportReceive(msgs []*schedulepb.Message) {
	if c.CheckSpanReplicationEnabled() {
		return
	}

	// - table scheduler -> span agent
	//   - Fill span based on table ID if span is empty
	// - table agent -> span scheduler
	//   - Fill span based on table ID if span is empty
	for i := range msgs {
		switch msgs[i].MsgType {
		case schedulepb.MessageType_MsgDispatchTableRequest:
			switch req := msgs[i].DispatchTableRequest.Request.(type) {
			case *schedulepb.DispatchTableRequest_AddTable:
				if req.AddTable.Span.TableId == 0 {
					// Only set span if it is not set before.
					req.AddTable.Span = spanz.TableIDToComparableSpan(
						req.AddTable.TableId)
				}
			case *schedulepb.DispatchTableRequest_RemoveTable:
				if req.RemoveTable.Span.TableId == 0 {
					req.RemoveTable.Span = spanz.TableIDToComparableSpan(
						req.RemoveTable.TableId)
				}
			}
		case schedulepb.MessageType_MsgDispatchTableResponse:
			switch resp := msgs[i].DispatchTableResponse.Response.(type) {
			case *schedulepb.DispatchTableResponse_AddTable:
				if resp.AddTable.Status.Span.TableId == 0 {
					resp.AddTable.Status.Span = spanz.TableIDToComparableSpan(
						resp.AddTable.Status.TableId)
				}
			case *schedulepb.DispatchTableResponse_RemoveTable:
				if resp.RemoveTable.Status.Span.TableId == 0 {
					resp.RemoveTable.Status.Span = spanz.TableIDToComparableSpan(
						resp.RemoveTable.Status.TableId)
				}
			}
		case schedulepb.MessageType_MsgHeartbeat:
			if len(msgs[i].Heartbeat.Spans) == 0 {
				spans := make([]*tablepb.Span, 0, len(msgs[i].Heartbeat.TableIds))
				for _, tableID := range msgs[i].Heartbeat.TableIds {
					spans = append(spans, spanz.TableIDToComparableSpan(tableID))
				}
				msgs[i].Heartbeat.Spans = spans
			}
		case schedulepb.MessageType_MsgHeartbeatResponse:
			resp := msgs[i].HeartbeatResponse
			for j := range resp.Tables {
				if resp.Tables[j].Span.TableId == 0 {
					resp.Tables[j].Span = spanz.TableIDToComparableSpan(
						resp.Tables[j].TableId)
				}
			}
		}
	}
}
