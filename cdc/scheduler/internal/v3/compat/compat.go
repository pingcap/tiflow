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

var (
	// SpanReplicationMinVersion is the min version that allows span replication.
	SpanReplicationMinVersion = semver.New("6.6.0-alpha")
	// ChangefeedEpochMinVersion is the min version that enables changefeed epoch.
	ChangefeedEpochMinVersion = semver.New("6.7.0-alpha")
)

// Compat is a compatibility layer between span replication and table replication.
type Compat struct {
	config      *config.ChangefeedSchedulerConfig
	captureInfo map[model.CaptureID]*model.CaptureInfo

	spanReplicationHasChecked bool
	spanReplicationEnabled    bool
	changefeedEpoch           map[model.CaptureID]bool
}

// New returns a new Compat.
func New(
	config *config.SchedulerConfig,
	captureInfo map[model.CaptureID]*model.CaptureInfo,
) *Compat {
	return &Compat{
		config:          config.ChangefeedSettings,
		captureInfo:     captureInfo,
		changefeedEpoch: make(map[string]bool),
	}
}

// UpdateCaptureInfo update the latest alive capture info.
// Returns true if capture info has changed.
func (c *Compat) UpdateCaptureInfo(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) bool {
	if len(aliveCaptures) != len(c.captureInfo) {
		c.captureInfo = aliveCaptures
		c.spanReplicationHasChecked = false
		c.changefeedEpoch = make(map[string]bool, len(aliveCaptures))
		return true
	}
	for id, alive := range aliveCaptures {
		info, ok := c.captureInfo[id]
		if !ok || info.Version != alive.Version {
			c.captureInfo = aliveCaptures
			c.spanReplicationHasChecked = false
			c.changefeedEpoch = make(map[string]bool, len(aliveCaptures))
			return true
		}
	}
	return false
}

// CheckSpanReplicationEnabled check if the changefeed can enable span replication.
func (c *Compat) CheckSpanReplicationEnabled() bool {
	if c.spanReplicationHasChecked {
		return c.spanReplicationEnabled
	}
	c.spanReplicationHasChecked = true

	c.spanReplicationEnabled = c.config.EnableTableAcrossNodes
	for _, capture := range c.captureInfo {
		if len(capture.Version) == 0 {
			c.spanReplicationEnabled = false
			break
		}
		captureVer := semver.New(version.SanitizeVersion(capture.Version))
		if captureVer.Compare(*SpanReplicationMinVersion) < 0 {
			c.spanReplicationEnabled = false
			break
		}
	}

	return c.spanReplicationEnabled
}

// CheckChangefeedEpochEnabled check if the changefeed enables epoch.
func (c *Compat) CheckChangefeedEpochEnabled(captureID model.CaptureID) bool {
	isEnabled, ok := c.changefeedEpoch[captureID]
	if ok {
		return isEnabled
	}

	captureInfo, ok := c.captureInfo[captureID]
	if !ok {
		return false
	}
	if len(captureInfo.Version) != 0 {
		captureVer := semver.New(version.SanitizeVersion(captureInfo.Version))
		isEnabled = captureVer.Compare(*ChangefeedEpochMinVersion) >= 0
	} else {
		isEnabled = false
	}
	c.changefeedEpoch[captureID] = isEnabled
	return isEnabled
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
		case schedulepb.MsgDispatchTableRequest:
			switch req := msgs[i].DispatchTableRequest.Request.(type) {
			case *schedulepb.DispatchTableRequest_AddTable:
				req.AddTable.TableID = req.AddTable.Span.TableID
			case *schedulepb.DispatchTableRequest_RemoveTable:
				req.RemoveTable.TableID = req.RemoveTable.Span.TableID
			}
		case schedulepb.MsgDispatchTableResponse:
			switch resp := msgs[i].DispatchTableResponse.Response.(type) {
			case *schedulepb.DispatchTableResponse_AddTable:
				resp.AddTable.Status.TableID = resp.AddTable.Status.Span.TableID
			case *schedulepb.DispatchTableResponse_RemoveTable:
				resp.RemoveTable.Status.TableID = resp.RemoveTable.Status.Span.TableID
			}
		case schedulepb.MsgHeartbeat:
			tableIDs := make([]model.TableID, 0, len(msgs[i].Heartbeat.Spans))
			for _, span := range msgs[i].Heartbeat.Spans {
				tableIDs = append(tableIDs, span.TableID)
			}
			msgs[i].Heartbeat.TableIDs = tableIDs
		case schedulepb.MsgHeartbeatResponse:
			resp := msgs[i].HeartbeatResponse
			for i := range resp.Tables {
				resp.Tables[i].TableID = resp.Tables[i].Span.TableID
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
		case schedulepb.MsgDispatchTableRequest:
			switch req := msgs[i].DispatchTableRequest.Request.(type) {
			case *schedulepb.DispatchTableRequest_AddTable:
				if req.AddTable.Span.TableID == 0 {
					// Only set span if it is not set before.
					req.AddTable.Span = spanz.TableIDToComparableSpan(
						req.AddTable.TableID)
				}
			case *schedulepb.DispatchTableRequest_RemoveTable:
				if req.RemoveTable.Span.TableID == 0 {
					req.RemoveTable.Span = spanz.TableIDToComparableSpan(
						req.RemoveTable.TableID)
				}
			}
		case schedulepb.MsgDispatchTableResponse:
			switch resp := msgs[i].DispatchTableResponse.Response.(type) {
			case *schedulepb.DispatchTableResponse_AddTable:
				if resp.AddTable.Status.Span.TableID == 0 {
					resp.AddTable.Status.Span = spanz.TableIDToComparableSpan(
						resp.AddTable.Status.TableID)
				}
			case *schedulepb.DispatchTableResponse_RemoveTable:
				if resp.RemoveTable.Status.Span.TableID == 0 {
					resp.RemoveTable.Status.Span = spanz.TableIDToComparableSpan(
						resp.RemoveTable.Status.TableID)
				}
			}
		case schedulepb.MsgHeartbeat:
			if len(msgs[i].Heartbeat.Spans) == 0 {
				spans := make([]tablepb.Span, 0, len(msgs[i].Heartbeat.TableIDs))
				for _, tableID := range msgs[i].Heartbeat.TableIDs {
					spans = append(spans, spanz.TableIDToComparableSpan(tableID))
				}
				msgs[i].Heartbeat.Spans = spans
			}
		case schedulepb.MsgHeartbeatResponse:
			resp := msgs[i].HeartbeatResponse
			for j := range resp.Tables {
				if resp.Tables[j].Span.TableID == 0 {
					resp.Tables[j].Span = spanz.TableIDToComparableSpan(
						resp.Tables[j].TableID)
				}
			}
		}
	}
}
