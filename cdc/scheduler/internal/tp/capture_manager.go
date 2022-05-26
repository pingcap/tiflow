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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"go.uber.org/zap"
)

// CaptureState is the state of a capture.
//
//      ┌──────────────┐ Heartbeat Resp ┌─────────────┐
//      │ Uninitialize ├───────────────>│ Initialized │
//      └──────┬───────┘                └──────┬──────┘
//             │                               │
//  IsStopping │          ┌──────────┐         │ IsStopping
//             └────────> │ Stopping │ <───────┘
//                        └──────────┘
type CaptureState int

const (
	// CaptureStateUninitialize means the capture status is unknown,
	// no heartbeat response received yet.
	CaptureStateUninitialize CaptureState = 1
	// CaptureStateInitialized means owner has received heartbeat response.
	CaptureStateInitialized CaptureState = 2
	// CaptureStateStopping means the capture is removing, e.g., shutdown.
	CaptureStateStopping CaptureState = 3
)

// CaptureStatus represent captrue's status.
type CaptureStatus struct {
	OwnerRev schedulepb.OwnerRevision
	Epoch    schedulepb.ProcessorEpoch
	State    CaptureState
}

func newCaptureStatus(rev schedulepb.OwnerRevision) *CaptureStatus {
	return &CaptureStatus{OwnerRev: rev, State: CaptureStateUninitialize}
}

func (c *CaptureStatus) handleHeartbeatResponse(
	resp *schedulepb.HeartbeatResponse, epoch schedulepb.ProcessorEpoch,
) {
	// Check epoch for initialized captures.
	if c.State != CaptureStateUninitialize && c.Epoch.Epoch != epoch.Epoch {
		log.Warn("tpscheduler: ignore heartbeat response",
			zap.String("epoch", c.Epoch.Epoch),
			zap.String("respEpoch", epoch.Epoch),
			zap.Int64("ownerRev", c.OwnerRev.Revision))
		return
	}

	if c.State == CaptureStateUninitialize {
		c.Epoch = epoch
		c.State = CaptureStateInitialized
	}
	if resp.IsStopping {
		c.State = CaptureStateStopping
	}
}

type captureManager struct {
	OwnerRev schedulepb.OwnerRevision
	Captures map[model.CaptureID]*CaptureStatus

	// A logical clock counter, for heartbeat.
	tickCounter   int
	heartbeatTick int
}

func newCaptureManager(rev schedulepb.OwnerRevision, heartbeatTick int) *captureManager {
	return &captureManager{
		OwnerRev:      rev,
		Captures:      make(map[model.CaptureID]*CaptureStatus),
		heartbeatTick: heartbeatTick,
	}
}

func (c *captureManager) CaptureTableSets() map[model.CaptureID]*CaptureStatus {
	return c.Captures
}

func (c *captureManager) CheckAllCaptureInitialized() bool {
	for _, captrueStatus := range c.Captures {
		if captrueStatus.State == CaptureStateUninitialize {
			return false
		}
	}
	return true
}

func (c *captureManager) Tick() []*schedulepb.Message {
	c.tickCounter++
	if c.tickCounter < c.heartbeatTick {
		return nil
	}
	c.tickCounter = 0
	msgs := make([]*schedulepb.Message, 0, len(c.Captures))
	for to := range c.Captures {
		msgs = append(msgs, &schedulepb.Message{
			To:        to,
			MsgType:   schedulepb.MsgHeartbeat,
			Heartbeat: &schedulepb.Heartbeat{},
		})
	}
	return msgs
}

func (c *captureManager) Poll(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
	msgs []*schedulepb.Message,
) []*schedulepb.Message {
	outMsgs := c.onAliveCaptureUpdate(aliveCaptures)
	for _, msg := range msgs {
		if msg.MsgType == schedulepb.MsgHeartbeatResponse {
			captureStatus, ok := c.Captures[msg.From]
			if !ok {
				continue
			}
			captureStatus.handleHeartbeatResponse(
				msg.GetHeartbeatResponse(), msg.Header.ProcessorEpoch)
		}
	}
	return outMsgs
}

func (c *captureManager) onAliveCaptureUpdate(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) []*schedulepb.Message {
	msgs := make([]*schedulepb.Message, 0)
	for id := range aliveCaptures {
		if _, ok := c.Captures[id]; !ok {
			// A new capture.
			c.Captures[id] = newCaptureStatus(c.OwnerRev)
			log.Info("tpscheduler: find a new capture", zap.String("newCapture", id))
			msgs = append(msgs, &schedulepb.Message{
				To:        id,
				MsgType:   schedulepb.MsgHeartbeat,
				Heartbeat: &schedulepb.Heartbeat{},
			})
		}
	}
	return msgs
}
