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
//      ┌───────────────┐ Heartbeat Resp ┌─────────────┐
//      │ Uninitialized ├───────────────>│ Initialized │
//      └──────┬────────┘                └──────┬──────┘
//             │                                │
//  IsStopping │          ┌──────────┐          │ IsStopping
//             └────────> │ Stopping │ <────────┘
//                        └──────────┘
type CaptureState int

const (
	// CaptureStateUninitialized means the capture status is unknown,
	// no heartbeat response received yet.
	CaptureStateUninitialized CaptureState = 1
	// CaptureStateInitialized means owner has received heartbeat response.
	CaptureStateInitialized CaptureState = 2
	// CaptureStateStopping means the capture is removing, e.g., shutdown.
	CaptureStateStopping CaptureState = 3
)

var captureStateMap = map[CaptureState]string{
	CaptureStateUninitialized: "CaptureStateUninitialized",
	CaptureStateInitialized:   "CaptureStateInitialized",
	CaptureStateStopping:      "CaptureStateStopping",
}

func (s CaptureState) String() string {
	return captureStateMap[s]
}

// CaptureStatus represent capture's status.
type CaptureStatus struct {
	OwnerRev schedulepb.OwnerRevision
	Epoch    schedulepb.ProcessorEpoch
	State    CaptureState
	Tables   []schedulepb.TableStatus
	Addr     string
}

func newCaptureStatus(rev schedulepb.OwnerRevision, addr string) *CaptureStatus {
	return &CaptureStatus{OwnerRev: rev, State: CaptureStateUninitialized, Addr: addr}
}

func (c *CaptureStatus) handleHeartbeatResponse(
	resp *schedulepb.HeartbeatResponse, epoch schedulepb.ProcessorEpoch,
) {
	// Check epoch for initialized captures.
	if c.State != CaptureStateUninitialized && c.Epoch.Epoch != epoch.Epoch {
		log.Warn("tpscheduler: ignore heartbeat response",
			zap.String("epoch", c.Epoch.Epoch),
			zap.String("respEpoch", epoch.Epoch),
			zap.Int64("ownerRev", c.OwnerRev.Revision))
		return
	}

	if c.State == CaptureStateUninitialized {
		c.Epoch = epoch
		c.State = CaptureStateInitialized
	}
	if resp.IsStopping {
		c.State = CaptureStateStopping
	}
	c.Tables = resp.Tables
}

type captureChanges struct {
	Init    map[model.CaptureID][]schedulepb.TableStatus
	Removed map[model.CaptureID][]schedulepb.TableStatus
}

type captureManager struct {
	OwnerRev schedulepb.OwnerRevision
	Captures map[model.CaptureID]*CaptureStatus

	initialized bool
	changes     *captureChanges

	// A logical clock counter, for heartbeat.
	tickCounter   int
	heartbeatTick int

	changefeedID model.ChangeFeedID
}

func newCaptureManager(
	changefeedID model.ChangeFeedID, rev schedulepb.OwnerRevision, heartbeatTick int,
) *captureManager {
	return &captureManager{
		OwnerRev:      rev,
		Captures:      make(map[model.CaptureID]*CaptureStatus),
		heartbeatTick: heartbeatTick,

		changefeedID: changefeedID,
	}
}

func (c *captureManager) CheckAllCaptureInitialized() bool {
	return c.initialized && c.checkAllCaptureInitialized()
}

func (c *captureManager) checkAllCaptureInitialized() bool {
	for _, captureStatus := range c.Captures {
		// CaptureStateStopping is also considered initialized, because when
		// a capture shutdown, it becomes stopping, we need to move its tables
		// to other captures.
		if captureStatus.State == CaptureStateUninitialized {
			return false
		}
	}
	if len(c.Captures) == 0 {
		return false
	}
	return true
}

func (c *captureManager) Tick(
	reps map[model.TableID]*ReplicationSet, drainingCapture model.CaptureID,
) []*schedulepb.Message {
	c.tickCounter++
	if c.tickCounter < c.heartbeatTick {
		return nil
	}
	c.tickCounter = 0
	tables := make(map[model.CaptureID][]model.TableID)
	for tableID, rep := range reps {
		if rep.Primary != "" {
			tables[rep.Primary] = append(tables[rep.Primary], tableID)
		}
		if rep.Secondary != "" {
			tables[rep.Secondary] = append(tables[rep.Secondary], tableID)
		}
	}
	msgs := make([]*schedulepb.Message, 0, len(c.Captures))
	for to := range c.Captures {
		msgs = append(msgs, &schedulepb.Message{
			To:      to,
			MsgType: schedulepb.MsgHeartbeat,
			Heartbeat: &schedulepb.Heartbeat{
				TableIDs: tables[to],
				// IsStopping let the receiver capture know that it should be stopping now.
				// At the moment, this is trigger by `DrainCapture` scheduler.
				IsStopping: drainingCapture == to,
			},
		})
	}
	return msgs
}

func (c *captureManager) HandleMessage(
	msgs []*schedulepb.Message,
) {
	for _, msg := range msgs {
		if msg.MsgType == schedulepb.MsgHeartbeatResponse {
			captureStatus, ok := c.Captures[msg.From]
			if !ok {
				log.Warn("tpscheduler: heartbeat response from unknown capture",
					zap.String("capture", msg.From))
				continue
			}
			captureStatus.handleHeartbeatResponse(
				msg.GetHeartbeatResponse(), msg.Header.ProcessorEpoch)
		}
	}
}

func (c *captureManager) HandleAliveCaptureUpdate(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) []*schedulepb.Message {
	msgs := make([]*schedulepb.Message, 0)
	for id, info := range aliveCaptures {
		if _, ok := c.Captures[id]; !ok {
			// A new capture.
			c.Captures[id] = newCaptureStatus(c.OwnerRev, info.AdvertiseAddr)
			log.Info("tpscheduler: find a new capture", zap.String("capture", id))
			msgs = append(msgs, &schedulepb.Message{
				To:        id,
				MsgType:   schedulepb.MsgHeartbeat,
				Heartbeat: &schedulepb.Heartbeat{},
			})
		}
	}

	// Find removed captures.
	for id, capture := range c.Captures {
		if _, ok := aliveCaptures[id]; !ok {
			log.Info("tpscheduler: removed a capture", zap.String("capture", id))
			delete(c.Captures, id)

			// Only update changes after initialization.
			if !c.initialized {
				continue
			}
			if c.changes == nil {
				c.changes = &captureChanges{}
			}
			if c.changes.Removed == nil {
				c.changes.Removed = make(map[string][]schedulepb.TableStatus)
			}
			c.changes.Removed[id] = capture.Tables

			cf := c.changefeedID
			captureTableGauge.DeleteLabelValues(cf.Namespace, cf.ID, capture.Addr)
		}
	}

	// Check if this is the first time all captures are initialized.
	if !c.initialized && c.checkAllCaptureInitialized() {
		c.changes = &captureChanges{Init: make(map[string][]schedulepb.TableStatus)}
		for id, capture := range c.Captures {
			c.changes.Init[id] = capture.Tables
		}
		log.Info("tpscheduler: all capture initialized",
			zap.Int("captureCount", len(c.Captures)))
		c.initialized = true
	}

	return msgs
}

func (c *captureManager) TakeChanges() *captureChanges {
	// Only return changes when it's initialized.
	if !c.initialized {
		return nil
	}
	changes := c.changes
	c.changes = nil
	return changes
}

func (c *captureManager) CollectMetrics() {
	cf := c.changefeedID
	for _, capture := range c.Captures {
		captureTableGauge.
			WithLabelValues(cf.Namespace, cf.ID, capture.Addr).
			Set(float64(len(capture.Tables)))
	}
}

func (c *captureManager) CleanMetrics() {
	cf := c.changefeedID
	for _, capture := range c.Captures {
		captureTableGauge.DeleteLabelValues(cf.Namespace, cf.ID, capture.Addr)
	}
}
