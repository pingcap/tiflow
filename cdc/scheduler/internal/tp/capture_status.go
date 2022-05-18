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
	resp *schedulepb.HeartbeatResponse,
) {
	revMismatch := c.OwnerRev.Revision != resp.OwnerRevision.Revision
	epochMismatch := c.State != CaptureStateUninitialize &&
		c.Epoch.Epoch != resp.ProcessorEpoch.Epoch
	if revMismatch || epochMismatch {
		log.Warn("tpscheduler: ignore heartbeat response",
			zap.String("epoch", c.Epoch.Epoch),
			zap.String("respEpoch", resp.ProcessorEpoch.Epoch),
			zap.Int64("ownerRev", c.OwnerRev.Revision),
			zap.Int64("respOwnerRev", resp.OwnerRevision.Revision))
		return
	}

	if c.State == CaptureStateUninitialize {
		c.Epoch = resp.ProcessorEpoch
		c.State = CaptureStateInitialized
	}
	if resp.IsStopping {
		c.State = CaptureStateStopping
	}
}
