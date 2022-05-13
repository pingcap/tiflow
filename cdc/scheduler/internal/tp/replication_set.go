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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
)

// ReplicationSetState is the state of ReplicationSet in owner.
//
//     Absent
//       │
//       v
//  ┌───────────┐
//  │ Preparing │<──────┐
//  └────┬──────┘       │
//       v              │
//  ┌────────┐   ┌──────┴──────┐
//  │ Commit ├──>│ Replicating │<── Move table
//  └────────┘   └─────────────┘
type ReplicationSetState int

const (
	Prepare     ReplicationSetState = 1
	Commit      ReplicationSetState = 2
	Replicating ReplicationSetState = 3
)

type ReplicationSet struct {
	State      ReplicationSetState
	Primary    model.CaptureID
	Secondary  []model.CaptureID
	Checkpoint model.Ts
}

func newReplicationSet(tableStatus *schedulepb.TableStatus) *ReplicationSet {
	return nil
}

func (r *ReplicationSet) onTableStatus(
	from model.CaptureID, status *schedulepb.TableStatus,
) *schedulepb.Message {
	return nil
}

func (r *ReplicationSet) onDispatchTableRequest(
	request *schedulepb.DispatchTableRequest,
) *schedulepb.Message {
	return nil
}

func (r *ReplicationSet) onDispatchTableResponse(
	from model.CaptureID, response *schedulepb.DispatchTableResponse,
) *schedulepb.Message {
	return nil
}

func (r *ReplicationSet) onCheckpoint(
	from model.CaptureID, checkpoint *schedulepb.Checkpoint,
) *schedulepb.Message {
	return nil
}
