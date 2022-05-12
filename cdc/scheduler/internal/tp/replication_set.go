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
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
)

// ReplicationSetState is the state of ReplicationSet in owner.
//
//  ┌────────┐   ┌─────────┐
//  │ Absent ├─> │ Prepare │
//  └────────┘   └──┬──────┘
//       ┌──────────┘   ^
//       v              │
//  ┌────────┐   ┌──────┴──────┐
//  │ Commit ├──>│ Replicating │<── Move table
//  └────────┘   └─────────────┘
type ReplicationSetState int

const (
	// Unknown means the replication state is unknown, it should not happen.
	Unknown ReplicationSetState = 0
	// Absent means there is no one replicates or prepares it.
	Absent      ReplicationSetState = 1
	Prepare     ReplicationSetState = 2
	Commit      ReplicationSetState = 3
	Replicating ReplicationSetState = 4
)

func (r ReplicationSetState) String() string {
	switch r {
	case Absent:
		return "Absent"
	case Prepare:
		return "Prepare"
	case Commit:
		return "Commit"
	case Replicating:
		return "Replicating"
	default:
		return fmt.Sprintf("Unknown %d", r)
	}
}

type ReplicationSet struct {
	TableID    model.TableID
	State      ReplicationSetState
	Primary    model.CaptureID
	Captures   map[model.CaptureID]schedulepb.TableState
	Checkpoint model.Ts
}

func newReplicationSet(
	tableStatus map[model.CaptureID]*schedulepb.TableStatus,
) *ReplicationSet {
	return nil
}

// poll transit replication state based on input and the current state.
// See ReplicationSetState's comment for the state transition.
func (r *ReplicationSet) poll(
	input schedulepb.TableState, captureID model.CaptureID,
) []*schedulepb.Message {
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
