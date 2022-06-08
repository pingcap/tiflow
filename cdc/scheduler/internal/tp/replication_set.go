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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// ReplicationSetState is the state of ReplicationSet in owner.
//
//   AddTable
//  ┌────────┐   ┌─────────┐
//  │ Absent ├─> │ Prepare │
//  └────────┘   └──┬──────┘
//       ┌──────────┘   ^
//       v              │ MoveTable
//  ┌────────┐   ┌──────┴──────┐ RemoveTable ┌──────────┐
//  │ Commit ├──>│ Replicating │────────────>│ Removing │
//  └────────┘   └─────────────┘             └──────────┘
//
// When a capture shutdown unexpectedly, we may need to transit the state to
// Absent or Replicating immediately.
type ReplicationSetState int

const (
	// ReplicationSetStateUnknown means the replication state is unknown,
	// it should not happen.
	ReplicationSetStateUnknown ReplicationSetState = 0
	// ReplicationSetStateAbsent means there is no one replicates or prepares it.
	ReplicationSetStateAbsent ReplicationSetState = 1
	// ReplicationSetStatePrepare means it needs to add a secondary.
	ReplicationSetStatePrepare ReplicationSetState = 2
	// ReplicationSetStateCommit means it needs to promote secondary to primary.
	ReplicationSetStateCommit ReplicationSetState = 3
	// ReplicationSetStateReplicating means there is exactly one capture
	// that is replicating the table.
	ReplicationSetStateReplicating ReplicationSetState = 4
	// ReplicationSetStateRemoving means all captures need to
	// stop replication eventually.
	ReplicationSetStateRemoving ReplicationSetState = 5
)

func (r ReplicationSetState) String() string {
	switch r {
	case ReplicationSetStateAbsent:
		return "Absent"
	case ReplicationSetStatePrepare:
		return "Prepare"
	case ReplicationSetStateCommit:
		return "Commit"
	case ReplicationSetStateReplicating:
		return "Replicating"
	case ReplicationSetStateRemoving:
		return "Removing"
	default:
		return fmt.Sprintf("Unknown %d", r)
	}
}

// ReplicationSet is a state machine that manages replication states.
type ReplicationSet struct {
	TableID    model.TableID
	State      ReplicationSetState
	Primary    model.CaptureID
	Secondary  model.CaptureID
	Captures   map[model.CaptureID]struct{}
	Checkpoint schedulepb.Checkpoint
}

func newReplicationSet(
	tableID model.TableID,
	checkpoint model.Ts,
	tableStatus map[model.CaptureID]*schedulepb.TableStatus,
) (*ReplicationSet, error) {
	r := &ReplicationSet{
		TableID:  tableID,
		Captures: make(map[string]struct{}),
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: checkpoint,
			ResolvedTs:   checkpoint,
		},
	}
	committed := false
	for captureID, table := range tableStatus {
		r.updateCheckpoint(table.Checkpoint)
		if r.TableID != table.TableID {
			return nil, r.inconsistentError(table, captureID,
				"tpscheduler: table id inconsistent")
		}

		switch table.State {
		case schedulepb.TableStateReplicating:
			// Recognize primary if it's table is in replicating state.
			if len(r.Primary) == 0 {
				r.Primary = captureID
				r.Captures[captureID] = struct{}{}
			} else {
				return nil, r.multiplePrimaryError(
					table, captureID, "tpscheduler: multiple primary",
					zap.Any("status", tableStatus))
			}
		case schedulepb.TableStatePreparing:
			// Recognize secondary if it's table is in preparing state.
			r.Secondary = captureID
			r.Captures[captureID] = struct{}{}
		case schedulepb.TableStatePrepared:
			// Recognize secondary and Commit state if it's table is in prepared state.
			committed = true
			r.Secondary = captureID
			r.Captures[captureID] = struct{}{}
		case schedulepb.TableStateAbsent,
			schedulepb.TableStateStopping,
			schedulepb.TableStateStopped:
			// Ignore stop state.
		default:
			log.Warn("tpscheduler: unknown table state",
				zap.Any("replicationSet", r),
				zap.Int64("tableID", table.TableID),
				zap.Any("status", tableStatus))
		}
	}

	// Build state from primary, secondary and captures.
	if len(r.Primary) != 0 {
		r.State = ReplicationSetStateReplicating
	}
	// Move table or add table is in-progress.
	if len(r.Secondary) != 0 {
		r.State = ReplicationSetStatePrepare
	}
	// Move table or add table is committed.
	if committed {
		r.State = ReplicationSetStateCommit
	}
	if len(r.Captures) == 0 {
		r.State = ReplicationSetStateAbsent
	}
	log.Info("tpscheduler: initialize replication set",
		zap.Any("replicationSet", r))

	return r, nil
}

func (r *ReplicationSet) inconsistentError(
	input *schedulepb.TableStatus, captureID model.CaptureID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.Stringer("tableState", input),
		zap.Any("replicationSet", r),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return cerror.ErrReplicationSetInconsistent.GenWithStackByArgs(
		fmt.Sprintf("tableID %d, %s", r.TableID, msg))
}

func (r *ReplicationSet) multiplePrimaryError(
	input *schedulepb.TableStatus, captureID model.CaptureID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("captureID", captureID),
		zap.Stringer("tableState", input),
		zap.Any("replicationSet", r),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return cerror.ErrReplicationSetMultiplePrimaryError.GenWithStackByArgs(
		fmt.Sprintf("tableID %d, %s", r.TableID, msg))
}

// checkInvariant ensures ReplicationSet invariant is hold.
func (r *ReplicationSet) checkInvariant(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) error {
	if r.TableID != input.TableID {
		return r.inconsistentError(input, captureID,
			"tpscheduler: tableID must be the same")
	}
	if r.Primary == r.Secondary && r.Primary != "" {
		return r.inconsistentError(input, captureID,
			"tpscheduler: primary and secondary can not be the same")
	}
	_, okP := r.Captures[r.Primary]
	_, okS := r.Captures[r.Secondary]
	if (!okP && r.Primary != "") || (!okS && r.Secondary != "") {
		return r.inconsistentError(input, captureID,
			"tpscheduler: capture inconsistent")
	}
	if _, ok := r.Captures[captureID]; !ok &&
		input.State != schedulepb.TableStateAbsent {
		return r.inconsistentError(input, captureID, fmt.Sprintf(
			"tpscheduler: unknown capture: \"%s\", input: \"%s\"", captureID, input))
	}
	return nil
}

// poll transit replication state based on input and the current state.
// See ReplicationSetState's comment for the state transition.
func (r *ReplicationSet) poll(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) ([]*schedulepb.Message, error) {
	err := r.checkInvariant(input, captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	msgBuf := make([]*schedulepb.Message, 0)
	stateChanged := true
	for stateChanged {
		oldState := r.State
		var msg *schedulepb.Message
		switch r.State {
		case ReplicationSetStateAbsent:
			msg, stateChanged, err = r.pollOnAbsent(input, captureID)
		case ReplicationSetStatePrepare:
			msg, stateChanged, err = r.pollOnPrepare(input, captureID)
		case ReplicationSetStateCommit:
			msg, stateChanged, err = r.pollOnCommit(input, captureID)
		case ReplicationSetStateReplicating:
			msg, stateChanged, err = r.pollOnReplicating(input, captureID)
		case ReplicationSetStateRemoving:
			msg, stateChanged, err = r.pollOnRemoving(input, captureID)
		default:
			return nil, r.inconsistentError(
				input, captureID, "tpscheduler: table state unknown")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if msg != nil {
			msgBuf = append(msgBuf, msg)
		}
		if stateChanged {
			log.Info("tpscheduler: replication state transition, poll",
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Stringer("old", oldState), zap.Stringer("new", r.State))
		}
	}

	return msgBuf, nil
}

func (r *ReplicationSet) pollOnAbsent(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case schedulepb.TableStateAbsent:
		if r.Primary != "" || r.Secondary != "" {
			return nil, false, r.inconsistentError(
				input, captureID, "tpscheduler: there must be no primary or secondary")
		}
		r.State = ReplicationSetStatePrepare
		r.Secondary = captureID
		return nil, true, nil

	case schedulepb.TableStateStopped:
		// Ignore stopped table state as a capture may shutdown unexpectedly.
		return nil, false, nil
	case schedulepb.TableStatePreparing,
		schedulepb.TableStatePrepared,
		schedulepb.TableStateReplicating,
		schedulepb.TableStateStopping:
	}
	log.Warn("tpscheduler: ignore input, unexpected replication set state",
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) pollOnPrepare(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case schedulepb.TableStateAbsent:
		if r.Secondary == captureID {
			return &schedulepb.Message{
				To:      captureID,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							TableID:     r.TableID,
							IsSecondary: true,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, false, nil
		}
	case schedulepb.TableStatePreparing:
		if r.Secondary == captureID {
			// Ignore secondary Preparing, it may take a long time.
			return nil, false, nil
		}
	case schedulepb.TableStatePrepared:
		if r.Secondary == captureID {
			// Secondary is prepared, transit to Commit state.
			r.State = ReplicationSetStateCommit
			return nil, true, nil
		}
	case schedulepb.TableStateReplicating:
		if r.Primary == captureID {
			r.updateCheckpoint(input.Checkpoint)
			return nil, false, nil
		}
	case schedulepb.TableStateStopping, schedulepb.TableStateStopped:
		if r.Primary == captureID {
			// Primary is stopped, but we may still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("tpscheduler: primary is stopped during Prepare",
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			r.Primary = ""
			delete(r.Captures, captureID)
			return nil, false, nil
		} else if r.Secondary == captureID {
			log.Info("tpscheduler: capture is stopped during Prepare",
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			r.Secondary = ""
			delete(r.Captures, captureID)
			if r.Primary != "" {
				// Secondary is stopped, and we still has primary.
				// Transit to Replicating.
				r.State = ReplicationSetStateReplicating
			} else {
				// Secondary is stopped, and we do not has primary.
				// Transit to Absent.
				r.State = ReplicationSetStateAbsent
			}
			return nil, true, nil
		}
	}
	log.Warn("tpscheduler: ignore input, unexpected replication set state",
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) pollOnCommit(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case schedulepb.TableStatePrepared:
		if r.Secondary == captureID {
			if r.Primary != "" {
				// Secondary capture is prepared and waiting for stopping primary.
				// Send message to primary, ask for stopping.
				return &schedulepb.Message{
					To:      r.Primary,
					MsgType: schedulepb.MsgDispatchTableRequest,
					DispatchTableRequest: &schedulepb.DispatchTableRequest{
						Request: &schedulepb.DispatchTableRequest_RemoveTable{
							RemoveTable: &schedulepb.RemoveTableRequest{TableID: r.TableID},
						},
					},
				}, false, nil
			}
			// No primary, promote secondary to primary.
			r.Primary = r.Secondary
			r.Secondary = ""
			log.Info("tpscheduler: replication state promote secondary, no primary",
				zap.Any("replicationSet", r),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID))
		}
		// Secondary has been promoted, retry AddTableRequest.
		if r.Primary == captureID && r.Secondary == "" {
			return &schedulepb.Message{
				To:      captureID,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							TableID:     r.TableID,
							IsSecondary: false,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, false, nil
		}

	case schedulepb.TableStateStopped, schedulepb.TableStateAbsent:
		if r.Primary == captureID {
			r.updateCheckpoint(input.Checkpoint)
			original := r.Primary
			delete(r.Captures, r.Primary)
			r.Primary = ""
			if r.Secondary == "" {
				// If there is no secondary, transit to Absent.
				log.Info("tpscheduler: primary is stopped during Commit",
					zap.Stringer("tableState", input),
					zap.String("captureID", captureID),
					zap.Any("replicationSet", r))
				r.State = ReplicationSetStateAbsent
				return nil, true, nil
			}
			// Primary is stopped, promote secondary to primary.
			r.Primary = r.Secondary
			r.Secondary = ""
			log.Info("tpscheduler: replication state promote secondary",
				zap.Any("replicationSet", r),
				zap.Stringer("tableState", input),
				zap.String("original", original),
				zap.String("captureID", captureID))
			return &schedulepb.Message{
				To:      r.Primary,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							TableID:     r.TableID,
							IsSecondary: false,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, false, nil
		} else if r.Secondary == captureID {
			// As it sends RemoveTableRequest to the original primary
			// upon entering Commit state. Do not change state and wait
			// the original primary reports its table.
			log.Info("tpscheduler: secondary is stopped during Commit",
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			delete(r.Captures, r.Secondary)
			r.Secondary = ""
			return nil, true, nil
		}

	case schedulepb.TableStateReplicating:
		if r.Primary == captureID {
			r.updateCheckpoint(input.Checkpoint)
			if r.Secondary != "" {
				// Original primary is not stopped, ask for stopping.
				return &schedulepb.Message{
					To:      captureID,
					MsgType: schedulepb.MsgDispatchTableRequest,
					DispatchTableRequest: &schedulepb.DispatchTableRequest{
						Request: &schedulepb.DispatchTableRequest_RemoveTable{
							RemoveTable: &schedulepb.RemoveTableRequest{TableID: r.TableID},
						},
					},
				}, false, nil
			}

			// There are three cases for empty secondary.
			//
			// 1. Secondary has promoted to primary, and the new primary is
			//    replicating, transit to Replicating.
			// 2. Secondary has shutdown during Commit, the original primary
			//    does not receives RemoveTable request and continues to
			//    replicate, transit to Replicating.
			// 3. Secondary has shutdown during Commit, we receives a message
			//    before the original primary receives RemoveTable request.
			//    Transit to Replicating, and wait for the next table state of
			//    the primary, Stopping or Stopped.
			r.State = ReplicationSetStateReplicating
			return nil, true, nil
		}
		return nil, false, r.multiplePrimaryError(
			input, captureID, "tpscheduler: multiple primary")

	case schedulepb.TableStateStopping:
		if r.Primary == captureID && r.Secondary != "" {
			r.updateCheckpoint(input.Checkpoint)
			return nil, false, nil
		}
	case schedulepb.TableStatePreparing:
	}
	log.Warn("tpscheduler: ignore input, unexpected replication set state",
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) pollOnReplicating(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case schedulepb.TableStateReplicating:
		if r.Primary == captureID {
			r.updateCheckpoint(input.Checkpoint)
			return nil, false, nil
		}
		return nil, false, r.multiplePrimaryError(
			input, captureID, "tpscheduler: multiple primary")

	case schedulepb.TableStateAbsent:
	case schedulepb.TableStatePreparing:
	case schedulepb.TableStatePrepared:
	case schedulepb.TableStateStopping:
	case schedulepb.TableStateStopped:
		if r.Primary == captureID {
			r.updateCheckpoint(input.Checkpoint)

			// Primary is stopped, but we still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("tpscheduler: primary is stopped during Replicating",
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			r.Primary = ""
			delete(r.Captures, captureID)
			r.State = ReplicationSetStateAbsent
			return nil, true, nil
		}
	}
	log.Warn("tpscheduler: ignore input, unexpected replication set state",
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) pollOnRemoving(
	input *schedulepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case schedulepb.TableStatePreparing,
		schedulepb.TableStatePrepared,
		schedulepb.TableStateReplicating:
		return &schedulepb.Message{
			To:      captureID,
			MsgType: schedulepb.MsgDispatchTableRequest,
			DispatchTableRequest: &schedulepb.DispatchTableRequest{
				Request: &schedulepb.DispatchTableRequest_RemoveTable{
					RemoveTable: &schedulepb.RemoveTableRequest{TableID: r.TableID},
				},
			},
		}, false, nil
	case schedulepb.TableStateAbsent, schedulepb.TableStateStopped:
		if r.Primary == captureID {
			r.Primary = ""
		} else if r.Secondary == captureID {
			r.Secondary = ""
		}
		delete(r.Captures, captureID)
		log.Info("tpscheduler: replication state remove capture",
			zap.Any("replicationSet", r),
			zap.Stringer("tableState", input),
			zap.String("captureID", captureID))
		return nil, false, nil
	case schedulepb.TableStateStopping:
		return nil, false, nil
	}
	log.Warn("tpscheduler: ignore input, unexpected replication set state",
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) handleTableStatus(
	from model.CaptureID, status *schedulepb.TableStatus,
) ([]*schedulepb.Message, error) {
	return r.poll(status, from)
}

func (r *ReplicationSet) handleAddTable(
	captureID model.CaptureID,
) ([]*schedulepb.Message, error) {
	// Ignore add table if it's not in Absent state.
	if r.State != ReplicationSetStateAbsent {
		log.Warn("tpscheduler: add table is ignored",
			zap.Any("replicationSet", r), zap.Int64("tableID", r.TableID))
		return nil, nil
	}
	oldState := r.State
	r.State = ReplicationSetStateAbsent
	log.Info("tpscheduler: replication state transition, add table",
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState), zap.Stringer("new", r.State))
	r.Captures[captureID] = struct{}{}
	status := schedulepb.TableStatus{
		TableID:    r.TableID,
		State:      schedulepb.TableStateAbsent,
		Checkpoint: schedulepb.Checkpoint{},
	}
	return r.poll(&status, captureID)
}

func (r *ReplicationSet) handleMoveTable(
	dest model.CaptureID,
) ([]*schedulepb.Message, error) {
	// Ignore move table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("tpscheduler: move table is ignored",
			zap.Any("replicationSet", r), zap.Int64("tableID", r.TableID))
		return nil, nil
	}
	// Ignore move table if it's not in Replicating state.
	if r.State != ReplicationSetStateReplicating {
		log.Warn("tpscheduler: move table is ignored",
			zap.Any("replicationSet", r), zap.Int64("tableID", r.TableID))
		return nil, nil
	}
	oldState := r.State
	r.State = ReplicationSetStatePrepare
	log.Info("tpscheduler: replication state transition, move table",
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState), zap.Stringer("new", r.State))
	r.Secondary = dest
	r.Captures[dest] = struct{}{}
	status := schedulepb.TableStatus{
		TableID:    r.TableID,
		State:      schedulepb.TableStateAbsent,
		Checkpoint: schedulepb.Checkpoint{},
	}
	return r.poll(&status, r.Secondary)
}

func (r *ReplicationSet) handleRemoveTable() ([]*schedulepb.Message, error) {
	// Ignore remove table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("tpscheduler: remove table is ignored",
			zap.Any("replicationSet", r), zap.Int64("tableID", r.TableID))
		return nil, nil
	}
	// Ignore remove table if it's not in Replicating state.
	if r.State != ReplicationSetStateReplicating {
		log.Warn("tpscheduler: remove table is ignored",
			zap.Any("replicationSet", r), zap.Int64("tableID", r.TableID))
		return nil, nil
	}
	oldState := r.State
	r.State = ReplicationSetStateRemoving
	log.Info("tpscheduler: replication state transition, remove table",
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState), zap.Stringer("new", r.State))
	status := schedulepb.TableStatus{
		TableID: r.TableID,
		State:   schedulepb.TableStateReplicating,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: r.Checkpoint.CheckpointTs,
			ResolvedTs:   r.Checkpoint.ResolvedTs,
		},
	}
	return r.poll(&status, r.Primary)
}

func (r *ReplicationSet) hasRemoved() bool {
	// It has been removed successfully if it's state is Removing,
	// and there is no capture has it.
	return r.State == ReplicationSetStateRemoving && len(r.Captures) == 0
}

func (r *ReplicationSet) handleCaptureShutdown(
	captureID model.CaptureID,
) ([]*schedulepb.Message, error) {
	_, ok := r.Captures[captureID]
	if !ok {
		return nil, nil
	}
	// The capture has shutdown, the table has stopped.
	status := schedulepb.TableStatus{
		TableID: r.TableID,
		State:   schedulepb.TableStateStopped,
	}
	return r.poll(&status, captureID)
}

func (r *ReplicationSet) updateCheckpoint(checkpoint schedulepb.Checkpoint) {
	if r.Checkpoint.CheckpointTs < checkpoint.CheckpointTs {
		r.Checkpoint.CheckpointTs = checkpoint.CheckpointTs
	}
	if r.Checkpoint.ResolvedTs < checkpoint.ResolvedTs {
		r.Checkpoint.ResolvedTs = checkpoint.ResolvedTs
	}
}
