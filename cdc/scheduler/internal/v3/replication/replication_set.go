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

package replication

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// ReplicationSetState is the state of ReplicationSet in owner.
//
//	 AddTable
//	┌────────┐   ┌─────────┐
//	│ Absent ├─> │ Prepare │
//	└────────┘   └──┬──────┘
//	     ┌──────────┘   ^
//	     v              │ MoveTable
//	┌────────┐   ┌──────┴──────┐ RemoveTable ┌──────────┐
//	│ Commit ├──>│ Replicating │────────────>│ Removing │
//	└────────┘   └─────────────┘             └──────────┘
//
// When a capture shutdown unexpectedly, we may need to transit the state to
// Absent or Replicating immediately.
//
//nolint:revive
type ReplicationSetState int

const (
	// ReplicationSetStateUnknown means the replication state is unknown,
	// it should not happen.
	ReplicationSetStateUnknown ReplicationSetState = 0

	// ReplicationSetStateAbsent means there is no one replicates or prepares it.
	ReplicationSetStateAbsent ReplicationSetState = 1

	// ReplicationSetStatePrepare means one capture is preparing it,
	// there might have another capture is replicating the table.
	ReplicationSetStatePrepare ReplicationSetState = 2

	// ReplicationSetStateCommit means one capture is prepared,
	// it needs to promote secondary to primary.
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
		return "Unknown"
	}
}

// MarshalJSON returns r as the JSON encoding of ReplicationSetState.
// Only used for pretty print in zap log.
func (r ReplicationSetState) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// Role is the role of a capture.
type Role int

const (
	// RolePrimary primary role.
	RolePrimary = 1
	// RoleSecondary secondary role.
	RoleSecondary = 2
	// RoleUndetermined means that we don't know its state, it may be
	// replicating, stopping or stopped.
	RoleUndetermined = 3
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "Primary"
	case RoleSecondary:
		return "Secondary"
	case RoleUndetermined:
		return "Undetermined"
	default:
		return fmt.Sprintf("Unknown %d", r)
	}
}

// MarshalJSON returns r as the JSON encoding of CaptureRole.
// Only used for pretty print in zap log.
func (r Role) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// ReplicationSet is a state machine that manages replication states.
type ReplicationSet struct { //nolint:revive
	Changefeed model.ChangeFeedID
	Span       tablepb.Span
	State      ReplicationSetState
	// Primary is the capture ID that is currently replicating the table.
	Primary model.CaptureID
	// Captures is a map of captures that has the table replica.
	// NB: Invariant, 1) at most one primary, 2) primary capture must be in
	//     CaptureRolePrimary.
	Captures   map[model.CaptureID]Role
	Checkpoint tablepb.Checkpoint
	Stats      tablepb.Stats
}

// NewReplicationSet returns a new replication set.
func NewReplicationSet(
	span tablepb.Span,
	checkpoint model.Ts,
	tableStatus map[model.CaptureID]*tablepb.TableStatus,
	changefeed model.ChangeFeedID,
) (*ReplicationSet, error) {
	r := &ReplicationSet{
		Changefeed: changefeed,
		Span:       span,
		Captures:   make(map[string]Role),
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: checkpoint,
			ResolvedTs:   checkpoint,
		},
	}
	// Count of captures that is in Stopping states.
	stoppingCount := 0
	committed := false
	for captureID, table := range tableStatus {
		if !r.Span.Eq(&table.Span) {
			return nil, r.inconsistentError(table, captureID,
				"schedulerv3: table id inconsistent")
		}
		r.updateCheckpointAndStats(table.Checkpoint, table.Stats)

		switch table.State {
		case tablepb.TableStateReplicating:
			if len(r.Primary) != 0 {
				return nil, r.multiplePrimaryError(
					table, captureID, "schedulerv3: multiple primary",
					zap.Any("primary", r.Primary),
					zap.Any("status", tableStatus))
			}
			// Recognize primary if it's table is in replicating state.
			err := r.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = r.promoteSecondary(captureID)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case tablepb.TableStatePreparing:
			// Recognize secondary if it's table is in preparing state.
			err := r.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case tablepb.TableStatePrepared:
			// Recognize secondary and Commit state if it's table is in prepared state.
			committed = true
			err := r.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case tablepb.TableStateStopping:
			// The capture is stopping the table. It is possible that the
			// capture is primary, and is still replicating data to downstream.
			// We need to wait its state becomes Stopped or Absent before
			// proceeding further scheduling.
			log.Warn("schedulerv3: found a stopping capture during initializing",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Int64("tableID", table.Span.TableID),
				zap.Any("replicationSet", r),
				zap.Any("status", tableStatus))
			err := r.setCapture(captureID, RoleUndetermined)
			if err != nil {
				return nil, errors.Trace(err)
			}
			stoppingCount++
		case tablepb.TableStateAbsent,
			tablepb.TableStateStopped:
			// Ignore stop state.
		default:
			log.Warn("schedulerv3: unknown table state",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Int64("tableID", table.Span.TableID),
				zap.Any("replicationSet", r),
				zap.Any("status", tableStatus))
		}
	}

	// Build state from primary, secondary and captures.
	if len(r.Primary) != 0 {
		r.State = ReplicationSetStateReplicating
	}
	// Move table or add table is in-progress.
	if r.hasRole(RoleSecondary) {
		r.State = ReplicationSetStatePrepare
	}
	// Move table or add table is committed.
	if committed {
		r.State = ReplicationSetStateCommit
	}
	if len(r.Captures) == 0 {
		r.State = ReplicationSetStateAbsent
	}
	if r.State == ReplicationSetStateUnknown && len(r.Captures) == stoppingCount {
		r.State = ReplicationSetStateRemoving
	}
	log.Info("schedulerv3: initialize replication set",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Any("replicationSet", r))

	return r, nil
}

func (r *ReplicationSet) hasRole(role Role) bool {
	_, has := r.getRole(role)
	return has
}

func (r *ReplicationSet) isInRole(captureID model.CaptureID, role Role) bool {
	rc, ok := r.Captures[captureID]
	if !ok {
		return false
	}
	return rc == role
}

func (r *ReplicationSet) getRole(role Role) (model.CaptureID, bool) {
	for captureID, cr := range r.Captures {
		if cr == role {
			return captureID, true
		}
	}
	return "", false
}

func (r *ReplicationSet) setCapture(captureID model.CaptureID, role Role) error {
	cr, ok := r.Captures[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not set %s as %s, it's %s, %v", captureID, role, cr, string(jsonR)))
	}
	r.Captures[captureID] = role
	return nil
}

func (r *ReplicationSet) clearCapture(captureID model.CaptureID, role Role) error {
	cr, ok := r.Captures[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not clear %s as %s, it's %s, %v", captureID, role, cr, string(jsonR)))
	}
	delete(r.Captures, captureID)
	return nil
}

func (r *ReplicationSet) promoteSecondary(captureID model.CaptureID) error {
	if r.Primary == captureID {
		log.Warn("schedulerv3: capture is already promoted as the primary",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.String("captureID", captureID),
			zap.Any("replicationSet", r))
		return nil
	}
	role, ok := r.Captures[captureID]
	if ok && role != RoleSecondary {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not promote %s to primary, it's %s, %v", captureID, role, string(jsonR)))
	}
	if r.Primary != "" {
		delete(r.Captures, r.Primary)
	}
	r.Primary = captureID
	r.Captures[r.Primary] = RolePrimary
	return nil
}

func (r *ReplicationSet) clearPrimary() {
	delete(r.Captures, r.Primary)
	r.Primary = ""
}

//nolint:unparam
func (r *ReplicationSet) inconsistentError(
	input *tablepb.TableStatus, captureID model.CaptureID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.String("captureID", captureID),
		zap.Stringer("tableState", input),
		zap.Any("replicationSet", r),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(
		fmt.Sprintf("tableID %d, %s", r.Span.TableID, msg))
}

func (r *ReplicationSet) multiplePrimaryError(
	input *tablepb.TableStatus, captureID model.CaptureID, msg string, fields ...zap.Field,
) error {
	fields = append(fields, []zap.Field{
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.String("captureID", captureID),
		zap.Stringer("tableState", input),
		zap.Any("replicationSet", r),
	}...)
	log.L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
	return errors.ErrReplicationSetMultiplePrimaryError.GenWithStackByArgs(
		fmt.Sprintf("tableID %d, %s", r.Span.TableID, msg))
}

// checkInvariant ensures ReplicationSet invariant is hold.
func (r *ReplicationSet) checkInvariant(
	input *tablepb.TableStatus, captureID model.CaptureID,
) error {
	if !r.Span.Eq(&input.Span) {
		return r.inconsistentError(input, captureID,
			"schedulerv3: tableID must be the same")
	}
	if len(r.Captures) == 0 {
		if r.State == ReplicationSetStatePrepare ||
			r.State == ReplicationSetStateCommit ||
			r.State == ReplicationSetStateReplicating {
			// When the state is in prepare, commit or replicating, there must
			// be at least one of primary and secondary.
			return r.inconsistentError(input, captureID,
				"schedulerv3: empty primary/secondary in state prepare/commit/replicating")
		}
	}
	roleP, okP := r.Captures[r.Primary]
	if (!okP && r.Primary != "") || // Primary is not in Captures.
		(okP && roleP != RolePrimary) { // Primary is not in primary role.
		return r.inconsistentError(input, captureID,
			"schedulerv3: capture inconsistent")
	}
	for captureID, role := range r.Captures {
		if role == RolePrimary && captureID != r.Primary {
			return r.multiplePrimaryError(input, captureID,
				"schedulerv3: capture inconsistent")
		}
	}
	return nil
}

// poll transit replication state based on input and the current state.
// See ReplicationSetState's comment for the state transition.
func (r *ReplicationSet) poll(
	input *tablepb.TableStatus, captureID model.CaptureID,
) ([]*schedulepb.Message, error) {
	if _, ok := r.Captures[captureID]; !ok {
		return nil, nil
	}

	msgBuf := make([]*schedulepb.Message, 0)
	stateChanged := true
	for stateChanged {
		err := r.checkInvariant(input, captureID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldState := r.State
		var msg *schedulepb.Message
		switch r.State {
		case ReplicationSetStateAbsent:
			stateChanged, err = r.pollOnAbsent(input, captureID)
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
				input, captureID, "schedulerv3: table state unknown")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if msg != nil {
			msgBuf = append(msgBuf, msg)
		}
		if stateChanged {
			log.Info("schedulerv3: replication state transition, poll",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Stringer("old", oldState),
				zap.Stringer("new", r.State))
		}
	}

	return msgBuf, nil
}

//nolint:unparam
func (r *ReplicationSet) pollOnAbsent(
	input *tablepb.TableStatus, captureID model.CaptureID,
) (bool, error) {
	switch input.State {
	case tablepb.TableStateAbsent:
		r.State = ReplicationSetStatePrepare
		err := r.setCapture(captureID, RoleSecondary)
		return true, errors.Trace(err)

	case tablepb.TableStateStopped:
		// Ignore stopped table state as a capture may shutdown unexpectedly.
		return false, nil
	case tablepb.TableStatePreparing,
		tablepb.TableStatePrepared,
		tablepb.TableStateReplicating,
		tablepb.TableStateStopping:
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return false, nil
}

func (r *ReplicationSet) pollOnPrepare(
	input *tablepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case tablepb.TableStateAbsent:
		if r.isInRole(captureID, RoleSecondary) {
			return &schedulepb.Message{
				To:      captureID,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							Span:        r.Span,
							IsSecondary: true,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, false, nil
		}
	case tablepb.TableStatePreparing:
		if r.isInRole(captureID, RoleSecondary) {
			// Ignore secondary Preparing, it may take a long time.
			return nil, false, nil
		}
	case tablepb.TableStatePrepared:
		if r.isInRole(captureID, RoleSecondary) {
			// Secondary is prepared, transit to Commit state.
			r.State = ReplicationSetStateCommit
			return nil, true, nil
		}
	case tablepb.TableStateReplicating:
		if r.Primary == captureID {
			r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			return nil, false, nil
		}
	case tablepb.TableStateStopping, tablepb.TableStateStopped:
		if r.Primary == captureID {
			// Primary is stopped, but we may still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("schedulerv3: primary is stopped during Prepare",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			r.clearPrimary()
			return nil, false, nil
		}
		if r.isInRole(captureID, RoleSecondary) {
			log.Info("schedulerv3: capture is stopped during Prepare",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			err := r.clearCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
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
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) pollOnCommit(
	input *tablepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case tablepb.TableStatePrepared:
		if r.isInRole(captureID, RoleSecondary) {
			if r.Primary != "" {
				// Secondary capture is prepared and waiting for stopping primary.
				// Send message to primary, ask for stopping.
				return &schedulepb.Message{
					To:      r.Primary,
					MsgType: schedulepb.MsgDispatchTableRequest,
					DispatchTableRequest: &schedulepb.DispatchTableRequest{
						Request: &schedulepb.DispatchTableRequest_RemoveTable{
							RemoveTable: &schedulepb.RemoveTableRequest{
								Span: r.Span,
							},
						},
					},
				}, false, nil
			}
			if r.hasRole(RoleUndetermined) {
				// There are other captures that have the table.
				// Must waiting for other captures become stopped or absent
				// before promoting the secondary, otherwise there may be two
				// primary that write data and lead to data inconsistency.
				log.Info("schedulerv3: there are unknown captures during commit",
					zap.String("namespace", r.Changefeed.Namespace),
					zap.String("changefeed", r.Changefeed.ID),
					zap.Any("replicationSet", r),
					zap.Stringer("tableState", input),
					zap.String("captureID", captureID))
				return nil, false, nil
			}
			// No primary, promote secondary to primary.
			err := r.promoteSecondary(captureID)
			if err != nil {
				return nil, false, errors.Trace(err)
			}

			log.Info("schedulerv3: promote secondary, no primary",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Any("replicationSet", r),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID))
		}
		// Secondary has been promoted, retry AddTableRequest.
		if r.Primary == captureID && !r.hasRole(RoleSecondary) {
			return &schedulepb.Message{
				To:      captureID,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							Span:        r.Span,
							IsSecondary: false,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, false, nil
		}

	case tablepb.TableStateStopped, tablepb.TableStateAbsent:
		if r.Primary == captureID {
			r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			original := r.Primary
			r.clearPrimary()
			if !r.hasRole(RoleSecondary) {
				// If there is no secondary, transit to Absent.
				log.Info("schedulerv3: primary is stopped during Commit",
					zap.String("namespace", r.Changefeed.Namespace),
					zap.String("changefeed", r.Changefeed.ID),
					zap.Stringer("tableState", input),
					zap.String("captureID", captureID),
					zap.Any("replicationSet", r))
				r.State = ReplicationSetStateAbsent
				return nil, true, nil
			}
			// Primary is stopped, promote secondary to primary.
			secondary, _ := r.getRole(RoleSecondary)
			err := r.promoteSecondary(secondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			log.Info("schedulerv3: replication state promote secondary",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Any("replicationSet", r),
				zap.Stringer("tableState", input),
				zap.String("original", original),
				zap.String("captureID", secondary))
			return &schedulepb.Message{
				To:      r.Primary,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							Span:        r.Span,
							IsSecondary: false,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, false, nil
		} else if r.isInRole(captureID, RoleSecondary) {
			// As it sends RemoveTableRequest to the original primary
			// upon entering Commit state. Do not change state and wait
			// the original primary reports its table.
			log.Info("schedulerv3: secondary is stopped during Commit",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			err := r.clearCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if r.Primary == "" {
				// If there is no primary, transit to Absent.
				r.State = ReplicationSetStateAbsent
			}
			return nil, true, nil
		} else if r.isInRole(captureID, RoleUndetermined) {
			log.Info("schedulerv3: capture is stopped during Commit",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			err := r.clearCapture(captureID, RoleUndetermined)
			return nil, false, errors.Trace(err)
		}

	case tablepb.TableStateReplicating:
		if r.Primary == captureID {
			r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			if r.hasRole(RoleSecondary) {
				// Original primary is not stopped, ask for stopping.
				return &schedulepb.Message{
					To:      captureID,
					MsgType: schedulepb.MsgDispatchTableRequest,
					DispatchTableRequest: &schedulepb.DispatchTableRequest{
						Request: &schedulepb.DispatchTableRequest_RemoveTable{
							RemoveTable: &schedulepb.RemoveTableRequest{
								Span: r.Span,
							},
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
			input, captureID, "schedulerv3: multiple primary")

	case tablepb.TableStateStopping:
		if r.Primary == captureID && r.hasRole(RoleSecondary) {
			r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			return nil, false, nil
		} else if r.isInRole(captureID, RoleUndetermined) {
			log.Info("schedulerv3: capture is stopping during Commit",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			return nil, false, nil
		}

	case tablepb.TableStatePreparing:
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

//nolint:unparam
func (r *ReplicationSet) pollOnReplicating(
	input *tablepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case tablepb.TableStateReplicating:
		if r.Primary == captureID {
			r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			return nil, false, nil
		}
		return nil, false, r.multiplePrimaryError(
			input, captureID, "schedulerv3: multiple primary")

	case tablepb.TableStateAbsent:
	case tablepb.TableStatePreparing:
	case tablepb.TableStatePrepared:
	case tablepb.TableStateStopping:
	case tablepb.TableStateStopped:
		if r.Primary == captureID {
			r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			// Primary is stopped, but we still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("schedulerv3: primary is stopped during Replicating",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", r))
			r.clearPrimary()
			r.State = ReplicationSetStateAbsent
			return nil, true, nil
		}
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

//nolint:unparam
func (r *ReplicationSet) pollOnRemoving(
	input *tablepb.TableStatus, captureID model.CaptureID,
) (*schedulepb.Message, bool, error) {
	switch input.State {
	case tablepb.TableStatePreparing,
		tablepb.TableStatePrepared,
		tablepb.TableStateReplicating:
		return &schedulepb.Message{
			To:      captureID,
			MsgType: schedulepb.MsgDispatchTableRequest,
			DispatchTableRequest: &schedulepb.DispatchTableRequest{
				Request: &schedulepb.DispatchTableRequest_RemoveTable{
					RemoveTable: &schedulepb.RemoveTableRequest{
						Span: r.Span,
					},
				},
			},
		}, false, nil
	case tablepb.TableStateAbsent, tablepb.TableStateStopped:
		var err error
		if r.Primary == captureID {
			r.clearPrimary()
		} else if r.isInRole(captureID, RoleSecondary) {
			err = r.clearCapture(captureID, RoleSecondary)
		} else {
			err = r.clearCapture(captureID, RoleUndetermined)
		}
		if err != nil {
			log.Warn("schedulerv3: replication state remove capture with error",
				zap.String("namespace", r.Changefeed.Namespace),
				zap.String("changefeed", r.Changefeed.ID),
				zap.Any("replicationSet", r),
				zap.Stringer("tableState", input),
				zap.String("captureID", captureID),
				zap.Error(err))
		}
		return nil, false, nil
	case tablepb.TableStateStopping:
		return nil, false, nil
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Stringer("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", r))
	return nil, false, nil
}

func (r *ReplicationSet) handleTableStatus(
	from model.CaptureID, status *tablepb.TableStatus,
) ([]*schedulepb.Message, error) {
	return r.poll(status, from)
}

func (r *ReplicationSet) handleAddTable(
	captureID model.CaptureID,
) ([]*schedulepb.Message, error) {
	// Ignore add table if it's not in Absent state.
	if r.State != ReplicationSetStateAbsent {
		log.Warn("schedulerv3: add table is ignored",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	err := r.setCapture(captureID, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldState := r.State
	status := tablepb.TableStatus{
		Span:       r.Span,
		State:      tablepb.TableStateAbsent,
		Checkpoint: tablepb.Checkpoint{},
	}
	msgs, err := r.poll(&status, captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("schedulerv3: replication state transition, add table",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState), zap.Stringer("new", r.State))
	return msgs, nil
}

func (r *ReplicationSet) handleMoveTable(
	dest model.CaptureID,
) ([]*schedulepb.Message, error) {
	// Ignore move table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("schedulerv3: move table is ignored",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	// Ignore move table if
	// 1) it's not in Replicating state or
	// 2) the dest capture is the primary.
	if r.State != ReplicationSetStateReplicating || r.Primary == dest {
		log.Warn("schedulerv3: move table is ignored",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	oldState := r.State
	r.State = ReplicationSetStatePrepare
	err := r.setCapture(dest, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("schedulerv3: replication state transition, move table",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Stringer("new", r.State),
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState))
	status := tablepb.TableStatus{
		Span:       r.Span,
		State:      tablepb.TableStateAbsent,
		Checkpoint: tablepb.Checkpoint{},
	}
	return r.poll(&status, dest)
}

func (r *ReplicationSet) handleRemoveTable() ([]*schedulepb.Message, error) {
	// Ignore remove table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	// Ignore remove table if it's not in Replicating state.
	if r.State != ReplicationSetStateReplicating {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	oldState := r.State
	r.State = ReplicationSetStateRemoving
	log.Info("schedulerv3: replication state transition, remove table",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Int64("tableID", r.Span.TableID),
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState))
	status := tablepb.TableStatus{
		Span:  r.Span,
		State: tablepb.TableStateReplicating,
		Checkpoint: tablepb.Checkpoint{
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

// handleCaptureShutdown handle capture shutdown event.
// Besides returning messages and errors, it also returns a bool to indicate
// whether r is affected by the capture shutdown.
func (r *ReplicationSet) handleCaptureShutdown(
	captureID model.CaptureID,
) ([]*schedulepb.Message, bool, error) {
	_, ok := r.Captures[captureID]
	if !ok {
		// r is not affected by the capture shutdown.
		return nil, false, nil
	}
	// The capture has shutdown, the table has stopped.
	status := tablepb.TableStatus{
		Span:  r.Span,
		State: tablepb.TableStateStopped,
	}
	oldState := r.State
	msgs, err := r.poll(&status, captureID)
	log.Info("schedulerv3: replication state transition, capture shutdown",
		zap.String("namespace", r.Changefeed.Namespace),
		zap.String("changefeed", r.Changefeed.ID),
		zap.Int64("tableID", r.Span.TableID),
		zap.Any("replicationSet", r),
		zap.Stringer("old", oldState), zap.Stringer("new", r.State))
	return msgs, true, errors.Trace(err)
}

func (r *ReplicationSet) updateCheckpointAndStats(
	checkpoint tablepb.Checkpoint, stats tablepb.Stats,
) {
	if checkpoint.ResolvedTs < checkpoint.CheckpointTs {
		log.Warn("schedulerv3: resolved ts should not less than checkpoint ts",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r),
			zap.Any("checkpoint", checkpoint))

		// TODO: resolvedTs should not be zero, but we have to handle it for now.
		if checkpoint.ResolvedTs == 0 {
			checkpoint.ResolvedTs = checkpoint.CheckpointTs
		}
	}
	if r.Checkpoint.CheckpointTs < checkpoint.CheckpointTs {
		r.Checkpoint.CheckpointTs = checkpoint.CheckpointTs
	}
	if r.Checkpoint.ResolvedTs < checkpoint.ResolvedTs {
		r.Checkpoint.ResolvedTs = checkpoint.ResolvedTs
	}
	if r.Checkpoint.ResolvedTs < r.Checkpoint.CheckpointTs {
		log.Warn("schedulerv3: resolved ts should not less than checkpoint ts",
			zap.String("namespace", r.Changefeed.Namespace),
			zap.String("changefeed", r.Changefeed.ID),
			zap.Int64("tableID", r.Span.TableID),
			zap.Any("replicationSet", r),
			zap.Any("checkpointTs", r.Checkpoint.CheckpointTs),
			zap.Any("resolvedTs", r.Checkpoint.ResolvedTs))
	}

	if r.Checkpoint.LastSyncedTs < checkpoint.LastSyncedTs {
		r.Checkpoint.LastSyncedTs = checkpoint.LastSyncedTs
	}

	// we only update stats when stats is not empty, because we only collect stats every 10s.
	if stats.Size() > 0 {
		r.Stats = stats
	}
}

// SetHeap is a max-heap, it implements heap.Interface.
type SetHeap []*ReplicationSet

// NewReplicationSetHeap creates a new SetHeap.
func NewReplicationSetHeap(capacity int) SetHeap {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	return make(SetHeap, 0, capacity)
}

// Len returns the length of the heap.
func (h SetHeap) Len() int { return len(h) }

// Less returns true if the element at i is less than the element at j.
func (h SetHeap) Less(i, j int) bool {
	if h[i].Checkpoint.CheckpointTs > h[j].Checkpoint.CheckpointTs {
		return true
	}
	if h[i].Checkpoint.CheckpointTs == h[j].Checkpoint.CheckpointTs {
		return h[i].Checkpoint.ResolvedTs > h[j].Checkpoint.ResolvedTs
	}
	return false
}

// Swap swaps the elements with indexes i and j.
func (h SetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes an element to the heap.
func (h *SetHeap) Push(x interface{}) {
	*h = append(*h, x.(*ReplicationSet))
}

// Pop pops an element from the heap.
func (h *SetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}
