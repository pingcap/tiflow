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
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/schedulepb"
	"github.com/stretchr/testify/require"
)

// See https://stackoverflow.com/a/30230552/3920448 for details.
func nextPerm(p []int) {
	for i := len(p) - 1; i >= 0; i-- {
		if i == 0 || p[i] < len(p)-i-1 {
			p[i]++
			return
		}
		p[i] = 0
	}
}

func getPerm(orig, p []int) []int {
	result := append([]int{}, orig...)
	for i, v := range p {
		result[i], result[i+v] = result[i+v], result[i]
	}
	return result
}

func iterPermutation(sequence []int, fn func(sequence []int)) {
	for p := make([]int, len(sequence)); p[0] < len(p); nextPerm(p) {
		fn(getPerm(sequence, p))
	}
}

func TestNewReplicationSet(t *testing.T) {
	testcases := []struct {
		set         *ReplicationSet
		checkpoint  model.Ts
		tableStatus map[model.CaptureID]*schedulepb.TableStatus
	}{
		{
			set: &ReplicationSet{
				State:    ReplicationSetStateAbsent,
				Captures: map[string]CaptureRole{},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{},
		},
		{
			set: &ReplicationSet{
				Primary:  "1",
				State:    ReplicationSetStateReplicating,
				Captures: map[string]CaptureRole{"1": CaptureRolePrimary},
				Checkpoint: schedulepb.Checkpoint{
					CheckpointTs: 2, ResolvedTs: 2,
				},
			},
			checkpoint: 2,
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State: schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{
						CheckpointTs: 1, ResolvedTs: 1,
					},
				},
			},
		},
		{
			// Rebuild add table state.
			set: &ReplicationSet{
				State:     ReplicationSetStatePrepare,
				Secondary: "1",
				Captures:  map[string]CaptureRole{"1": CaptureRoleSecondary},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStatePreparing,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Prepare.
			set: &ReplicationSet{
				State:     ReplicationSetStatePrepare,
				Primary:   "2",
				Secondary: "1",
				Captures: map[string]CaptureRole{
					"1": CaptureRoleSecondary, "2": CaptureRolePrimary,
				},
				Checkpoint: schedulepb.Checkpoint{CheckpointTs: 2},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStatePreparing,
					Checkpoint: schedulepb.Checkpoint{CheckpointTs: 1},
				},
				"2": {
					State:      schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{CheckpointTs: 2},
				},
			},
		},
		{
			// Rebuild move table state, Commit.
			set: &ReplicationSet{
				State:     ReplicationSetStateCommit,
				Primary:   "2",
				Secondary: "1",
				Captures: map[string]CaptureRole{
					"1": CaptureRoleSecondary, "2": CaptureRolePrimary,
				},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStatePrepared,
					Checkpoint: schedulepb.Checkpoint{},
				},
				"2": {
					State:      schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Commit, original primary stopping.
			set: &ReplicationSet{
				State:     ReplicationSetStateCommit,
				Secondary: "1",
				Captures: map[string]CaptureRole{
					"1": CaptureRoleSecondary, "2": CaptureRoleSecondary,
				},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStatePrepared,
					Checkpoint: schedulepb.Checkpoint{},
				},
				"2": {
					State:      schedulepb.TableStateStopping,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Commit, original primary stopped.
			set: &ReplicationSet{
				State:     ReplicationSetStateCommit,
				Secondary: "1",
				Captures:  map[string]CaptureRole{"1": CaptureRoleSecondary},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStatePrepared,
					Checkpoint: schedulepb.Checkpoint{},
				},
				"2": {
					State:      schedulepb.TableStateStopped,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild remove table state, Removing.
			set: &ReplicationSet{
				State: ReplicationSetStateRemoving,
				Captures: map[string]CaptureRole{
					"1": CaptureRoleSecondary, "2": CaptureRoleSecondary,
				},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStateStopping,
					Checkpoint: schedulepb.Checkpoint{},
				},
				"2": {
					State:      schedulepb.TableStateStopping,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Multiple primary error.
			set: nil,
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{},
				},
				"2": {
					State:      schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
	}
	for id, tc := range testcases {
		set := tc.set
		status := tc.tableStatus
		checkpoint := tc.checkpoint

		output, err := NewReplicationSet(0, checkpoint, status, model.ChangeFeedID{})
		if set == nil {
			require.Error(t, err)
		} else {
			require.Nil(t, err)
			require.EqualValuesf(t, set, output, "%d", id)
		}
	}
}

// Test all table states and replication states.
func TestReplicationSetPoll(t *testing.T) {
	var testcases []map[string]schedulepb.TableState
	for state1 := range schedulepb.TableState_name {
		for state2 := range schedulepb.TableState_name {
			if state1 == state2 && state1 == int32(schedulepb.TableStateReplicating) {
				continue
			}
			tc := map[string]schedulepb.TableState{
				"1": schedulepb.TableState(state1),
				"2": schedulepb.TableState(state2),
			}
			testcases = append(testcases, tc)
		}
	}
	seed := time.Now().Unix()
	rnd := rand.New(rand.NewSource(seed))
	rnd.Shuffle(len(testcases), func(i, j int) {
		testcases[i], testcases[j] = testcases[j], testcases[i]
	})
	// It takes minutes to complete all test cases.
	// To speed up, we only test the first 2 cases.
	testcases = testcases[:2]

	from := "1"
	for _, states := range testcases {
		status := make(map[string]*schedulepb.TableStatus)
		for id, state := range states {
			status[id] = &schedulepb.TableStatus{
				TableID:    1,
				State:      state,
				Checkpoint: schedulepb.Checkpoint{},
			}
		}
		r, _ := NewReplicationSet(1, 0, status, model.ChangeFeedID{})
		var tableStates []int
		for state := range schedulepb.TableState_name {
			tableStates = append(tableStates, int(state))
		}
		input := &schedulepb.TableStatus{TableID: model.TableID(1)}
		iterPermutation(tableStates, func(tableStateSequence []int) {
			t.Logf("test %d, %v, %v", seed, status, tableStateSequence)
			for _, state := range tableStateSequence {
				input.State = schedulepb.TableState(state)
				msgs, _ := r.poll(input, from)
				for i := range msgs {
					if msgs[i] == nil {
						t.Errorf("nil messages: %v, input: %v, from: %s, r: %v",
							msgs, *input, from, *r)
					}
				}
				// For now, poll() is expected to output at most one message.
				if len(msgs) > 1 {
					t.Errorf("too many messages: %v, input: %v, from: %s, r: %v",
						msgs, *input, from, *r)
				}
			}
		})
	}
}

func TestReplicationSetPollUnknownCapture(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, map[model.CaptureID]*schedulepb.TableStatus{
		"1": {
			TableID:    tableID,
			State:      schedulepb.TableStateReplicating,
			Checkpoint: schedulepb.Checkpoint{},
		},
	}, model.ChangeFeedID{})
	require.Nil(t, err)

	msgs, err := r.poll(&schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	}, "unknown")
	require.Nil(t, msgs)
	require.Nil(t, err)

	msgs, err = r.poll(&schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
	}, "unknown")
	require.Len(t, msgs, 0)
	require.Nil(t, err)

	msgs, err = r.poll(&schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	}, "unknown")
	require.Len(t, msgs, 0)
	require.Nil(t, err)
}

func TestReplicationSetAddTable(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	// Absent -> Prepare
	msgs, err := r.handleAddTable(from)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, from, r.Secondary)

	// No-op if add table again.
	msgs, err = r.handleAddTable(from)
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// AddTableRequest is lost somehow, send AddTableRequest again.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, from, r.Secondary)

	// Prepare is in-progress.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePreparing,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, from, r.Secondary)

	// Prepare -> Commit.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, "", r.Secondary)
	// The secondary AddTable request may be lost.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, "", r.Secondary)

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, "", r.Secondary)

	// Replicating -> Replicating
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   4,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, "", r.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)
}

func TestReplicationSetRemoveTable(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	// Ignore removing table if it's not in replicating.
	msgs, err := r.handleRemoveTable()
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// Replicating -> Removing
	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.setSecondary(from))
	require.Nil(t, r.promoteSecondary(from))
	msgs, err = r.handleRemoveTable()
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{TableID: r.TableID},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.False(t, r.hasRemoved())

	// Ignore remove table if it's in-progress.
	msgs, err = r.handleRemoveTable()
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Removing is in-progress.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.False(t, r.hasRemoved())

	// Removed if the table is absent.
	rClone := clone(r)
	msgs, err = rClone.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateRemoving, rClone.State)
	require.True(t, rClone.hasRemoved())

	// Removed if the table is stopped.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.True(t, r.hasRemoved())
}

func clone(r *ReplicationSet) *ReplicationSet {
	rClone := *r
	rClone.Captures = make(map[string]CaptureRole)
	for captureID, role := range r.Captures {
		rClone.Captures[captureID] = role
	}
	return &rClone
}

func TestReplicationSetMoveTable(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	source := "1"
	dest := "2"
	// Ignore removing table if it's not in replicating.
	r.State = ReplicationSetStatePrepare
	require.Nil(t, r.setSecondary(source))
	msgs, err := r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.NotContains(t, r.Captures, dest)

	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.promoteSecondary(source))

	// Replicating -> Prepare
	msgs, err = r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, dest, r.Secondary)
	require.Equal(t, source, r.Primary)

	// No-op if add table again.
	msgs, err = r.handleAddTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Source primary sends heartbeat response
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: 1,
			ResolvedTs:   1,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 1,
		ResolvedTs:   1,
	}, r.Checkpoint)

	// AddTableRequest is lost somehow, send AddTableRequest again.
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, dest, r.Secondary)

	// Prepare -> Commit.
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      source,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{TableID: r.TableID},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.Equal(t, dest, r.Secondary)

	// Source updates it's table status
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: 2,
			ResolvedTs:   3,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1, "%v", r)
	require.EqualValues(t, &schedulepb.Message{
		To:      source,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{TableID: r.TableID},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.Equal(t, dest, r.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 2,
		ResolvedTs:   3,
	}, r.Checkpoint)

	// Removing source is in-progress.
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopping,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   3,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.Equal(t, dest, r.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   3,
	}, r.Checkpoint)

	// Source is removed.
	rClone := clone(r)
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopped,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   4,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)

	// Source stopped message is lost somehow.
	// rClone has checkpoint ts 3, resolved ts 3
	msgs, err = rClone.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					TableID:     r.TableID,
					IsSecondary: false,
					Checkpoint: schedulepb.Checkpoint{
						CheckpointTs: 3,
						ResolvedTs:   3,
					},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, rClone.State)
	require.Equal(t, dest, rClone.Primary)
	require.Equal(t, "", rClone.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   3,
	}, rClone.Checkpoint)

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)
}

func TestReplicationSetCaptureShutdown(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	// Add table, Absent -> Prepare
	msgs, err := r.handleAddTable(from)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
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
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, from, r.Secondary)

	affected := false
	// Secondary shutdown during Prepare, Prepare -> Absent
	t.Run("AddTableSecondaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Add table, Prepare -> Commit
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, "", r.Secondary)

	// Secondary shutdown during Commit, Commit -> Absent
	t.Run("AddTableSecondaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Add table, Commit -> Replicating
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, "", r.Secondary)

	// Primary shutdown during Replicating, Replicating -> Absent
	t.Run("AddTablePrimaryShutdownDuringReplicating", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Move table, Replicating -> Prepare
	dest := "2"
	msgs, err = r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, dest, r.Secondary)

	// Primary shutdown during Prepare, Prepare -> Prepare
	t.Run("MoveTablePrimaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]CaptureRole{dest: CaptureRoleSecondary}, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, dest, rClone.Secondary)
		require.Equal(t, ReplicationSetStatePrepare, rClone.State)
		// Secondary shutdown after primary shutdown, Prepare -> Absent
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Secondary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})
	// Primary shutdown during Prepare, Prepare -> Prepare
	t.Run("MoveTableSecondaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Secondary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]CaptureRole{from: CaptureRolePrimary}, rClone.Captures)
		require.Equal(t, from, rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateReplicating, rClone.State)
	})

	// Move table, Prepare -> Commit
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.Equal(t, dest, r.Secondary)

	// Original primary shutdown during Commit, Commit -> Commit
	t.Run("MoveTableOriginalPrimaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 1)
		require.EqualValues(t, &schedulepb.Message{
			To:      dest,
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
		}, msgs[0])
		require.EqualValues(t, map[string]CaptureRole{dest: CaptureRolePrimary}, rClone.Captures)
		require.Equal(t, dest, rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateCommit, rClone.State)
		// New primary shutdown after original primary shutdown, Commit -> Absent
		msgs, affected, err = rClone.handleCaptureShutdown(dest)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Secondary shutdown during Commit, Commit -> Commit
	t.Run("MoveTableSecondaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Secondary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]CaptureRole{from: CaptureRolePrimary}, rClone.Captures)
		require.Equal(t, from, rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateCommit, rClone.State)

		// Original primary is still replicating, Commit -> Replicating
		t.Run("OriginalPrimaryReplicating", func(t *testing.T) {
			rClone1 := clone(rClone)
			msgs, err = rClone1.handleTableStatus(rClone1.Primary, &schedulepb.TableStatus{
				TableID: 1,
				State:   schedulepb.TableStateReplicating,
			})
			require.Nil(t, err)
			require.Len(t, msgs, 0)
			require.EqualValues(
				t, map[string]CaptureRole{from: CaptureRolePrimary}, rClone1.Captures)
			require.Equal(t, from, rClone1.Primary)
			require.Equal(t, "", rClone1.Secondary)
			require.Equal(t, ReplicationSetStateReplicating, rClone1.State)
		})

		// Original primary is stopped, Commit -> Absent
		t.Run("OriginalPrimaryStopped", func(t *testing.T) {
			rClone1 := clone(rClone)
			msgs, err = rClone1.handleTableStatus(rClone1.Primary, &schedulepb.TableStatus{
				TableID: 1,
				State:   schedulepb.TableStateStopped,
			})
			require.Nil(t, err)
			require.Len(t, msgs, 0)
			require.Empty(t, rClone1.Captures)
			require.Equal(t, "", rClone1.Primary)
			require.Equal(t, "", rClone1.Secondary)
			require.Equal(t, ReplicationSetStateAbsent, rClone1.State)
		})

		// Original primary is absent, Commit -> Absent,
		// and then add the original primary back, Absent -> Prepare
		t.Run("OriginalPrimaryAbsent", func(t *testing.T) {
			rClone1 := clone(rClone)
			msgs, err = rClone1.handleTableStatus(rClone1.Primary, &schedulepb.TableStatus{
				TableID: 1,
				State:   schedulepb.TableStateAbsent,
			})
			require.Nil(t, err)
			require.Len(t, msgs, 1)
			require.EqualValues(t, &schedulepb.Message{
				To:      from,
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
			}, msgs[0])
			require.Contains(t, rClone1.Captures, from)
			require.Equal(t, "", rClone1.Primary)
			require.Equal(t, from, rClone1.Secondary)
			require.Equal(t, ReplicationSetStatePrepare, rClone1.State)
		})
	})

	// Move table, original primary is stopped.
	msgs, err = r.handleTableStatus(from, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)
	t.Run("MoveTableNewPrimaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)

	// Unknown capture shutdown has no effect.
	t.Run("UnknownCaptureShutdown", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown("unknown")
		require.Nil(t, err)
		require.False(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, r, rClone)
	})
}

func TestReplicationSetCaptureShutdownAfterReconstructCommitState(t *testing.T) {
	t.Parallel()

	// Reconstruct commit state
	from := "1"
	tableID := model.TableID(1)
	tableStatus := map[model.CaptureID]*schedulepb.TableStatus{
		from: {TableID: tableID, State: schedulepb.TableStatePrepared},
	}
	r, err := NewReplicationSet(tableID, 0, tableStatus, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "", r.Primary)
	require.Equal(t, from, r.Secondary)

	// Commit -> Absent as there is no primary nor secondary.
	msg, affected, err := r.handleCaptureShutdown(from)
	require.Nil(t, err)
	require.True(t, affected)
	require.Empty(t, msg)
	require.Equal(t, ReplicationSetStateAbsent, r.State)
	require.Equal(t, "", r.Primary)
	require.Equal(t, "", r.Secondary)
}

func TestReplicationSetMoveTableWithHeartbeatResponse(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	source := "1"
	dest := "2"
	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.setSecondary(source))
	require.Nil(t, r.promoteSecondary(source))

	// Replicating -> Prepare
	msgs, err := r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, dest, r.Secondary)
	require.Equal(t, source, r.Primary)

	// Prepare -> Commit.
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.Equal(t, dest, r.Secondary)

	// Source updates it's table status
	// Source is removed.
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopped,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   4,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)

	// Source sends a heartbeat response.
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)
	require.Equal(t, schedulepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(dest, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)
}

func TestReplicationSetMarshalJSON(t *testing.T) {
	t.Parallel()

	b, err := json.Marshal(ReplicationSet{State: ReplicationSetStateReplicating})
	require.Nil(t, err)
	require.Contains(t, string(b), "Replicating", string(b))
}

func TestReplicationSetMoveTableSameDestCapture(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	r, err := NewReplicationSet(tableID, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	source := "1"
	dest := source
	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.setSecondary(source))
	require.Nil(t, r.promoteSecondary(source))

	// Ignore move table.
	msgs, err := r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, "", r.Secondary)
	require.Equal(t, source, r.Primary)
}

func TestReplicationSetCommitRestart(t *testing.T) {
	t.Parallel()

	// Primary has received remove table message and is currently stopping.
	tableStatus := map[model.CaptureID]*schedulepb.TableStatus{
		"1": {
			State:      schedulepb.TableStatePrepared,
			Checkpoint: schedulepb.Checkpoint{},
		},
		"2": {
			State:      schedulepb.TableStateStopping,
			Checkpoint: schedulepb.Checkpoint{},
		},
	}
	r, err := NewReplicationSet(0, 0, tableStatus, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "1", r.Secondary)
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "2")

	// Can not promote to primary as there are other captures.
	msgs, err := r.handleTableStatus("1", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "1", r.Secondary)
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "2")

	// Table status reported by other captures does not change replication set.
	msgs, err = r.handleTableStatus("2", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "1", r.Secondary)
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "2")

	// Only Stopped or Absent allows secondary to be promoted.
	rClone := clone(r)
	msgs, err = rClone.handleTableStatus("2", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, rClone.State)
	require.Equal(t, "1", rClone.Secondary)
	require.Equal(t, "", rClone.Primary)
	require.NotContains(t, rClone.Captures, "2")
	msgs, err = r.handleTableStatus("2", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "1", r.Secondary)
	require.Equal(t, "", r.Primary)
	require.NotContains(t, r.Captures, "2")

	// No other captures, promote secondary.
	msgs, err = r.handleTableStatus("1", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "1", msgs[0].To)
	require.False(t, msgs[0].DispatchTableRequest.GetAddTable().IsSecondary)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "1", r.Primary)
	require.Equal(t, "", r.Secondary)
}

func TestReplicationSetRemoveRestart(t *testing.T) {
	t.Parallel()

	// Primary has received remove table message and is currently stopping.
	tableStatus := map[model.CaptureID]*schedulepb.TableStatus{
		"1": {
			State:      schedulepb.TableStateStopping,
			Checkpoint: schedulepb.Checkpoint{},
		},
		"2": {
			State:      schedulepb.TableStateStopping,
			Checkpoint: schedulepb.Checkpoint{},
		},
	}
	r, err := NewReplicationSet(0, 0, tableStatus, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.Equal(t, "", r.Secondary)
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "1")
	require.Contains(t, r.Captures, "2")
	require.False(t, r.hasRemoved())

	// A capture reports its status.
	msgs, err := r.handleTableStatus("2", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// A capture stopped.
	msgs, err = r.handleTableStatus("2", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// Another capture stopped too.
	msgs, err = r.handleTableStatus("1", &schedulepb.TableStatus{
		TableID: 0,
		State:   schedulepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.True(t, r.hasRemoved())
}
