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
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
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
		tableStatus map[model.CaptureID]*schedulepb.TableStatus
	}{
		{
			set: &ReplicationSet{
				State:    ReplicationSetStateAbsent,
				Captures: map[string]struct{}{},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{},
		},
		{
			set: &ReplicationSet{
				Primary:  "1",
				State:    ReplicationSetStateReplicating,
				Captures: map[string]struct{}{"1": {}},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild add table state.
			set: &ReplicationSet{
				State:     ReplicationSetStatePrepare,
				Secondary: "1",
				Captures:  map[string]struct{}{"1": {}},
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
				Captures:  map[string]struct{}{"1": {}, "2": {}},
			},
			tableStatus: map[model.CaptureID]*schedulepb.TableStatus{
				"1": {
					State:      schedulepb.TableStatePreparing,
					Checkpoint: schedulepb.Checkpoint{},
				},
				"2": {
					State:      schedulepb.TableStateReplicating,
					Checkpoint: schedulepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Commit.
			set: &ReplicationSet{
				State:     ReplicationSetStateCommit,
				Primary:   "2",
				Secondary: "1",
				Captures:  map[string]struct{}{"1": {}, "2": {}},
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
				Captures:  map[string]struct{}{"1": {}},
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
				Captures:  map[string]struct{}{"1": {}},
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
				State:    ReplicationSetStateAbsent,
				Captures: map[string]struct{}{},
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

		output, err := newReplicationSet(0, status)
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
		r, _ := newReplicationSet(1, status)
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
	r, err := newReplicationSet(tableID, map[model.CaptureID]*schedulepb.TableStatus{
		"1": {
			TableID:    tableID,
			State:      schedulepb.TableStateReplicating,
			Checkpoint: schedulepb.Checkpoint{},
		},
	})
	require.Nil(t, err)

	msgs, err := r.poll(&schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
	}, "unknown")
	require.Nil(t, msgs)
	require.Error(t, err)
}

func TestReplicationSetAddTable(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	r, err := newReplicationSet(tableID, nil)
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
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
}

func TestReplicationSetRemoveTable(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	r, err := newReplicationSet(tableID, nil)
	require.Nil(t, err)

	// Ignore removing table if it's not in replicating.
	msgs, err := r.handleRemoveTable()
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// Replicating -> Removing
	r.Captures[from] = struct{}{}
	r.Primary = from
	r.State = ReplicationSetStateReplicating
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

	// Removed
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
	rClone.Captures = make(map[string]struct{})
	for captureID := range r.Captures {
		rClone.Captures[captureID] = struct{}{}
	}
	return &rClone
}

func TestReplicationSetMoveTable(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	r, err := newReplicationSet(tableID, nil)
	require.Nil(t, err)

	source := "1"
	dest := "2"
	// Ignore removing table if it's not in replicating.
	r.State = ReplicationSetStatePrepare
	r.Secondary = source
	r.Captures[source] = struct{}{}
	msgs, err := r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.NotContains(t, r.Captures, dest)

	r.State = ReplicationSetStateReplicating
	r.Primary = source
	r.Secondary = ""

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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
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

	// Removing source is in-progress.
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.Equal(t, dest, r.Secondary)

	// Source is removed.
	rClone := clone(r)
	msgs, err = r.handleTableStatus(source, &schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateStopped,
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)

	// Source stopped message is lost somehow.
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.Equal(t, "", r.Secondary)

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
	r, err := newReplicationSet(tableID, nil)
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
					Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.Equal(t, from, r.Secondary)

	// Secondary shutdown during Prepare, Prepare -> Absent
	t.Run("AddTableSecondaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		msgs, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
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
		msgs, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
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
		msgs, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
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
		msgs, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]struct{}{dest: {}}, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, dest, rClone.Secondary)
		require.Equal(t, ReplicationSetStatePrepare, rClone.State)
		// Secondary shutdown after primary shutdown, Prepare -> Absent
		msgs, err = rClone.handleCaptureShutdown(rClone.Secondary)
		require.Nil(t, err)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})
	// Primary shutdown during Prepare, Prepare -> Prepare
	t.Run("MoveTableSecondaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		msgs, err = rClone.handleCaptureShutdown(rClone.Secondary)
		require.Nil(t, err)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]struct{}{from: {}}, rClone.Captures)
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
		msgs, err = rClone.handleCaptureShutdown(rClone.Primary)
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
						Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
					},
				},
			},
		}, msgs[0])
		require.EqualValues(t, map[string]struct{}{dest: {}}, rClone.Captures)
		require.Equal(t, dest, rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateCommit, rClone.State)
		// New primary shutdown after original primary shutdown, Commit -> Absent
		msgs, err = rClone.handleCaptureShutdown(dest)
		require.Nil(t, err)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.Equal(t, "", rClone.Secondary)
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Secondary shutdown during Commit, Commit -> Commit
	t.Run("MoveTableSecondaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, err = rClone.handleCaptureShutdown(rClone.Secondary)
		require.Nil(t, err)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]struct{}{from: {}}, rClone.Captures)
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
			require.EqualValues(t, map[string]struct{}{from: {}}, rClone1.Captures)
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
							Checkpoint:  &schedulepb.Checkpoint{CheckpointTs: r.CheckpointTs},
						},
					},
				},
			}, msgs[0])
			require.Empty(t, rClone1.Captures)
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
		msgs, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
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
}
