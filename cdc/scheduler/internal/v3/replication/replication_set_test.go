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
	"container/heap"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/spanz"
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
		tableStatus map[model.CaptureID]*tablepb.TableStatus
	}{
		{
			set: &ReplicationSet{
				State:    ReplicationSetStateAbsent,
				Captures: map[string]Role{},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{},
		},
		{
			set: &ReplicationSet{
				Primary:  "1",
				State:    ReplicationSetStateReplicating,
				Captures: map[string]Role{"1": RolePrimary},
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: 2, ResolvedTs: 2,
				},
			},
			checkpoint: 2,
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State: tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1, ResolvedTs: 1,
					},
				},
			},
		},
		{
			// Rebuild add table state.
			set: &ReplicationSet{
				State:    ReplicationSetStatePrepare,
				Captures: map[string]Role{"1": RoleSecondary},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State:      tablepb.TableStatePreparing,
					Checkpoint: tablepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Prepare.
			set: &ReplicationSet{
				State:   ReplicationSetStatePrepare,
				Primary: "2",
				Captures: map[string]Role{
					"1": RoleSecondary, "2": RolePrimary,
				},
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: 2,
					ResolvedTs:   2,
				},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State: tablepb.TableStatePreparing,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
				"2": {
					State: tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 2,
						ResolvedTs:   2,
					},
				},
			},
		},
		{
			// Rebuild move table state, Commit.
			set: &ReplicationSet{
				State:   ReplicationSetStateCommit,
				Primary: "2",
				Captures: map[string]Role{
					"1": RoleSecondary, "2": RolePrimary,
				},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State:      tablepb.TableStatePrepared,
					Checkpoint: tablepb.Checkpoint{},
				},
				"2": {
					State:      tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Commit, original primary stopping.
			set: &ReplicationSet{
				State: ReplicationSetStateCommit,
				Captures: map[string]Role{
					"1": RoleSecondary, "2": RoleUndetermined,
				},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State:      tablepb.TableStatePrepared,
					Checkpoint: tablepb.Checkpoint{},
				},
				"2": {
					State:      tablepb.TableStateStopping,
					Checkpoint: tablepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild move table state, Commit, original primary stopped.
			set: &ReplicationSet{
				State:    ReplicationSetStateCommit,
				Captures: map[string]Role{"1": RoleSecondary},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State:      tablepb.TableStatePrepared,
					Checkpoint: tablepb.Checkpoint{},
				},
				"2": {
					State:      tablepb.TableStateStopped,
					Checkpoint: tablepb.Checkpoint{},
				},
			},
		},
		{
			// Rebuild remove table state, Removing.
			set: &ReplicationSet{
				State: ReplicationSetStateRemoving,
				Captures: map[string]Role{
					"1": RoleUndetermined, "2": RoleUndetermined,
				},
			},
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State:      tablepb.TableStateStopping,
					Checkpoint: tablepb.Checkpoint{},
				},
				"2": {
					State:      tablepb.TableStateStopping,
					Checkpoint: tablepb.Checkpoint{},
				},
			},
		},
		{
			// Multiple primary error.
			set: nil,
			tableStatus: map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					State:      tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{},
				},
				"2": {
					State:      tablepb.TableStateReplicating,
					Checkpoint: tablepb.Checkpoint{},
				},
			},
		},
	}
	for id, tc := range testcases {
		set := tc.set
		status := tc.tableStatus
		checkpoint := tc.checkpoint

		span := tablepb.Span{TableID: 0}
		output, err := NewReplicationSet(span, checkpoint, status, model.ChangeFeedID{})
		if set == nil {
			require.Errorf(t, err, "%d", id)
		} else {
			require.Nilf(t, err, "%+v, %d", err, id)
			require.EqualValuesf(t, set, output, "%d", id)
		}
	}
}

// Test all table states and replication states.
func TestReplicationSetPoll(t *testing.T) {
	var testcases []map[string]tablepb.TableState
	for state1 := range tablepb.TableState_name {
		for state2 := range tablepb.TableState_name {
			if state1 == state2 && state1 == int32(tablepb.TableStateReplicating) {
				continue
			}
			tc := map[string]tablepb.TableState{
				"1": tablepb.TableState(state1),
				"2": tablepb.TableState(state2),
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
		status := make(map[string]*tablepb.TableStatus)
		for id, state := range states {
			status[id] = &tablepb.TableStatus{
				Span:       tablepb.Span{TableID: 1},
				State:      state,
				Checkpoint: tablepb.Checkpoint{},
			}
		}
		span := tablepb.Span{TableID: 1}
		r, _ := NewReplicationSet(span, 0, status, model.ChangeFeedID{})
		var tableStates []int
		for state := range tablepb.TableState_name {
			tableStates = append(tableStates, int(state))
		}
		input := &tablepb.TableStatus{Span: tablepb.Span{TableID: model.TableID(1)}}
		iterPermutation(tableStates, func(tableStateSequence []int) {
			t.Logf("test %d, %v, %v", seed, status, tableStateSequence)
			for _, state := range tableStateSequence {
				input.State = tablepb.TableState(state)
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
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, map[model.CaptureID]*tablepb.TableStatus{
		"1": {
			Span:       tablepb.Span{TableID: tableID},
			State:      tablepb.TableStateReplicating,
			Checkpoint: tablepb.Checkpoint{},
		},
	}, model.ChangeFeedID{})
	require.Nil(t, err)

	msgs, err := r.poll(&tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	}, "unknown")
	require.Nil(t, msgs)
	require.Nil(t, err)

	msgs, err = r.poll(&tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateAbsent,
	}, "unknown")
	require.Len(t, msgs, 0)
	require.Nil(t, err)

	msgs, err = r.poll(&tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	}, "unknown")
	require.Len(t, msgs, 0)
	require.Nil(t, err)
}

func TestReplicationSetAddTable(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, nil, model.ChangeFeedID{})
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
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: true,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(from, RoleSecondary))

	// No-op if add table again.
	msgs, err = r.handleAddTable(from)
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// AddTableRequest is lost somehow, send AddTableRequest again.
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: true,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(from, RoleSecondary))

	// Prepare is in-progress.
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePreparing,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(from, RoleSecondary))

	// Prepare -> Commit.
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: false,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
	// The secondary AddTable request may be lost.
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: false,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, from, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))

	// Replicating -> Replicating
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   4,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, from, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)
}

func TestReplicationSetRemoveTable(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	// Ignore removing table if it's not in replicating.
	msgs, err := r.handleRemoveTable()
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// Replicating -> Removing
	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.setCapture(from, RoleSecondary))
	require.Nil(t, r.promoteSecondary(from))
	msgs, err = r.handleRemoveTable()
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      from,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{
					Span: tablepb.Span{TableID: r.Span.TableID},
				},
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
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.False(t, r.hasRemoved())

	// Removed if the table is absent.
	rClone := clone(r)
	msgs, err = rClone.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateRemoving, rClone.State)
	require.True(t, rClone.hasRemoved())

	// Removed if the table is stopped.
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.True(t, r.hasRemoved())
}

func clone(r *ReplicationSet) *ReplicationSet {
	rClone := *r
	rClone.Captures = make(map[string]Role)
	for captureID, role := range r.Captures {
		rClone.Captures[captureID] = role
	}
	return &rClone
}

func TestReplicationSetMoveTable(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	source := "1"
	dest := "2"
	// Ignore removing table if it's not in replicating.
	r.State = ReplicationSetStatePrepare
	require.Nil(t, r.setCapture(source, RoleSecondary))
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
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: true,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(dest, RoleSecondary))
	require.Equal(t, source, r.Primary)

	// No-op if add table again.
	msgs, err = r.handleAddTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Source primary sends heartbeat response
	msgs, err = r.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: 1,
			ResolvedTs:   1,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 1,
		ResolvedTs:   1,
	}, r.Checkpoint)

	// AddTableRequest is lost somehow, send AddTableRequest again.
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: true,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(dest, RoleSecondary))

	// Prepare -> Commit.
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      source,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{Span: r.Span},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.True(t, r.isInRole(dest, RoleSecondary))

	// Source updates it's table status
	msgs, err = r.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
		Checkpoint: tablepb.Checkpoint{
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
				RemoveTable: &schedulepb.RemoveTableRequest{Span: r.Span},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.True(t, r.isInRole(dest, RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 2,
		ResolvedTs:   3,
	}, r.Checkpoint)

	// Removing source is in-progress.
	msgs, err = r.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopping,
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   3,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.True(t, r.isInRole(dest, RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   3,
	}, r.Checkpoint)

	// Source is removed.
	rClone := clone(r)
	msgs, err = r.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopped,
		Checkpoint: tablepb.Checkpoint{
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
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: false,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)

	// Source stopped message is lost somehow.
	// rClone has checkpoint ts 3, resolved ts 3
	msgs, err = rClone.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: false,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 3,
						ResolvedTs:   3,
					},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, rClone.State)
	require.Equal(t, dest, rClone.Primary)
	require.False(t, rClone.hasRole(RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   3,
	}, rClone.Checkpoint)

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
}

//nolint:tparallel
func TestReplicationSetCaptureShutdown(t *testing.T) {
	t.Parallel()

	from := "1"
	tableID := model.TableID(1)
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, nil, model.ChangeFeedID{})
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
					Span:        tablepb.Span{TableID: r.Span.TableID},
					IsSecondary: true,
					Checkpoint:  r.Checkpoint,
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(from, RoleSecondary))

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
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Add table, Prepare -> Commit
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))

	// Secondary shutdown during Commit, Commit -> Absent
	t.Run("AddTableSecondaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Add table, Commit -> Replicating
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, from, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))

	// Primary shutdown during Replicating, Replicating -> Absent
	t.Run("AddTablePrimaryShutdownDuringReplicating", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(from)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Move table, Replicating -> Prepare
	dest := "2"
	msgs, err = r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(dest, RoleSecondary))

	// Primary shutdown during Prepare, Prepare -> Prepare
	t.Run("MoveTablePrimaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]Role{dest: RoleSecondary}, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.True(t, rClone.isInRole(dest, RoleSecondary))
		require.Equal(t, ReplicationSetStatePrepare, rClone.State)
		// Secondary shutdown after primary shutdown, Prepare -> Absent
		secondary, ok := rClone.getRole(RoleSecondary)
		require.True(t, ok)
		msgs, affected, err = rClone.handleCaptureShutdown(secondary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})
	// Primary shutdown during Prepare, Prepare -> Prepare
	t.Run("MoveTableSecondaryShutdownDuringPrepare", func(t *testing.T) {
		rClone := clone(r)
		secondary, ok := rClone.getRole(RoleSecondary)
		require.True(t, ok)
		msgs, affected, err = rClone.handleCaptureShutdown(secondary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]Role{from: RolePrimary}, rClone.Captures)
		require.Equal(t, from, rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateReplicating, rClone.State)
	})

	// Move table, Prepare -> Commit
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, from, r.Primary)
	require.True(t, r.isInRole(dest, RoleSecondary))

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
						Span:        tablepb.Span{TableID: r.Span.TableID},
						IsSecondary: false,
						Checkpoint:  r.Checkpoint,
					},
				},
			},
		}, msgs[0])
		require.EqualValues(t, map[string]Role{dest: RolePrimary}, rClone.Captures)
		require.Equal(t, dest, rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateCommit, rClone.State)
		// New primary shutdown after original primary shutdown, Commit -> Absent
		msgs, affected, err = rClone.handleCaptureShutdown(dest)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Secondary shutdown during Commit, Commit -> Commit
	t.Run("MoveTableSecondaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		secondary, ok := rClone.getRole(RoleSecondary)
		require.True(t, ok)
		msgs, affected, err = rClone.handleCaptureShutdown(secondary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.EqualValues(t, map[string]Role{from: RolePrimary}, rClone.Captures)
		require.Equal(t, from, rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateCommit, rClone.State)

		// Original primary is still replicating, Commit -> Replicating
		t.Run("OriginalPrimaryReplicating", func(t *testing.T) {
			rClone1 := clone(rClone)
			msgs, err = rClone1.handleTableStatus(rClone1.Primary, &tablepb.TableStatus{
				Span:  tablepb.Span{TableID: 1},
				State: tablepb.TableStateReplicating,
			})
			require.Nil(t, err)
			require.Len(t, msgs, 0)
			require.EqualValues(
				t, map[string]Role{from: RolePrimary}, rClone1.Captures)
			require.Equal(t, from, rClone1.Primary)
			require.False(t, rClone1.hasRole(RoleSecondary))
			require.Equal(t, ReplicationSetStateReplicating, rClone1.State)
		})

		// Original primary is stopped, Commit -> Absent
		t.Run("OriginalPrimaryStopped", func(t *testing.T) {
			rClone1 := clone(rClone)
			msgs, err = rClone1.handleTableStatus(rClone1.Primary, &tablepb.TableStatus{
				Span:  tablepb.Span{TableID: 1},
				State: tablepb.TableStateStopped,
			})
			require.Nil(t, err)
			require.Len(t, msgs, 0)
			require.Empty(t, rClone1.Captures)
			require.Equal(t, "", rClone1.Primary)
			require.False(t, rClone1.hasRole(RoleSecondary))
			require.Equal(t, ReplicationSetStateAbsent, rClone1.State)
		})

		// Original primary is absent, Commit -> Absent,
		// and then add the original primary back, Absent -> Prepare
		t.Run("OriginalPrimaryAbsent", func(t *testing.T) {
			rClone1 := clone(rClone)
			msgs, err = rClone1.handleTableStatus(rClone1.Primary, &tablepb.TableStatus{
				Span:  tablepb.Span{TableID: 1},
				State: tablepb.TableStateAbsent,
			})
			require.Nil(t, err)
			require.Len(t, msgs, 1)
			require.EqualValues(t, &schedulepb.Message{
				To:      from,
				MsgType: schedulepb.MsgDispatchTableRequest,
				DispatchTableRequest: &schedulepb.DispatchTableRequest{
					Request: &schedulepb.DispatchTableRequest_AddTable{
						AddTable: &schedulepb.AddTableRequest{
							Span:        tablepb.Span{TableID: r.Span.TableID},
							IsSecondary: true,
							Checkpoint:  r.Checkpoint,
						},
					},
				},
			}, msgs[0])
			require.Contains(t, rClone1.Captures, from)
			require.Equal(t, "", rClone1.Primary)
			require.True(t, rClone1.isInRole(from, RoleSecondary))
			require.Equal(t, ReplicationSetStatePrepare, rClone1.State)
		})
	})

	// Move table, original primary is stopped.
	msgs, err = r.handleTableStatus(from, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
	t.Run("MoveTableNewPrimaryShutdownDuringCommit", func(t *testing.T) {
		rClone := clone(r)
		msgs, affected, err = rClone.handleCaptureShutdown(rClone.Primary)
		require.Nil(t, err)
		require.True(t, affected)
		require.Len(t, msgs, 0)
		require.Empty(t, rClone.Captures)
		require.Equal(t, "", rClone.Primary)
		require.False(t, rClone.hasRole(RoleSecondary))
		require.Equal(t, ReplicationSetStateAbsent, rClone.State)
	})

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))

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
	tableStatus := map[model.CaptureID]*tablepb.TableStatus{
		from: {Span: tablepb.Span{TableID: tableID}, State: tablepb.TableStatePrepared},
	}
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, tableStatus, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "", r.Primary)
	require.True(t, r.isInRole(from, RoleSecondary))

	// Commit -> Absent as there is no primary nor secondary.
	msg, affected, err := r.handleCaptureShutdown(from)
	require.Nil(t, err)
	require.True(t, affected)
	require.Empty(t, msg)
	require.Equal(t, ReplicationSetStateAbsent, r.State)
	require.Equal(t, "", r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
}

func TestReplicationSetMoveTableWithHeartbeatResponse(t *testing.T) {
	t.Parallel()

	tableID := model.TableID(1)
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	source := "1"
	dest := "2"
	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.setCapture(source, RoleSecondary))
	require.Nil(t, r.promoteSecondary(source))

	// Replicating -> Prepare
	msgs, err := r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStatePrepare, r.State)
	require.True(t, r.isInRole(dest, RoleSecondary))
	require.Equal(t, source, r.Primary)

	// Prepare -> Commit.
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, source, r.Primary)
	require.True(t, r.isInRole(dest, RoleSecondary))

	// Source updates it's table status
	// Source is removed.
	msgs, err = r.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateStopped,
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: 3,
			ResolvedTs:   4,
		},
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)

	// Source sends a heartbeat response.
	msgs, err = r.handleTableStatus(source, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
	require.Equal(t, tablepb.Checkpoint{
		CheckpointTs: 3,
		ResolvedTs:   4,
	}, r.Checkpoint)

	// Commit -> Replicating
	msgs, err = r.handleTableStatus(dest, &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: tableID},
		State: tablepb.TableStateReplicating,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.Equal(t, dest, r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
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
	span := tablepb.Span{TableID: tableID}
	r, err := NewReplicationSet(span, 0, nil, model.ChangeFeedID{})
	require.Nil(t, err)

	source := "1"
	dest := source
	r.State = ReplicationSetStateReplicating
	require.Nil(t, r.setCapture(source, RoleSecondary))
	require.Nil(t, r.promoteSecondary(source))

	// Ignore move table.
	msgs, err := r.handleMoveTable(dest)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.State)
	require.False(t, r.hasRole(RoleSecondary))
	require.Equal(t, source, r.Primary)
}

func TestReplicationSetCommitRestart(t *testing.T) {
	t.Parallel()

	// Primary has received remove table message and is currently stopping.
	tableStatus := map[model.CaptureID]*tablepb.TableStatus{
		"1": {
			State:      tablepb.TableStatePrepared,
			Checkpoint: tablepb.Checkpoint{},
		},
		"2": {
			State:      tablepb.TableStateStopping,
			Checkpoint: tablepb.Checkpoint{},
		},
	}
	span := tablepb.Span{TableID: 0}
	r, err := NewReplicationSet(span, 0, tableStatus, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.EqualValues(t, RoleSecondary, r.Captures["1"])
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "2")

	// Can not promote to primary as there are other captures.
	msgs, err := r.handleTableStatus("1", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.EqualValues(t, RoleSecondary, r.Captures["1"])
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "2")

	// Table status reported by other captures does not change replication set.
	msgs, err = r.handleTableStatus("2", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.EqualValues(t, RoleSecondary, r.Captures["1"])
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "2")

	// Only Stopped or Absent allows secondary to be promoted.
	rClone := clone(r)
	msgs, err = rClone.handleTableStatus("2", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, rClone.State)
	require.EqualValues(t, RoleSecondary, rClone.Captures["1"])
	require.Equal(t, "", rClone.Primary)
	require.NotContains(t, rClone.Captures, "2")
	msgs, err = r.handleTableStatus("2", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.EqualValues(t, RoleSecondary, r.Captures["1"])
	require.Equal(t, "", r.Primary)
	require.NotContains(t, r.Captures, "2")

	// No other captures, promote secondary.
	msgs, err = r.handleTableStatus("1", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStatePrepared,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "1", msgs[0].To)
	require.False(t, msgs[0].DispatchTableRequest.GetAddTable().IsSecondary)
	require.Equal(t, ReplicationSetStateCommit, r.State)
	require.Equal(t, "1", r.Primary)
	require.False(t, r.hasRole(RoleSecondary))
}

func TestReplicationSetRemoveRestart(t *testing.T) {
	t.Parallel()

	// Primary has received remove table message and is currently stopping.
	tableStatus := map[model.CaptureID]*tablepb.TableStatus{
		"1": {
			State:      tablepb.TableStateStopping,
			Checkpoint: tablepb.Checkpoint{},
		},
		"2": {
			State:      tablepb.TableStateStopping,
			Checkpoint: tablepb.Checkpoint{},
		},
	}
	span := tablepb.Span{TableID: 0}
	r, err := NewReplicationSet(span, 0, tableStatus, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateRemoving, r.State)
	require.False(t, r.hasRole(RoleSecondary))
	require.Equal(t, "", r.Primary)
	require.Contains(t, r.Captures, "1")
	require.Contains(t, r.Captures, "2")
	require.False(t, r.hasRemoved())

	// A capture reports its status.
	msgs, err := r.handleTableStatus("2", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStateStopping,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// A capture stopped.
	msgs, err = r.handleTableStatus("2", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStateStopped,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.False(t, r.hasRemoved())

	// Another capture stopped too.
	msgs, err = r.handleTableStatus("1", &tablepb.TableStatus{
		Span:  tablepb.Span{TableID: 0},
		State: tablepb.TableStateAbsent,
	})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.True(t, r.hasRemoved())
}

func TestReplicationSetHeap_Len(t *testing.T) {
	t.Parallel()

	h := NewReplicationSetHeap(defaultSlowTableHeapSize)
	require.Equal(t, 0, h.Len())

	h = append(h, &ReplicationSet{Span: spanz.TableIDToComparableSpan(0)})
	require.Equal(t, 1, h.Len())

	h = append(h, &ReplicationSet{Span: spanz.TableIDToComparableSpan(1)})
	require.Equal(t, 2, h.Len())
}

func TestReplicationSetHeap_Less(t *testing.T) {
	t.Parallel()

	h := NewReplicationSetHeap(defaultSlowTableHeapSize)
	h = append(h, &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(0),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 1},
	})
	h = append(h, &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(1),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 2, ResolvedTs: 3},
	})
	h = append(h, &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(2),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 2, ResolvedTs: 4},
	})
	require.True(t, h.Less(1, 0))
	require.True(t, h.Less(2, 1))
}

func TestReplicationSetHeap_Basic(t *testing.T) {
	t.Parallel()

	h := NewReplicationSetHeap(defaultSlowTableHeapSize)
	heap.Init(&h)
	heap.Push(&h, &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(0),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 1},
	})
	heap.Push(&h, &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(1),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 2},
	})
	require.Equal(t, 2, h.Len())

	require.Equal(t, int64(1), heap.Pop(&h).(*ReplicationSet).Span.TableID)
	require.Equal(t, 1, h.Len())

	require.Equal(t, int64(0), heap.Pop(&h).(*ReplicationSet).Span.TableID)
	require.Equal(t, 0, h.Len())
}

// TestReplicationSetHeap_MinK tests that the heap can be
// used to keep the min K elements.
func TestReplicationSetHeap_MinK(t *testing.T) {
	t.Parallel()

	// K = defaultSlowTableHeapSize
	h := NewReplicationSetHeap(defaultSlowTableHeapSize)
	heap.Init(&h)

	for i := 2 * defaultSlowTableHeapSize; i > 0; i-- {
		replicationSet := &ReplicationSet{
			Span:       spanz.TableIDToComparableSpan(int64(i)),
			Checkpoint: tablepb.Checkpoint{CheckpointTs: uint64(i)},
		}
		heap.Push(&h, replicationSet)
		if h.Len() > defaultSlowTableHeapSize {
			heap.Pop(&h)
		}
	}

	require.Equal(t, defaultSlowTableHeapSize, h.Len())

	expectedTables := make([]int64, 0)
	for i := defaultSlowTableHeapSize; i > 0; i-- {
		expectedTables = append(expectedTables, int64(i))
	}

	tables := make([]model.TableID, 0)
	tableCounts := h.Len()
	for i := 0; i < tableCounts; i++ {
		element := heap.Pop(&h).(*ReplicationSet)
		t.Log(element.Span)
		tables = append(tables, element.Span.TableID)
	}
	require.Equal(t, expectedTables, tables)
	require.Equal(t, 0, h.Len())
}

func TestUpdateCheckpointAndStats(t *testing.T) {
	cases := []struct {
		checkpoint tablepb.Checkpoint
		stats      tablepb.Stats
	}{
		{
			checkpoint: tablepb.Checkpoint{
				CheckpointTs: 1,
				ResolvedTs:   2,
			},
			stats: tablepb.Stats{},
		},
		{
			checkpoint: tablepb.Checkpoint{
				CheckpointTs: 2,
				ResolvedTs:   1,
			},
			stats: tablepb.Stats{},
		},
	}
	r := &ReplicationSet{}
	for _, c := range cases {
		r.updateCheckpointAndStats(c.checkpoint, c.stats)
	}
}
