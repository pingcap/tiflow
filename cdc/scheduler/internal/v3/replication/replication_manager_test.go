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
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestReplicationManagerHandleAddTableTask(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(10, model.ChangeFeedID{})
	addTableCh := make(chan int, 1)
	// Absent -> Prepare
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{
			Span: spanz.TableIDToComparableSpan(1), CaptureID: "1", CheckpointTs: 1,
		},
		Accept: func() {
			addTableCh <- 1
			close(addTableCh)
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      "1",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: true,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(1)))
	require.Equal(t, 1, <-addTableCh)

	// Ignore if add the table again.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept:   func() { t.Fatalf("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Prepare -> Commit.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    "1",
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status: &tablepb.TableStatus{
						Span:  spanz.TableIDToComparableSpan(1),
						State: tablepb.TableStatePrepared,
					},
				},
			},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      "1",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: false,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
			},
		},
	}, msgs[0])
	require.Equal(
		t, ReplicationSetStateCommit, r.spans.GetV(spanz.TableIDToComparableSpan(1)).State)
	require.Equal(t, "1", r.spans.GetV(spanz.TableIDToComparableSpan(1)).Primary)
	require.False(t, r.spans.GetV(spanz.TableIDToComparableSpan(1)).hasRole(RoleSecondary))

	// Commit -> Replicating through heartbeat response.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    "1",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{{
				Span:  spanz.TableIDToComparableSpan(1),
				State: tablepb.TableStateReplicating,
			}},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(
		t, ReplicationSetStateReplicating, r.spans.GetV(spanz.TableIDToComparableSpan(1)).State)
	require.Equal(t, "1", r.spans.GetV(spanz.TableIDToComparableSpan(1)).Primary)
	require.False(t, r.spans.GetV(spanz.TableIDToComparableSpan(1)).hasRole(RoleSecondary))

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks.GetV(spanz.TableIDToComparableSpan(1)))
}

func TestReplicationManagerRemoveTable(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(10, model.ChangeFeedID{})
	removeTableCh := make(chan int, 1)

	// Ignore remove table if there is no such table.
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		RemoveTable: &RemoveTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept:      func() { t.Fatal("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Add the table.
	span := spanz.TableIDToComparableSpan(1)
	tbl, err := NewReplicationSet(span, 0, map[string]*tablepb.TableStatus{
		"1": {Span: span, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateReplicating, tbl.State)
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(1), tbl)

	// Remove the table.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		RemoveTable: &RemoveTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept: func() {
			removeTableCh <- 1
			close(removeTableCh)
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      "1",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{Span: span},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(1)))
	require.Equal(t, 1, <-removeTableCh)

	// Ignore if remove table again.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		RemoveTable: &RemoveTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept:      func() { t.Fatalf("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Removing is in-progress through remove table response.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    "1",
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableResponse{
					Status: &tablepb.TableStatus{
						Span:  span,
						State: tablepb.TableStateStopping,
					},
				},
			},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Removed through heartbeat response.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    "1",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{{
				Span:  span,
				State: tablepb.TableStateStopped,
			}},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.spans.GetV(spanz.TableIDToComparableSpan(1)))

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks.GetV(spanz.TableIDToComparableSpan(1)))
}

func TestReplicationManagerMoveTable(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(10, model.ChangeFeedID{})
	moveTableCh := make(chan int, 1)

	source := "1"
	dest := "2"

	// Ignore move table if it's not exist.
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		MoveTable: &MoveTable{Span: spanz.TableIDToComparableSpan(1), DestCapture: dest},
		Accept:    func() { t.Fatal("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Add the table.
	span := spanz.TableIDToComparableSpan(1)
	tbl, err := NewReplicationSet(span, 0, map[string]*tablepb.TableStatus{
		source: {Span: span, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateReplicating, tbl.State)
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(1), tbl)

	// Replicating -> Prepare
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		MoveTable: &MoveTable{Span: spanz.TableIDToComparableSpan(1), DestCapture: dest},
		Accept: func() {
			moveTableCh <- 1
			close(moveTableCh)
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        span,
					IsSecondary: true,
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(1)))
	require.Equal(t, 1, <-moveTableCh)

	// Ignore if move table again.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		MoveTable: &MoveTable{Span: spanz.TableIDToComparableSpan(1), DestCapture: dest},
		Accept: func() {
			moveTableCh <- 1
			close(moveTableCh)
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Prepare -> Commit.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    dest,
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status: &tablepb.TableStatus{
						Span:  span,
						State: tablepb.TableStatePrepared,
					},
				},
			},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      source,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{Span: span},
			},
		},
	}, msgs[0])

	// Source is removed,
	// updates it's table status through heartbeat response.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    source,
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{{
				Span:  span,
				State: tablepb.TableStateStopped,
			}},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      dest,
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        span,
					IsSecondary: false,
				},
			},
		},
	}, msgs[0])

	// Commit -> Replicating
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    dest,
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status: &tablepb.TableStatus{
						Span:  span,
						State: tablepb.TableStateReplicating,
					},
				},
			},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(
		t, ReplicationSetStateReplicating, r.spans.GetV(spanz.TableIDToComparableSpan(1)).State)
	require.Equal(t, dest, r.spans.GetV(spanz.TableIDToComparableSpan(1)).Primary)

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks.GetV(spanz.TableIDToComparableSpan(1)))
}

func TestReplicationManagerBurstBalance(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	balanceTableCh := make(chan int, 1)

	// Burst balance is not limited by maxTaskConcurrency.
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{
			Span: spanz.TableIDToComparableSpan(1), CaptureID: "0", CheckpointTs: 1,
		},
	}, {
		BurstBalance: &BurstBalance{
			AddTables: []AddTable{{
				Span: spanz.TableIDToComparableSpan(1), CaptureID: "1", CheckpointTs: 1,
			}, {
				Span: spanz.TableIDToComparableSpan(2), CaptureID: "2", CheckpointTs: 1,
			}, {
				Span: spanz.TableIDToComparableSpan(3), CaptureID: "3", CheckpointTs: 1,
			}},
		},
		Accept: func() {
			balanceTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Equal(t, 1, <-balanceTableCh)
	require.Len(t, msgs, 3)
	for tableID, captureID := range map[model.TableID]model.CaptureID{
		1: "0", 2: "2", 3: "3",
	} {
		require.Contains(t, msgs, &schedulepb.Message{
			To:      captureID,
			MsgType: schedulepb.MsgDispatchTableRequest,
			DispatchTableRequest: &schedulepb.DispatchTableRequest{
				Request: &schedulepb.DispatchTableRequest_AddTable{
					AddTable: &schedulepb.AddTableRequest{
						Span:        spanz.TableIDToComparableSpan(tableID),
						IsSecondary: true,
						Checkpoint: tablepb.Checkpoint{
							CheckpointTs: 1,
							ResolvedTs:   1,
						},
					},
				},
			},
		}, msgs)
		require.True(t, r.spans.Has(spanz.TableIDToComparableSpan(tableID)))
		require.True(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(tableID)))
	}

	// Add a new table.
	span := spanz.TableIDToComparableSpan(5)
	table5, err := NewReplicationSet(span, 0, map[string]*tablepb.TableStatus{
		"5": {Span: span, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	r.spans.ReplaceOrInsert(span, table5)

	// More burst balance is still allowed.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		BurstBalance: &BurstBalance{
			AddTables: []AddTable{{
				Span: spanz.TableIDToComparableSpan(4), CaptureID: "4", CheckpointTs: 2,
			}, {
				Span: spanz.TableIDToComparableSpan(1), CaptureID: "0", CheckpointTs: 2,
			}},
			RemoveTables: []RemoveTable{{
				Span: spanz.TableIDToComparableSpan(5), CaptureID: "5",
			}, {
				Span: spanz.TableIDToComparableSpan(1), CaptureID: "0",
			}},
		},
		Accept: func() {
			balanceTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Equal(t, 1, <-balanceTableCh)
	require.Len(t, msgs, 2)
	require.Contains(t, msgs, &schedulepb.Message{
		To:      "4",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(4),
					IsSecondary: true,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 2,
						ResolvedTs:   2,
					},
				},
			},
		},
	}, msgs)
	require.Contains(t, msgs, &schedulepb.Message{
		To:      "5",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableRequest{
					Span: spanz.TableIDToComparableSpan(5),
				},
			},
		},
	}, msgs)
}

func TestReplicationManagerBurstBalanceMoveTables(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	balanceTableCh := make(chan int, 1)

	var err error
	// Two tables in "1".
	span := spanz.TableIDToComparableSpan(1)
	table, err := NewReplicationSet(span, 0, map[string]*tablepb.TableStatus{
		"1": {Span: span, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	r.spans.ReplaceOrInsert(span, table)
	span2 := spanz.TableIDToComparableSpan(2)
	table2, err := NewReplicationSet(span2, 0, map[string]*tablepb.TableStatus{
		"1": {
			Span: span2, State: tablepb.TableStateReplicating,
			Checkpoint: tablepb.Checkpoint{CheckpointTs: 1, ResolvedTs: 1},
		},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	r.spans.ReplaceOrInsert(span2, table2)

	msgs, err := r.HandleTasks([]*ScheduleTask{{
		BurstBalance: &BurstBalance{
			MoveTables: []MoveTable{{
				Span: spanz.TableIDToComparableSpan(2), DestCapture: "2",
			}},
		},
		Accept: func() {
			balanceTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Equal(t, 1, <-balanceTableCh)
	require.Len(t, msgs, 1)
	require.Contains(t, msgs, &schedulepb.Message{
		To:      "2",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        span2,
					IsSecondary: true,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
			},
		},
	}, msgs)
	require.True(t, r.spans.Has(span2))
	require.True(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(2)))
}

func TestReplicationManagerMaxTaskConcurrency(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	addTableCh := make(chan int, 1)

	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept: func() {
			addTableCh <- 1
			close(addTableCh)
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.EqualValues(t, &schedulepb.Message{
		To:      "1",
		MsgType: schedulepb.MsgDispatchTableRequest,
		DispatchTableRequest: &schedulepb.DispatchTableRequest{
			Request: &schedulepb.DispatchTableRequest_AddTable{
				AddTable: &schedulepb.AddTableRequest{
					Span:        spanz.TableIDToComparableSpan(1),
					IsSecondary: true,
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(1)))
	require.Equal(t, 1, <-addTableCh)

	// No more tasks allowed.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{Span: spanz.TableIDToComparableSpan(2), CaptureID: "1"},
		Accept: func() {
			t.Fatal("must not accept")
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
}

type mockRedoMetaManager struct {
	util.Runnable

	checkpointTs model.Ts
	resolvedTs   model.Ts
	enable       bool
}

func (m *mockRedoMetaManager) UpdateMeta(checkpointTs, resolvedTs model.Ts) {
}

func (m *mockRedoMetaManager) GetFlushedMeta() common.LogMeta {
	return common.LogMeta{
		CheckpointTs: m.checkpointTs,
		ResolvedTs:   m.resolvedTs,
	}
}

func (m *mockRedoMetaManager) Cleanup(ctx context.Context) error {
	return nil
}

func (m *mockRedoMetaManager) Enabled() bool {
	return m.enable
}

func (m *mockRedoMetaManager) Running() bool {
	return true
}

func TestReplicationManagerAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	span := spanz.TableIDToComparableSpan(1)
	rs, err := NewReplicationSet(span, model.Ts(10),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				Span:  spanz.TableIDToComparableSpan(1),
				State: tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(10),
					ResolvedTs:   model.Ts(20),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(span, rs)

	span2 := spanz.TableIDToComparableSpan(2)
	rs, err = NewReplicationSet(span2, model.Ts(15),
		map[model.CaptureID]*tablepb.TableStatus{
			"2": {
				Span:  spanz.TableIDToComparableSpan(2),
				State: tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(15),
					ResolvedTs:   model.Ts(30),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(span2, rs)

	redoMetaManager := &mockRedoMetaManager{enable: false}

	// no tables are replicating, resolvedTs should be advanced to globalBarrierTs and checkpoint
	// should be advanced to minTableBarrierTs.
	currentTables := &TableRanges{}
	checkpoint, resolved := r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(5), redoMetaManager)
	require.Equal(t, model.Ts(5), checkpoint)
	require.Equal(t, model.Ts(5), resolved)

	// all tables are replicating
	currentTables.UpdateTables([]model.TableID{1, 2})
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(10), checkpoint)
	require.Equal(t, model.Ts(20), resolved)

	// some table not exist yet.
	currentTables.UpdateTables([]model.TableID{1, 2, 3})
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, checkpointCannotProceed, checkpoint)
	require.Equal(t, checkpointCannotProceed, resolved)

	span3 := spanz.TableIDToComparableSpan(3)
	rs, err = NewReplicationSet(span3, model.Ts(5),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				Span:  spanz.TableIDToComparableSpan(3),
				State: tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(5),
					ResolvedTs:   model.Ts(40),
				},
			},
			"2": {
				Span:  spanz.TableIDToComparableSpan(3),
				State: tablepb.TableStatePreparing,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(5),
					ResolvedTs:   model.Ts(40),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(span3, rs)
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(5), checkpoint)
	require.Equal(t, model.Ts(20), resolved)

	currentTables.UpdateTables([]model.TableID{1, 2, 3, 4})
	span4 := spanz.TableIDToComparableSpan(4)
	rs, err = NewReplicationSet(span4, model.Ts(3),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				Span:  spanz.TableIDToComparableSpan(4),
				State: tablepb.TableStatePrepared,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(3),
					ResolvedTs:   model.Ts(10),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(span4, rs)
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(3), checkpoint)
	require.Equal(t, model.Ts(10), resolved)

	// Split table 5 into 2 spans.
	currentTables.UpdateTables([]model.TableID{1, 2, 3, 4, 5})
	span5_1 := spanz.TableIDToComparableSpan(5)
	span5_1.EndKey = append(span5_1.StartKey, 0)
	span5_2 := spanz.TableIDToComparableSpan(5)
	span5_2.StartKey = append(span5_2.StartKey, 0)
	for _, span := range []tablepb.Span{span5_1, span5_2} {
		rs, err = NewReplicationSet(span, model.Ts(3),
			map[model.CaptureID]*tablepb.TableStatus{
				"1": {
					Span:  span,
					State: tablepb.TableStatePrepared,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: model.Ts(3),
						ResolvedTs:   model.Ts(10),
					},
				},
			}, model.ChangeFeedID{})
		require.NoError(t, err)
		r.spans.ReplaceOrInsert(span, rs)
	}
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(3), checkpoint)
	require.Equal(t, model.Ts(10), resolved)

	// The start span is missing
	rs5_1, _ := r.spans.Delete(span5_1)
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, checkpointCannotProceed, checkpoint)
	require.Equal(t, checkpointCannotProceed, resolved)

	// The end span is missing
	r.spans.ReplaceOrInsert(span5_1, rs5_1)
	r.spans.Delete(span5_2)
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, checkpointCannotProceed, checkpoint)
	require.Equal(t, checkpointCannotProceed, resolved)

	// redo is enabled
	currentTables.UpdateTables([]model.TableID{4})
	spanRedo := spanz.TableIDToComparableSpan(4)
	rs, err = NewReplicationSet(spanRedo, model.Ts(3),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				Span:  spanz.TableIDToComparableSpan(4),
				State: tablepb.TableStatePrepared,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(10),
					ResolvedTs:   model.Ts(15),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(spanRedo, rs)
	barrier := schedulepb.NewBarrierWithMinTs(30)
	redoMetaManager.enable = true
	redoMetaManager.resolvedTs = 9
	checkpoint, resolved = r.AdvanceCheckpoint(currentTables, time.Now(), barrier, redoMetaManager)
	require.Equal(t, model.Ts(9), resolved)
	require.Equal(t, model.Ts(9), checkpoint)
	require.Equal(t, model.Ts(9), barrier.GetGlobalBarrierTs())
}

func TestReplicationManagerAdvanceCheckpointWithRedoEnabled(t *testing.T) {
	t.Parallel()
	r := NewReplicationManager(1, model.ChangeFeedID{})
	span := spanz.TableIDToComparableSpan(1)
	rs, err := NewReplicationSet(span, model.Ts(10),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				Span:  spanz.TableIDToComparableSpan(1),
				State: tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(10),
					ResolvedTs:   model.Ts(20),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(span, rs)

	span2 := spanz.TableIDToComparableSpan(2)
	rs, err = NewReplicationSet(span2, model.Ts(15),
		map[model.CaptureID]*tablepb.TableStatus{
			"2": {
				Span:  spanz.TableIDToComparableSpan(2),
				State: tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(15),
					ResolvedTs:   model.Ts(30),
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.spans.ReplaceOrInsert(span2, rs)

	redoMetaManager := &mockRedoMetaManager{enable: true, resolvedTs: 25}

	// some table not exist yet with redo is enabled.
	currentTables := &TableRanges{}
	currentTables.UpdateTables([]model.TableID{1, 2, 3})
	barrier := schedulepb.NewBarrierWithMinTs(30)
	checkpoint, resolved := r.AdvanceCheckpoint(currentTables, time.Now(), barrier, redoMetaManager)
	require.Equal(t, checkpointCannotProceed, checkpoint)
	require.Equal(t, checkpointCannotProceed, resolved)
	require.Equal(t, uint64(25), barrier.Barrier.GetGlobalBarrierTs())
}

func TestReplicationManagerHandleCaptureChanges(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	init := map[model.CaptureID][]tablepb.TableStatus{
		"1": {{Span: spanz.TableIDToComparableSpan(1), State: tablepb.TableStateReplicating}},
		"2": {{Span: spanz.TableIDToComparableSpan(2), State: tablepb.TableStateReplicating}},
		"3": {
			{Span: spanz.TableIDToComparableSpan(3), State: tablepb.TableStateReplicating},
			{Span: spanz.TableIDToComparableSpan(2), State: tablepb.TableStatePreparing},
		},
		"4": {{Span: spanz.TableIDToComparableSpan(4), State: tablepb.TableStateStopping}},
		"5": {{Span: spanz.TableIDToComparableSpan(5), State: tablepb.TableStateStopped}},
	}
	msgs, err := r.HandleCaptureChanges(init, nil, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, r.spans.Len(), 5)
	require.Equal(
		t, ReplicationSetStateReplicating, r.spans.GetV(spanz.TableIDToComparableSpan(1)).State)
	require.Equal(
		t, ReplicationSetStatePrepare, r.spans.GetV(spanz.TableIDToComparableSpan(2)).State)
	require.Equal(
		t, ReplicationSetStateReplicating, r.spans.GetV(spanz.TableIDToComparableSpan(3)).State)
	require.Equal(
		t, ReplicationSetStateRemoving, r.spans.GetV(spanz.TableIDToComparableSpan(4)).State)
	require.Equal(
		t, ReplicationSetStateAbsent, r.spans.GetV(spanz.TableIDToComparableSpan(5)).State)

	removed := map[string][]tablepb.TableStatus{
		"1": {{Span: spanz.TableIDToComparableSpan(1), State: tablepb.TableStateReplicating}},
	}
	msgs, err = r.HandleCaptureChanges(nil, removed, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, r.spans.Len(), 5)
	require.Equal(
		t, ReplicationSetStateAbsent, r.spans.GetV(spanz.TableIDToComparableSpan(1)).State)
	require.Equal(
		t, ReplicationSetStatePrepare, r.spans.GetV(spanz.TableIDToComparableSpan(2)).State)
	require.Equal(
		t, ReplicationSetStateReplicating, r.spans.GetV(spanz.TableIDToComparableSpan(3)).State)
	require.Equal(
		t, ReplicationSetStateRemoving, r.spans.GetV(spanz.TableIDToComparableSpan(4)).State)
	require.Equal(
		t, ReplicationSetStateAbsent, r.spans.GetV(spanz.TableIDToComparableSpan(5)).State)
}

func TestReplicationManagerHandleCaptureChangesDuringAddTable(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	addTableCh := make(chan int, 1)

	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept: func() {
			addTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.NotNil(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(1)))
	require.Equal(t, 1, <-addTableCh)

	removed := map[string][]tablepb.TableStatus{
		"1": {{Span: spanz.TableIDToComparableSpan(1), State: tablepb.TableStatePreparing}},
	}
	msgs, err = r.HandleCaptureChanges(nil, removed, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, r.spans.Len(), 1)
	require.Equal(
		t, ReplicationSetStateAbsent, r.spans.GetV(spanz.TableIDToComparableSpan(1)).State)
	require.Nil(t, r.runningTasks.GetV(spanz.TableIDToComparableSpan(1)))

	// New task must be accepted.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{Span: spanz.TableIDToComparableSpan(1), CaptureID: "1"},
		Accept: func() {
			addTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.NotNil(t, r.runningTasks.Has(spanz.TableIDToComparableSpan(1)))
	require.Equal(t, 1, <-addTableCh)
}

func TestLogSlowTableInfo(t *testing.T) {
	t.Parallel()
	r := NewReplicationManager(1, model.ChangeFeedID{})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(1), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(1),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 1},
		State:      ReplicationSetStateReplicating,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(2), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(2),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 2},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(3), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(3),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 3},
		State:      ReplicationSetStatePrepare,
	})
	r.logSlowTableInfo(time.Now())
	// make sure all tables are will be pop out from heal after logged
	require.Equal(t, r.slowTableHeap.Len(), 0)
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(4), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(4),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 4},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(5), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(5),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 5},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(6), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(6),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 6},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(7), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(7),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 7},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(8), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(8),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 8},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(9), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(9),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 9},
		State:      ReplicationSetStatePrepare,
	})
	r.spans.ReplaceOrInsert(spanz.TableIDToComparableSpan(1), &ReplicationSet{
		Span:       spanz.TableIDToComparableSpan(10),
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 10},
		State:      ReplicationSetStatePrepare,
	})
	r.logSlowTableInfo(time.Now())
	// make sure the slowTableHeap's capacity will not extend
	require.Equal(t, cap(r.slowTableHeap), 8)
}
