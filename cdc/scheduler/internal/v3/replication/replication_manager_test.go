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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/stretchr/testify/require"
)

func TestReplicationManagerHandleAddTableTask(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(10, model.ChangeFeedID{})
	addTableCh := make(chan int, 1)
	// Absent -> Prepare
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 1, CaptureID: "1", CheckpointTs: 1},
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
					TableID:     1,
					IsSecondary: true,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-addTableCh)

	// Ignore if add the table again.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 1, CaptureID: "1"},
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
						TableID: 1,
						State:   tablepb.TableStatePrepared,
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
					TableID:     1,
					IsSecondary: false,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.tables[1].State)
	require.Equal(t, "1", r.tables[1].Primary)
	require.False(t, r.tables[1].hasRole(RoleSecondary))

	// Commit -> Replicating through heartbeat response.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    "1",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []tablepb.TableStatus{{
				TableID: 1,
				State:   tablepb.TableStateReplicating,
			}},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[1].State)
	require.Equal(t, "1", r.tables[1].Primary)
	require.False(t, r.tables[1].hasRole(RoleSecondary))

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks[1])
}

func TestReplicationManagerRemoveTable(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(10, model.ChangeFeedID{})
	removeTableCh := make(chan int, 1)

	// Ignore remove table if there is no such table.
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		RemoveTable: &RemoveTable{TableID: 1, CaptureID: "1"},
		Accept:      func() { t.Fatal("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Add the table.
	tbl, err := NewReplicationSet(1, 0, map[string]*tablepb.TableStatus{
		"1": {TableID: 1, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateReplicating, tbl.State)
	r.tables[1] = tbl

	// Remove the table.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		RemoveTable: &RemoveTable{TableID: 1, CaptureID: "1"},
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
				RemoveTable: &schedulepb.RemoveTableRequest{TableID: 1},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-removeTableCh)

	// Ignore if remove table again.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		RemoveTable: &RemoveTable{TableID: 1, CaptureID: "1"},
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
						TableID: 1,
						State:   tablepb.TableStateStopping,
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
				TableID: 1,
				State:   tablepb.TableStateStopped,
			}},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.tables[1])

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks[1])
}

func TestReplicationManagerMoveTable(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(10, model.ChangeFeedID{})
	moveTableCh := make(chan int, 1)

	source := "1"
	dest := "2"

	// Ignore move table if it's not exist.
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		MoveTable: &MoveTable{TableID: 1, DestCapture: dest},
		Accept:    func() { t.Fatal("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Add the table.
	tbl, err := NewReplicationSet(1, 0, map[string]*tablepb.TableStatus{
		source: {TableID: 1, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateReplicating, tbl.State)
	r.tables[1] = tbl

	// Replicating -> Prepare
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		MoveTable: &MoveTable{TableID: 1, DestCapture: dest},
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
					TableID:     1,
					IsSecondary: true,
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-moveTableCh)

	// Ignore if move table again.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		MoveTable: &MoveTable{TableID: 1, DestCapture: dest},
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
						TableID: 1,
						State:   tablepb.TableStatePrepared,
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
				RemoveTable: &schedulepb.RemoveTableRequest{TableID: 1},
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
				TableID: 1,
				State:   tablepb.TableStateStopped,
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
					TableID:     1,
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
						TableID: 1,
						State:   tablepb.TableStateReplicating,
					},
				},
			},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[1].State)
	require.Equal(t, dest, r.tables[1].Primary)

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks[1])
}

func TestReplicationManagerBurstBalance(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	balanceTableCh := make(chan int, 1)

	// Burst balance is not limited by maxTaskConcurrency.
	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 1, CaptureID: "0", CheckpointTs: 1},
	}, {
		BurstBalance: &BurstBalance{
			AddTables: []AddTable{{
				TableID: 1, CaptureID: "1", CheckpointTs: 1,
			}, {
				TableID: 2, CaptureID: "2", CheckpointTs: 1,
			}, {
				TableID: 3, CaptureID: "3", CheckpointTs: 1,
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
						TableID:     tableID,
						IsSecondary: true,
						Checkpoint: tablepb.Checkpoint{
							CheckpointTs: 1,
							ResolvedTs:   1,
						},
					},
				},
			},
		}, msgs)
		require.Contains(t, r.tables, tableID)
		require.Contains(t, r.runningTasks, tableID)
	}

	// Add a new table.
	r.tables[5], err = NewReplicationSet(5, 0, map[string]*tablepb.TableStatus{
		"5": {TableID: 5, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)

	// More burst balance is still allowed.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		BurstBalance: &BurstBalance{
			AddTables: []AddTable{{
				TableID: 4, CaptureID: "4", CheckpointTs: 2,
			}, {
				TableID: 1, CaptureID: "0", CheckpointTs: 2,
			}},
			RemoveTables: []RemoveTable{{
				TableID: 5, CaptureID: "5",
			}, {
				TableID: 1, CaptureID: "0",
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
					TableID:     4,
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
					TableID: 5,
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
	r.tables[1], err = NewReplicationSet(1, 0, map[string]*tablepb.TableStatus{
		"1": {TableID: 1, State: tablepb.TableStateReplicating},
	}, model.ChangeFeedID{})
	require.Nil(t, err)
	r.tables[2], err = NewReplicationSet(2, 0, map[string]*tablepb.TableStatus{
		"1": {
			TableID: 2, State: tablepb.TableStateReplicating,
			Checkpoint: tablepb.Checkpoint{CheckpointTs: 1, ResolvedTs: 1},
		},
	}, model.ChangeFeedID{})
	require.Nil(t, err)

	msgs, err := r.HandleTasks([]*ScheduleTask{{
		BurstBalance: &BurstBalance{
			MoveTables: []MoveTable{{
				TableID: 2, DestCapture: "2",
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
					TableID:     2,
					IsSecondary: true,
					Checkpoint: tablepb.Checkpoint{
						CheckpointTs: 1,
						ResolvedTs:   1,
					},
				},
			},
		},
	}, msgs)
	require.Contains(t, r.tables, model.TableID(2))
	require.Contains(t, r.runningTasks, model.TableID(2))
}

func TestReplicationManagerMaxTaskConcurrency(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	addTableCh := make(chan int, 1)

	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 1, CaptureID: "1"},
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
					TableID:     1,
					IsSecondary: true,
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-addTableCh)

	// No more tasks allowed.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 2, CaptureID: "1"},
		Accept: func() {
			t.Fatal("must not accept")
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
}

type mockRedoMetaManager struct {
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

func (m *mockRedoMetaManager) Run(ctx context.Context) error {
	return nil
}

func (m *mockRedoMetaManager) Running() bool {
	return true
}

func TestReplicationManagerAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	rs, err := NewReplicationSet(model.TableID(1), model.Ts(10),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				TableID: model.TableID(1),
				State:   tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(10),
					ResolvedTs:   model.Ts(20),
					LastSyncedTs: model.Ts(15),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(30),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[model.TableID(1)] = rs

	rs, err = NewReplicationSet(model.TableID(2), model.Ts(15),
		map[model.CaptureID]*tablepb.TableStatus{
			"2": {
				TableID: model.TableID(2),
				State:   tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(15),
					ResolvedTs:   model.Ts(30),
					LastSyncedTs: model.Ts(20),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(40),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[model.TableID(2)] = rs

	redoMetaManager := &mockRedoMetaManager{enable: false}
	// no tables are replicating, resolvedTs should be advanced to globalBarrierTs and checkpoint
	// should be advanced to minTableBarrierTs.
	currentTables := []model.TableID{}
	watermark := r.AdvanceCheckpoint(
		currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(5), redoMetaManager)
	require.Equal(t, model.Ts(5), watermark.CheckpointTs)
	require.Equal(t, model.Ts(5), watermark.ResolvedTs)
	require.Equal(t, model.Ts(0), watermark.LastSyncedTs)
	require.Equal(t, model.Ts(math.MaxUint64), watermark.PullerResolvedTs)

	// all tables are replicating
	currentTables = []model.TableID{1, 2}
	watermark = r.AdvanceCheckpoint(
		currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(10), watermark.CheckpointTs)
	require.Equal(t, model.Ts(20), watermark.ResolvedTs)
	require.Equal(t, model.Ts(20), watermark.LastSyncedTs)
	require.Equal(t, model.Ts(30), watermark.PullerResolvedTs)

	// some table not exist yet.
	currentTables = append(currentTables, 3)
	watermark = r.AdvanceCheckpoint(
		currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, checkpointCannotProceed, watermark.CheckpointTs)
	require.Equal(t, checkpointCannotProceed, watermark.ResolvedTs)
	require.Equal(t, checkpointCannotProceed, watermark.LastSyncedTs)
	require.Equal(t, checkpointCannotProceed, watermark.PullerResolvedTs)

	rs, err = NewReplicationSet(model.TableID(3), model.Ts(5),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				TableID: model.TableID(3),
				State:   tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(5),
					ResolvedTs:   model.Ts(40),
					LastSyncedTs: model.Ts(30),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(50),
						},
					},
				},
			},
			"2": {
				TableID: model.TableID(3),
				State:   tablepb.TableStatePreparing,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(5),
					ResolvedTs:   model.Ts(40),
					LastSyncedTs: model.Ts(32),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(50),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[model.TableID(3)] = rs
	watermark = r.AdvanceCheckpoint(
		currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(5), watermark.CheckpointTs)
	require.Equal(t, model.Ts(20), watermark.ResolvedTs)
	require.Equal(t, model.Ts(32), watermark.LastSyncedTs)
	require.Equal(t, model.Ts(30), watermark.PullerResolvedTs)

	currentTables = append(currentTables, 4)
	rs, err = NewReplicationSet(model.TableID(4), model.Ts(3),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				TableID: model.TableID(4),
				State:   tablepb.TableStatePrepared,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(3),
					ResolvedTs:   model.Ts(10),
					LastSyncedTs: model.Ts(5),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(12),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[model.TableID(4)] = rs
	watermark = r.AdvanceCheckpoint(
		currentTables, time.Now(), schedulepb.NewBarrierWithMinTs(30), redoMetaManager)
	require.Equal(t, model.Ts(3), watermark.CheckpointTs)
	require.Equal(t, model.Ts(10), watermark.ResolvedTs)
	require.Equal(t, model.Ts(32), watermark.LastSyncedTs)
	require.Equal(t, model.Ts(12), watermark.PullerResolvedTs)
	// redo is enabled
	currentTables = append(currentTables[:0], 4)
	rs, err = NewReplicationSet(model.TableID(4), model.Ts(3),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				TableID: model.TableID(4),
				State:   tablepb.TableStatePrepared,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(10),
					ResolvedTs:   model.Ts(15),
					LastSyncedTs: model.Ts(12),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(16),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[model.TableID(4)] = rs
	barrier := schedulepb.NewBarrierWithMinTs(30)
	redoMetaManager.enable = true
	redoMetaManager.resolvedTs = 9
	redoMetaManager.checkpointTs = 9
	watermark = r.AdvanceCheckpoint(
		currentTables, time.Now(), barrier, redoMetaManager)
	require.Equal(t, model.Ts(9), watermark.ResolvedTs)
	require.Equal(t, model.Ts(9), watermark.CheckpointTs)
	require.Equal(t, model.Ts(12), watermark.LastSyncedTs)
	require.Equal(t, model.Ts(16), watermark.PullerResolvedTs)
	require.Equal(t, model.Ts(9), barrier.GetGlobalBarrierTs())
}

func TestReplicationManagerAdvanceCheckpointWithRedoEnabled(t *testing.T) {
	t.Parallel()
	r := NewReplicationManager(1, model.ChangeFeedID{})
	rs, err := NewReplicationSet(1, model.Ts(10),
		map[model.CaptureID]*tablepb.TableStatus{
			"1": {
				TableID: 1,
				State:   tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(10),
					ResolvedTs:   model.Ts(20),
					LastSyncedTs: model.Ts(12),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(26),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[1] = rs

	rs, err = NewReplicationSet(2, model.Ts(15),
		map[model.CaptureID]*tablepb.TableStatus{
			"2": {
				TableID: 2,
				State:   tablepb.TableStateReplicating,
				Checkpoint: tablepb.Checkpoint{
					CheckpointTs: model.Ts(15),
					ResolvedTs:   model.Ts(30),
					LastSyncedTs: model.Ts(18),
				},
				Stats: tablepb.Stats{
					StageCheckpoints: map[string]tablepb.Checkpoint{
						"puller-egress": {
							ResolvedTs: model.Ts(39),
						},
					},
				},
			},
		}, model.ChangeFeedID{})
	require.NoError(t, err)
	r.tables[2] = rs

	redoMetaManager := &mockRedoMetaManager{enable: true, resolvedTs: 25}

	// some table not exist yet with redo is enabled.
	currentTables := []model.TableID{1, 2, 3}
	barrier := schedulepb.NewBarrierWithMinTs(30)
	watermark := r.AdvanceCheckpoint(
		currentTables,
		time.Now(), barrier, redoMetaManager)
	require.Equal(t, checkpointCannotProceed, watermark.CheckpointTs)
	require.Equal(t, checkpointCannotProceed, watermark.ResolvedTs)
	require.Equal(t, checkpointCannotProceed, watermark.LastSyncedTs)
	require.Equal(t, checkpointCannotProceed, watermark.PullerResolvedTs)
	require.Equal(t, uint64(25), barrier.Barrier.GetGlobalBarrierTs())
}

func TestReplicationManagerHandleCaptureChanges(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	init := map[model.CaptureID][]tablepb.TableStatus{
		"1": {{TableID: 1, State: tablepb.TableStateReplicating}},
		"2": {{TableID: 2, State: tablepb.TableStateReplicating}},
		"3": {
			{TableID: 3, State: tablepb.TableStateReplicating},
			{TableID: 2, State: tablepb.TableStatePreparing},
		},
		"4": {{TableID: 4, State: tablepb.TableStateStopping}},
		"5": {{TableID: 5, State: tablepb.TableStateStopped}},
	}
	msgs, err := r.HandleCaptureChanges(init, nil, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, r.tables, 5)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[1].State)
	require.Equal(t, ReplicationSetStatePrepare, r.tables[2].State)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[3].State)
	require.Equal(t, ReplicationSetStateRemoving, r.tables[4].State)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[5].State)

	removed := map[string][]tablepb.TableStatus{
		"1": {{TableID: 1, State: tablepb.TableStateReplicating}},
	}
	msgs, err = r.HandleCaptureChanges(nil, removed, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, r.tables, 5)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[1].State)
	require.Equal(t, ReplicationSetStatePrepare, r.tables[2].State)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[3].State)
	require.Equal(t, ReplicationSetStateRemoving, r.tables[4].State)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[5].State)
}

func TestReplicationManagerHandleCaptureChangesDuringAddTable(t *testing.T) {
	t.Parallel()

	r := NewReplicationManager(1, model.ChangeFeedID{})
	addTableCh := make(chan int, 1)

	msgs, err := r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 1, CaptureID: "1"},
		Accept: func() {
			addTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-addTableCh)

	removed := map[string][]tablepb.TableStatus{
		"1": {{TableID: 1, State: tablepb.TableStatePreparing}},
	}
	msgs, err = r.HandleCaptureChanges(nil, removed, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, r.tables, 1)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[1].State)
	require.Nil(t, r.runningTasks[1])

	// New task must be accepted.
	msgs, err = r.HandleTasks([]*ScheduleTask{{
		AddTable: &AddTable{TableID: 1, CaptureID: "1"},
		Accept: func() {
			addTableCh <- 1
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 1)
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-addTableCh)
}

func TestLogSlowTableInfo(t *testing.T) {
	t.Parallel()
	r := NewReplicationManager(1, model.ChangeFeedID{})
	r.tables[1] = &ReplicationSet{
		TableID:    1,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 1},
		State:      ReplicationSetStateReplicating,
	}
	r.tables[2] = &ReplicationSet{
		TableID:    2,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 2},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[3] = &ReplicationSet{
		TableID:    3,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 3},
		State:      ReplicationSetStatePrepare,
	}
	currentTables := []model.TableID{1, 2, 3}
	r.logSlowTableInfo(currentTables, time.Now())
	// make sure all tables are will be pop out from heal after logged
	require.Equal(t, r.slowTableHeap.Len(), 0)
	r.tables[4] = &ReplicationSet{
		TableID:    4,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 4},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[5] = &ReplicationSet{
		TableID:    5,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 5},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[6] = &ReplicationSet{
		TableID:    6,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 6},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[7] = &ReplicationSet{
		TableID:    7,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 7},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[8] = &ReplicationSet{
		TableID:    8,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 8},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[9] = &ReplicationSet{
		TableID:    9,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 9},
		State:      ReplicationSetStatePrepare,
	}
	r.tables[10] = &ReplicationSet{
		TableID:    10,
		Checkpoint: tablepb.Checkpoint{CheckpointTs: 10},
		State:      ReplicationSetStatePrepare,
	}
	currentTables = []model.TableID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	r.logSlowTableInfo(currentTables, time.Now())
	// make sure the slowTableHeap's capacity will not extend
	require.Equal(t, cap(r.slowTableHeap), 8)
}
