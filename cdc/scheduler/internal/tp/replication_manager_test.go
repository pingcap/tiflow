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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/require"
)

func TestReplicationManagerHandleAddTableTask(t *testing.T) {
	t.Parallel()

	r := newReplicationManager(10)
	addTableCh := make(chan int, 1)
	// Absent -> Prepare
	msgs, err := r.HandleTasks([]*scheduleTask{{
		addTable: &addTable{TableID: 1, CaptureID: "1", CheckpointTs: 1},
		accept: func() {
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
					Checkpoint:  schedulepb.Checkpoint{CheckpointTs: 1},
				},
			},
		},
	}, msgs[0])
	require.NotNil(t, r.runningTasks[1])
	require.Equal(t, 1, <-addTableCh)

	// Ignore if add the table again.
	msgs, err = r.HandleTasks([]*scheduleTask{{
		addTable: &addTable{TableID: 1, CaptureID: "1"},
		accept:   func() { t.Fatalf("must not accept") },
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
					Status: &schedulepb.TableStatus{
						TableID: 1,
						State:   schedulepb.TableStatePrepared,
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
					Checkpoint:  schedulepb.Checkpoint{CheckpointTs: 1},
				},
			},
		},
	}, msgs[0])
	require.Equal(t, ReplicationSetStateCommit, r.tables[1].State)
	require.Equal(t, "1", r.tables[1].Primary)
	require.Equal(t, "", r.tables[1].Secondary)

	// Commit -> Replicating through heartbeat response.
	msgs, err = r.HandleMessage([]*schedulepb.Message{{
		From:    "1",
		MsgType: schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: &schedulepb.HeartbeatResponse{
			Tables: []schedulepb.TableStatus{{
				TableID: 1,
				State:   schedulepb.TableStateReplicating,
			}},
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[1].State)
	require.Equal(t, "1", r.tables[1].Primary)
	require.Equal(t, "", r.tables[1].Secondary)

	// Handle task again to clear runningTasks
	msgs, err = r.HandleTasks(nil)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Nil(t, r.runningTasks[1])
}

func TestReplicationManagerRemoveTable(t *testing.T) {
	t.Parallel()

	r := newReplicationManager(10)
	removeTableCh := make(chan int, 1)

	// Ignore remove table if there is no such table.
	msgs, err := r.HandleTasks([]*scheduleTask{{
		removeTable: &removeTable{TableID: 1, CaptureID: "1"},
		accept:      func() { t.Fatal("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Add the table.
	tbl, err := newReplicationSet(1, 0, map[string]*schedulepb.TableStatus{
		"1": {TableID: 1, State: schedulepb.TableStateReplicating},
	})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateReplicating, tbl.State)
	r.tables[1] = tbl

	// Remove the table.
	msgs, err = r.HandleTasks([]*scheduleTask{{
		removeTable: &removeTable{TableID: 1, CaptureID: "1"},
		accept: func() {
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
	msgs, err = r.HandleTasks([]*scheduleTask{{
		removeTable: &removeTable{TableID: 1, CaptureID: "1"},
		accept:      func() { t.Fatalf("must not accept") },
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
					Status: &schedulepb.TableStatus{
						TableID: 1,
						State:   schedulepb.TableStateStopping,
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
			Tables: []schedulepb.TableStatus{{
				TableID: 1,
				State:   schedulepb.TableStateStopped,
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

	r := newReplicationManager(10)
	moveTableCh := make(chan int, 1)

	source := "1"
	dest := "2"

	// Ignore move table if it's not exist.
	msgs, err := r.HandleTasks([]*scheduleTask{{
		moveTable: &moveTable{TableID: 1, DestCapture: dest},
		accept:    func() { t.Fatal("must not accept") },
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)

	// Add the table.
	tbl, err := newReplicationSet(1, 0, map[string]*schedulepb.TableStatus{
		source: {TableID: 1, State: schedulepb.TableStateReplicating},
	})
	require.Nil(t, err)
	require.Equal(t, ReplicationSetStateReplicating, tbl.State)
	r.tables[1] = tbl

	// Replicating -> Prepare
	msgs, err = r.HandleTasks([]*scheduleTask{{
		moveTable: &moveTable{TableID: 1, DestCapture: dest},
		accept: func() {
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
	msgs, err = r.HandleTasks([]*scheduleTask{{
		moveTable: &moveTable{TableID: 1, DestCapture: dest},
		accept: func() {
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
					Status: &schedulepb.TableStatus{
						TableID: 1,
						State:   schedulepb.TableStatePrepared,
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
			Tables: []schedulepb.TableStatus{{
				TableID: 1,
				State:   schedulepb.TableStateStopped,
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
					Status: &schedulepb.TableStatus{
						TableID: 1,
						State:   schedulepb.TableStateReplicating,
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

	r := newReplicationManager(1)
	balanceTableCh := make(chan int, 1)

	// Burst balance is not limited by maxTaskConcurrency.
	msgs, err := r.HandleTasks([]*scheduleTask{{
		addTable: &addTable{TableID: 1, CaptureID: "0", CheckpointTs: 1},
	}, {
		burstBalance: &burstBalance{
			AddTables: []addTable{{
				TableID: 1, CaptureID: "1", CheckpointTs: 1,
			}, {
				TableID: 2, CaptureID: "2", CheckpointTs: 1,
			}, {
				TableID: 3, CaptureID: "3", CheckpointTs: 1,
			}},
		},
		accept: func() {
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
						Checkpoint:  schedulepb.Checkpoint{CheckpointTs: 1},
					},
				},
			},
		}, msgs)
		require.Contains(t, r.tables, tableID)
		require.Contains(t, r.runningTasks, tableID)
	}

	// Add a new table.
	r.tables[5], err = newReplicationSet(5, 0, map[string]*schedulepb.TableStatus{
		"5": {TableID: 5, State: schedulepb.TableStateReplicating},
	})
	require.Nil(t, err)

	// More burst balance is still allowed.
	msgs, err = r.HandleTasks([]*scheduleTask{{
		burstBalance: &burstBalance{
			AddTables: []addTable{{
				TableID: 4, CaptureID: "4", CheckpointTs: 2,
			}, {
				TableID: 1, CaptureID: "0", CheckpointTs: 2,
			}},
			RemoveTables: []removeTable{{
				TableID: 5, CaptureID: "5",
			}, {
				TableID: 1, CaptureID: "0",
			}},
		},
		accept: func() {
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
					Checkpoint:  schedulepb.Checkpoint{CheckpointTs: 2},
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

	r := newReplicationManager(1)
	balanceTableCh := make(chan int, 1)

	var err error
	// Two tables in "1".
	r.tables[1], err = newReplicationSet(1, 0, map[string]*schedulepb.TableStatus{
		"1": {TableID: 1, State: schedulepb.TableStateReplicating},
	})
	require.Nil(t, err)
	r.tables[2], err = newReplicationSet(2, 0, map[string]*schedulepb.TableStatus{
		"1": {
			TableID: 2, State: schedulepb.TableStateReplicating,
			Checkpoint: schedulepb.Checkpoint{CheckpointTs: 1},
		},
	})
	require.Nil(t, err)

	msgs, err := r.HandleTasks([]*scheduleTask{{
		burstBalance: &burstBalance{
			MoveTables: []moveTable{{
				TableID: 2, DestCapture: "2",
			}},
		},
		accept: func() {
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
					Checkpoint:  schedulepb.Checkpoint{CheckpointTs: 1},
				},
			},
		},
	}, msgs)
	require.Contains(t, r.tables, model.TableID(2))
	require.Contains(t, r.runningTasks, model.TableID(2))
}

func TestReplicationManagerMaxTaskConcurrency(t *testing.T) {
	t.Parallel()

	r := newReplicationManager(1)
	addTableCh := make(chan int, 1)

	msgs, err := r.HandleTasks([]*scheduleTask{{
		addTable: &addTable{TableID: 1, CaptureID: "1"},
		accept: func() {
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
	msgs, err = r.HandleTasks([]*scheduleTask{{
		addTable: &addTable{TableID: 2, CaptureID: "1"},
		accept: func() {
			t.Fatal("must not accept")
		},
	}})
	require.Nil(t, err)
	require.Len(t, msgs, 0)
}

func TestReplicationManagerHandleCaptureChanges(t *testing.T) {
	t.Parallel()

	r := newReplicationManager(1)
	changes := captureChanges{Init: map[model.CaptureID][]schedulepb.TableStatus{
		"1": {{TableID: 1, State: schedulepb.TableStateReplicating}},
		"2": {{TableID: 2, State: schedulepb.TableStateReplicating}},
		"3": {
			{TableID: 3, State: schedulepb.TableStateReplicating},
			{TableID: 2, State: schedulepb.TableStatePreparing},
		},
		"4": {{TableID: 4, State: schedulepb.TableStateStopping}},
	}}
	msgs, err := r.HandleCaptureChanges(&changes, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, r.tables, 4)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[1].State)
	require.Equal(t, ReplicationSetStatePrepare, r.tables[2].State)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[3].State)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[4].State)

	changes = captureChanges{Removed: map[string][]schedulepb.TableStatus{
		"1": {{TableID: 1, State: schedulepb.TableStateReplicating}},
	}}
	msgs, err = r.HandleCaptureChanges(&changes, 0)
	require.Nil(t, err)
	require.Len(t, msgs, 0)
	require.Len(t, r.tables, 4)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[1].State)
	require.Equal(t, ReplicationSetStatePrepare, r.tables[2].State)
	require.Equal(t, ReplicationSetStateReplicating, r.tables[3].State)
	require.Equal(t, ReplicationSetStateAbsent, r.tables[4].State)
}
