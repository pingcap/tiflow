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

package scheduler

import (
	"sync/atomic"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRebalance(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	captures := map[model.CaptureID]*member.CaptureStatus{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2, 3, 4}

	replications := map[model.TableID]*replication.ReplicationSet{
		1: {
			State: replication.ReplicationSetStateReplicating, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RolePrimary,
			},
		},
		2: {
			State: replication.ReplicationSetStateCommit,
			Captures: map[string]replication.Role{
				"b": replication.RoleSecondary,
			},
		},
		3: {
			State: replication.ReplicationSetStatePrepare, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RolePrimary, "b": replication.RoleSecondary,
			},
		},
		4: {State: replication.ReplicationSetStateAbsent},
	}

	scheduler := newRebalanceScheduler(model.ChangeFeedID{})
	require.Equal(t, "rebalance-scheduler", scheduler.Name())
	// rebalance is not triggered
	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	atomic.StoreInt32(&scheduler.rebalance, 1)
	// no captures
	tasks = scheduler.Schedule(
		checkpointTs, currentTables, map[model.CaptureID]*member.CaptureStatus{}, replications)
	require.Len(t, tasks, 0)

	// table not in the replication set,
	tasks = scheduler.Schedule(checkpointTs, []model.TableID{0}, captures, replications)
	require.Len(t, tasks, 0)

	// not all tables are replicating,
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	// table distribution is balanced, should have no task.
	replications = map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		2: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		3: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
		4: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
	}
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	// Imbalance.
	replications[5] = &replication.ReplicationSet{
		State:   replication.ReplicationSetStateReplicating,
		Primary: "a",
	}
	replications[6] = &replication.ReplicationSet{
		State:   replication.ReplicationSetStateReplicating,
		Primary: "a",
	}

	// capture is stopping, ignore the request
	captures["a"].State = member.CaptureStateStopping
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	require.Equal(t, atomic.LoadInt32(&scheduler.rebalance), int32(0))

	captures["a"].State = member.CaptureStateInitialized
	atomic.StoreInt32(&scheduler.rebalance, 1)
	scheduler.random = nil // disable random to make test easier.
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].BurstBalance.MoveTables, replication.MoveTable{
		TableID: 1, DestCapture: "b",
	})
	require.EqualValues(t, 1, atomic.LoadInt32(&scheduler.rebalance))

	tasks[0].Accept()
	require.EqualValues(t, 0, atomic.LoadInt32(&scheduler.rebalance))

	// pending task is not consumed yet, this turn should have no tasks.
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
}
