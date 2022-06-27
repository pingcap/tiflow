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
	"sync/atomic"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRebalance(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	captures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2, 3, 4}

	replications := map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateCommit, Secondary: "b"},
		3: {State: ReplicationSetStatePrepare, Primary: "a", Secondary: "b"},
		4: {State: ReplicationSetStateAbsent},
	}

	scheduler := newRebalanceScheduler()
	require.Equal(t, "rebalance-scheduler", scheduler.Name())
	// rebalance is not triggered
	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)

	atomic.StoreInt32(&scheduler.rebalance, 1)
	// no captures
	tasks = scheduler.Schedule(checkpointTs, currentTables, map[model.CaptureID]*model.CaptureInfo{}, replications, false)
	require.Len(t, tasks, 0)

	// table not in the replication set,
	tasks = scheduler.Schedule(checkpointTs, []model.TableID{0}, captures, replications, false)
	require.Len(t, tasks, 0)

	// not all tables are replicating,
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)

	// table distribution is balanced, should have no task.
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
		3: {State: ReplicationSetStateReplicating, Primary: "b"},
		4: {State: ReplicationSetStateReplicating, Primary: "b"},
	}
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)

	// Imbalance.
	replications[5] = &ReplicationSet{
		State:   ReplicationSetStateReplicating,
		Primary: "a",
	}
	replications[6] = &ReplicationSet{
		State:   ReplicationSetStateReplicating,
		Primary: "a",
	}

	scheduler.random = nil // disable random to make test easier.
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].burstBalance.MoveTables, moveTable{
		TableID: 1, DestCapture: "b",
	})
	require.EqualValues(t, 1, atomic.LoadInt32(&scheduler.rebalance))

	tasks[0].accept()
	require.EqualValues(t, 0, atomic.LoadInt32(&scheduler.rebalance))

	// pending task is not consumed yet, this turn should have no tasks.
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)
}
