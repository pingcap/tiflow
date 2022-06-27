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
	"github.com/stretchr/testify/require"
)

func TestSchedulerMoveTable(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	captures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2, 3, 4}

	replications := map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
	}

	scheduler := newMoveTableScheduler()
	require.Equal(t, "move-table-scheduler", scheduler.Name())

	tasks := scheduler.Schedule(checkpointTs, currentTables,
		map[model.CaptureID]*model.CaptureInfo{}, replications, false)
	require.Len(t, tasks, 0)

	scheduler.addTask(model.TableID(0), "a")
	tasks = scheduler.Schedule(checkpointTs, currentTables,
		map[model.CaptureID]*model.CaptureInfo{}, replications, false)
	require.Len(t, tasks, 0)

	// move a not exist table
	scheduler.addTask(model.TableID(0), "a")
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)

	// move table to a not exist capture
	scheduler.addTask(model.TableID(1), "c")
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)

	// move table not replicating
	scheduler.addTask(model.TableID(1), "b")
	tasks = scheduler.Schedule(checkpointTs, currentTables,
		captures, map[model.TableID]*ReplicationSet{}, false)
	require.Len(t, tasks, 0)

	scheduler.addTask(model.TableID(1), "b")
	replications[model.TableID(1)].State = ReplicationSetStatePrepare
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 0)

	scheduler.addTask(model.TableID(1), "b")
	replications[model.TableID(1)].State = ReplicationSetStateReplicating
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications, false)
	require.Len(t, tasks, 1)
	require.Equal(t, model.TableID(1), tasks[0].moveTable.TableID)
	require.Equal(t, "b", tasks[0].moveTable.DestCapture)
	require.Equal(t, scheduler.tasks[model.TableID(1)], tasks[0])
}
