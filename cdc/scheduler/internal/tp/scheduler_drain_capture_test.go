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

func TestDrainCapture(t *testing.T) {
	t.Parallel()

	scheduler := newDrainCaptureScheduler(10)
	require.Equal(t, "drain-capture-scheduler", scheduler.Name())

	var checkpointTs model.Ts
	captures := make(map[model.CaptureID]*CaptureStatus)
	currentTables := make([]model.TableID, 0)
	replications := make(map[model.TableID]*ReplicationSet)

	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	ok := scheduler.setTarget("a")
	require.True(t, ok)

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	// the target capture has no table at the beginning, so reset the target
	require.Equal(t, captureIDNotDraining, scheduler.target)

	captures["a"] = &CaptureStatus{}
	ok = scheduler.setTarget("b")
	require.True(t, ok)

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	// the target capture cannot be found in the latest captures
	require.Equal(t, captureIDNotDraining, scheduler.target)

	captures["b"] = &CaptureStatus{}
	currentTables = []model.TableID{1, 2, 3, 4, 5, 6, 7}
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateCommit, Secondary: "a"},
		3: {State: ReplicationSetStatePrepare, Secondary: "a"},
		4: {State: ReplicationSetStatePrepare, Primary: "a", Secondary: "b"},
		5: {State: ReplicationSetStateRemoving, Primary: "a"},
		6: {State: ReplicationSetStateReplicating, Primary: "b"},
		7: {State: ReplicationSetStateCommit, Secondary: "b"},
	}

	ok = scheduler.setTarget("a")
	require.True(t, ok)
	// not all table is replicating, skip this tick.
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 0)

	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
		3: {State: ReplicationSetStateReplicating, Primary: "a"},
		4: {State: ReplicationSetStateReplicating, Primary: "b"},
		6: {State: ReplicationSetStateReplicating, Primary: "b"},
		7: {State: ReplicationSetStateReplicating, Primary: "b"},
	}

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 3)

	scheduler = newDrainCaptureScheduler(1)
	require.True(t, scheduler.setTarget("a"))
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 1)
}

func TestDrainStoppingCapture(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	captures := make(map[model.CaptureID]*CaptureStatus)
	currentTables := make([]model.TableID, 0)
	replications := make(map[model.TableID]*ReplicationSet)
	scheduler := newDrainCaptureScheduler(10)

	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Empty(t, tasks)

	captures["a"] = &CaptureStatus{}
	captures["b"] = &CaptureStatus{State: CaptureStateStopping}
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "b"},
	}
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.EqualValues(t, 2, tasks[0].moveTable.TableID)
	require.EqualValues(t, "a", tasks[0].moveTable.DestCapture)
	require.EqualValues(t, "b", scheduler.getTarget())
}
