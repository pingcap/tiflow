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

	scheduler := newDrainCaptureScheduler()
	require.Equal(t, "drain-capture-scheduler", scheduler.Name())

	var checkpointTs model.Ts
	captures := make(map[model.CaptureID]*model.CaptureInfo)
	currentTables := make([]model.TableID, 0)
	replications := make(map[model.TableID]*ReplicationSet)

	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	ok := scheduler.setTarget("a")
	require.True(t, ok)

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	require.Equal(t, captureIDNotDraining, scheduler.target)

	captures["a"] = &model.CaptureInfo{}
	ok = scheduler.setTarget("b")
	require.True(t, ok)

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	require.Equal(t, captureIDNotDraining, scheduler.target)

	captures["b"] = &model.CaptureInfo{}
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
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 1)
}
