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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestDrainCapture(t *testing.T) {
	t.Parallel()

	scheduler := newDrainCaptureScheduler(10, model.ChangeFeedID{})
	require.Equal(t, "drain-capture-scheduler", scheduler.Name())

	var checkpointTs model.Ts
	captures := make(map[model.CaptureID]*member.CaptureStatus)
	currentTables := make([]tablepb.Span, 0)
	replications := mapToSpanMap(make(map[model.TableID]*replication.ReplicationSet))

	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	ok := scheduler.setTarget("a")
	require.True(t, ok)

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	// the target capture has no table at the beginning, so reset the target
	require.Equal(t, captureIDNotDraining, scheduler.target)

	captures["a"] = &member.CaptureStatus{}
	ok = scheduler.setTarget("b")
	require.True(t, ok)

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	// the target capture cannot be found in the latest captures
	require.Equal(t, captureIDNotDraining, scheduler.target)

	captures["b"] = &member.CaptureStatus{}
	currentTables = spanz.ArrayToSpan([]model.TableID{1, 2, 3, 4, 5, 6, 7})
	replications = mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {
			State: replication.ReplicationSetStateReplicating, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RolePrimary,
			},
		},
		2: {
			State: replication.ReplicationSetStateCommit,
			Captures: map[string]replication.Role{
				"a": replication.RoleSecondary,
			},
		},
		3: {
			State: replication.ReplicationSetStatePrepare,
			Captures: map[string]replication.Role{
				"a": replication.RoleSecondary,
			},
		},
		4: {
			State: replication.ReplicationSetStatePrepare, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RolePrimary, "b": replication.RoleSecondary,
			},
		},
		5: {
			State: replication.ReplicationSetStateRemoving, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RolePrimary,
			},
		},
		6: {
			State: replication.ReplicationSetStateReplicating, Primary: "b",
			Captures: map[string]replication.Role{
				"b": replication.RolePrimary,
			},
		},
		7: {
			State: replication.ReplicationSetStateCommit,
			Captures: map[string]replication.Role{
				"b": replication.RoleSecondary,
			},
		},
	})

	ok = scheduler.setTarget("a")
	require.True(t, ok)
	// not all table is replicating, skip this tick.
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 0)

	replications = mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		2: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		3: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		4: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
		6: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
		7: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
	})

	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 3)

	scheduler = newDrainCaptureScheduler(1, model.ChangeFeedID{})
	require.True(t, scheduler.setTarget("a"))
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Equal(t, "a", scheduler.target)
	require.Len(t, tasks, 1)
}

func TestDrainStoppingCapture(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	captures := make(map[model.CaptureID]*member.CaptureStatus)
	currentTables := make([]tablepb.Span, 0)
	replications := mapToSpanMap(make(map[model.TableID]*replication.ReplicationSet))
	scheduler := newDrainCaptureScheduler(10, model.ChangeFeedID{})

	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Empty(t, tasks)

	captures["a"] = &member.CaptureStatus{}
	captures["b"] = &member.CaptureStatus{State: member.CaptureStateStopping}
	replications = mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		2: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
	})
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.EqualValues(t, 2, tasks[0].MoveTable.Span.TableID)
	require.EqualValues(t, "a", tasks[0].MoveTable.DestCapture)
	require.EqualValues(t, "b", scheduler.getTarget())
}

func TestDrainSkipOwner(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	currentTables := make([]tablepb.Span, 0)
	captures := map[model.CaptureID]*member.CaptureStatus{
		"a": {},
		"b": {IsOwner: true, State: member.CaptureStateStopping},
	}
	replications := mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		2: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
	})
	scheduler := newDrainCaptureScheduler(10, model.ChangeFeedID{})
	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	require.EqualValues(t, captureIDNotDraining, scheduler.getTarget())
}

func TestDrainImbalanceCluster(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	currentTables := make([]tablepb.Span, 0)
	captures := map[model.CaptureID]*member.CaptureStatus{
		"a": {State: member.CaptureStateInitialized},
		"b": {IsOwner: true, State: member.CaptureStateInitialized},
	}
	replications := mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		2: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
	})
	scheduler := newDrainCaptureScheduler(10, model.ChangeFeedID{})
	scheduler.setTarget("a")
	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 2)
	require.EqualValues(t, "a", scheduler.getTarget())
}

func TestDrainEvenlyDistributedTables(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	currentTables := make([]tablepb.Span, 0)
	captures := map[model.CaptureID]*member.CaptureStatus{
		"a": {State: member.CaptureStateInitialized},
		"b": {IsOwner: true, State: member.CaptureStateInitialized},
		"c": {State: member.CaptureStateInitialized},
	}
	replications := mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		2: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		3: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
		6: {State: replication.ReplicationSetStateReplicating, Primary: "b"},
	})
	scheduler := newDrainCaptureScheduler(10, model.ChangeFeedID{})
	scheduler.setTarget("a")
	tasks := scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 3)
	taskMap := make(map[model.CaptureID]int)
	for _, t := range tasks {
		taskMap[t.MoveTable.DestCapture]++
	}
	require.Equal(t, 1, taskMap["b"])
	require.Equal(t, 2, taskMap["c"])
}
