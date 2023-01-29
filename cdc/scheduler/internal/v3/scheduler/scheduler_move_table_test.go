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

func TestSchedulerMoveTable(t *testing.T) {
	t.Parallel()

	var checkpointTs model.Ts
	captures := map[model.CaptureID]*member.CaptureStatus{"a": {
		State: member.CaptureStateInitialized,
	}, "b": {
		State: member.CaptureStateInitialized,
	}}
	currentTables := spanz.ArrayToSpan([]model.TableID{1, 2, 3, 4})

	replications := mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
	})

	scheduler := newMoveTableScheduler(model.ChangeFeedID{})
	require.Equal(t, "move-table-scheduler", scheduler.Name())

	tasks := scheduler.Schedule(
		checkpointTs, currentTables, map[model.CaptureID]*member.CaptureStatus{}, replications)
	require.Len(t, tasks, 0)

	scheduler.addTask(tablepb.Span{TableID: 0}, "a")
	tasks = scheduler.Schedule(
		checkpointTs, currentTables, map[model.CaptureID]*member.CaptureStatus{}, replications)
	require.Len(t, tasks, 0)

	// move a not exist table
	scheduler.addTask(tablepb.Span{TableID: 0}, "a")
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	// move table to a not exist capture
	scheduler.addTask(tablepb.Span{TableID: 1}, "c")
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	// move table not replicating
	scheduler.addTask(tablepb.Span{TableID: 1}, "b")
	tasks = scheduler.Schedule(
		checkpointTs, currentTables, captures, spanz.NewBtreeMap[*replication.ReplicationSet]())
	require.Len(t, tasks, 0)

	scheduler.addTask(tablepb.Span{TableID: 1}, "b")
	replications.GetV(tablepb.Span{TableID: 1}).State = replication.ReplicationSetStatePrepare
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	scheduler.addTask(tablepb.Span{TableID: 1}, "b")
	replications.GetV(tablepb.Span{TableID: 1}).State = replication.ReplicationSetStateReplicating
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Equal(t, model.TableID(1), tasks[0].MoveTable.Span.TableID)
	require.Equal(t, "b", tasks[0].MoveTable.DestCapture)
	require.Equal(t, scheduler.tasks.GetV(tablepb.Span{TableID: 1}), tasks[0])

	// the target capture is stopping
	scheduler.addTask(tablepb.Span{TableID: 1}, "b")
	captures["b"].State = member.CaptureStateStopping
	tasks = scheduler.Schedule(checkpointTs, currentTables, captures, replications)
	require.Len(t, tasks, 0)
	require.False(t, scheduler.tasks.Has(tablepb.Span{TableID: 1}))
}
