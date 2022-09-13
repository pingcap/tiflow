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
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/stretchr/testify/require"
)

func TestSchedulerBasic(t *testing.T) {
	t.Parallel()

	captures := map[model.CaptureID]*member.CaptureStatus{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2, 3, 4}

	// Initial table dispatch.
	// AddTable only
	replications := map[model.TableID]*replication.ReplicationSet{}
	b := newBasicScheduler(2, model.ChangeFeedID{})

	// one capture stopping, another one is initialized
	captures["a"].State = member.CaptureStateStopping
	tasks := b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].BurstBalance.AddTables, 2)
	require.Equal(t, tasks[0].BurstBalance.AddTables[0].CaptureID, "b")
	require.Equal(t, tasks[0].BurstBalance.AddTables[1].CaptureID, "b")

	// all capture's stopping, cannot add table
	captures["b"].State = member.CaptureStateStopping
	tasks = b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	captures["a"].State = member.CaptureStateInitialized
	captures["b"].State = member.CaptureStateInitialized
	tasks = b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].BurstBalance.AddTables, 2)
	require.Equal(t, tasks[0].BurstBalance.AddTables[0].TableID, model.TableID(1))
	require.Equal(t, tasks[0].BurstBalance.AddTables[1].TableID, model.TableID(2))

	// Capture offline, causes replication.ReplicationSetStateAbsent.
	// AddTable only.
	replications = map[model.TableID]*replication.ReplicationSet{
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
	tasks = b.Schedule(1, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Equal(t, tasks[0].BurstBalance.AddTables[0].TableID, model.TableID(4))
	require.Equal(t, tasks[0].BurstBalance.AddTables[0].CheckpointTs, model.Ts(1))

	// DDL CREATE/DROP/TRUNCATE TABLE.
	// AddTable 4, and RemoveTable 5.
	replications = map[model.TableID]*replication.ReplicationSet{
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
		5: {
			State: replication.ReplicationSetStateCommit, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RoleUndetermined, "b": replication.RoleSecondary,
			},
		},
	}
	tasks = b.Schedule(2, currentTables, captures, replications)
	require.Len(t, tasks, 2)
	if tasks[0].BurstBalance.AddTables != nil {
		require.Equal(t, tasks[0].BurstBalance.AddTables[0].TableID, model.TableID(4))
		require.Equal(t, tasks[0].BurstBalance.AddTables[0].CheckpointTs, model.Ts(2))
		require.Equal(t, tasks[1].BurstBalance.RemoveTables[0].TableID, model.TableID(5))
	} else {
		require.Equal(t, tasks[1].BurstBalance.AddTables[0].TableID, model.TableID(4))
		require.Equal(t, tasks[0].BurstBalance.AddTables[0].CheckpointTs, model.Ts(2))
		require.Equal(t, tasks[0].BurstBalance.RemoveTables[0].TableID, model.TableID(5))
	}

	// RemoveTable only.
	replications = map[model.TableID]*replication.ReplicationSet{
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
		4: {
			State: replication.ReplicationSetStatePrepare, Primary: "a",
			Captures: map[string]replication.Role{
				"a": replication.RolePrimary, "b": replication.RoleSecondary,
			},
		},
		5: {
			State: replication.ReplicationSetStatePrepare,
			Captures: map[string]replication.Role{
				"b": replication.RoleUndetermined,
			},
		},
	}
	tasks = b.Schedule(3, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Equal(t, tasks[0].BurstBalance.RemoveTables[0].TableID, model.TableID(5))
}

func TestSchedulerPriority(t *testing.T) {
	t.Parallel()

	// The basic scheduler have the highest priority.
	require.Less(t, schedulerPriorityBasic, schedulerPriorityDrainCapture)
	require.Less(t, schedulerPriorityBasic, schedulerPriorityBalance)
	require.Less(t, schedulerPriorityBasic, schedulerPriorityMoveTable)
	require.Less(t, schedulerPriorityBasic, schedulerPriorityRebalance)
	require.Less(t, schedulerPriorityBasic, schedulerPriorityMax)
}

func benchmarkSchedulerBalance(
	b *testing.B,
	factory func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*member.CaptureStatus,
		replications map[model.TableID]*replication.ReplicationSet,
		sched scheduler,
	),
) {
	size := 16384
	for total := 1; total <= size; total *= 2 {
		name, currentTables, captures, replications, sched := factory(total)
		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sched.Schedule(0, currentTables, captures, replications)
			}
		})
		b.StopTimer()
	}
}

func BenchmarkSchedulerBasicAddTables(b *testing.B) {
	benchmarkSchedulerBalance(b, func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*member.CaptureStatus,
		replications map[model.TableID]*replication.ReplicationSet,
		sched scheduler,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*member.CaptureStatus{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &member.CaptureStatus{}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		replications = map[model.TableID]*replication.ReplicationSet{}
		name = fmt.Sprintf("AddTable %d", total)
		sched = newBasicScheduler(50, model.ChangeFeedID{})
		return name, currentTables, captures, replications, sched
	})
}

func BenchmarkSchedulerBasicRemoveTables(b *testing.B) {
	benchmarkSchedulerBalance(b, func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*member.CaptureStatus,
		replications map[model.TableID]*replication.ReplicationSet,
		sched scheduler,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*member.CaptureStatus{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &member.CaptureStatus{}
		}
		currentTables = make([]model.TableID, 0, total)
		replications = map[model.TableID]*replication.ReplicationSet{}
		for i := 0; i < total; i++ {
			replications[int64(10000+i)] = &replication.ReplicationSet{
				Primary: fmt.Sprint(i % captureCount),
			}
		}
		name = fmt.Sprintf("RemoveTable %d", total)
		sched = newBasicScheduler(50, model.ChangeFeedID{})
		return name, currentTables, captures, replications, sched
	})
}

func BenchmarkSchedulerBasicAddRemoveTables(b *testing.B) {
	benchmarkSchedulerBalance(b, func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*member.CaptureStatus,
		replications map[model.TableID]*replication.ReplicationSet,
		sched scheduler,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*member.CaptureStatus{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &member.CaptureStatus{}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total/2; i++ {
			currentTables = append(currentTables, int64(100000+i))
		}
		replications = map[model.TableID]*replication.ReplicationSet{}
		for i := 0; i < total/2; i++ {
			replications[int64(200000+i)] = &replication.ReplicationSet{
				Primary: fmt.Sprint(i % captureCount),
			}
		}
		name = fmt.Sprintf("AddRemoveTable %d", total)
		sched = newBasicScheduler(50, model.ChangeFeedID{})
		return name, currentTables, captures, replications, sched
	})
}
