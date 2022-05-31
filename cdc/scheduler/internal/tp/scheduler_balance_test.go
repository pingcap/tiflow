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
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestSchedulerBalance(t *testing.T) {
	t.Parallel()

	captures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2, 3, 4}

	// AddTable only
	replications := map[model.TableID]*ReplicationSet{}
	b := newBalancer()
	tasks := b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].burstBalance.AddTables, model.TableID(1))
	require.Contains(t, tasks[0].burstBalance.AddTables, model.TableID(2))
	require.Contains(t, tasks[0].burstBalance.AddTables, model.TableID(3))
	require.Contains(t, tasks[0].burstBalance.AddTables, model.TableID(4))

	// AddTable ReplicationSetStateAbsent.
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateCommit, Secondary: "b"},
		3: {State: ReplicationSetStatePrepare, Primary: "a", Secondary: "b"},
		4: {State: ReplicationSetStateAbsent},
	}
	tasks = b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].burstBalance.AddTables, model.TableID(4))

	// AddTable 4, and RemoveTable 5.
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateCommit, Secondary: "b"},
		3: {State: ReplicationSetStatePrepare, Primary: "a", Secondary: "b"},
		5: {State: ReplicationSetStateCommit, Primary: "a", Secondary: "b"},
	}
	tasks = b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 2)
	if tasks[0].burstBalance.AddTables != nil {
		require.Contains(t, tasks[0].burstBalance.AddTables, model.TableID(4))
		require.Contains(t, tasks[1].burstBalance.RemoveTables, model.TableID(5))
	} else {
		require.Contains(t, tasks[1].burstBalance.AddTables, model.TableID(4))
		require.Contains(t, tasks[0].burstBalance.RemoveTables, model.TableID(5))
	}

	// RemoveTable only.
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateCommit, Secondary: "b"},
		3: {State: ReplicationSetStatePrepare, Primary: "a", Secondary: "b"},
		4: {State: ReplicationSetStatePrepare, Primary: "a", Secondary: "b"},
		5: {State: ReplicationSetStatePrepare, Secondary: "b"},
	}
	tasks = b.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].burstBalance.RemoveTables, model.TableID(5))
}

func benchmarkSchedulerBalance(
	b *testing.B,
	factory func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
		replications map[model.TableID]*ReplicationSet,
	),
) {
	size := 16384
	for total := 1; total <= size; total *= 2 {
		name, currentTables, captures, replications := factory(total)
		bal := newBalancer()
		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bal.Schedule(0, currentTables, captures, replications)
			}
		})
		b.StopTimer()
	}
}

func BenchmarkSchedulerBalanceAddTables(b *testing.B) {
	benchmarkSchedulerBalance(b, func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
		replications map[model.TableID]*ReplicationSet,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total; i++ {
			currentTables = append(currentTables, int64(10000+i))
		}
		replications = map[model.TableID]*ReplicationSet{}
		name = fmt.Sprintf("AddTable %d", total)
		return name, currentTables, captures, replications
	})
}

func BenchmarkSchedulerBalanceRemoveTables(b *testing.B) {
	benchmarkSchedulerBalance(b, func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
		replications map[model.TableID]*ReplicationSet,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
		}
		currentTables = make([]model.TableID, 0, total)
		replications = map[model.TableID]*ReplicationSet{}
		for i := 0; i < total; i++ {
			replications[int64(10000+i)] = &ReplicationSet{
				Primary: fmt.Sprint(i % captureCount),
			}
		}
		name = fmt.Sprintf("RemoveTable %d", total)
		return name, currentTables, captures, replications
	})
}

func BenchmarkSchedulerBalanceAddRemoveTables(b *testing.B) {
	benchmarkSchedulerBalance(b, func(total int) (
		name string,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
		replications map[model.TableID]*ReplicationSet,
	) {
		const captureCount = 8
		captures = map[model.CaptureID]*model.CaptureInfo{}
		for i := 0; i < captureCount; i++ {
			captures[fmt.Sprint(i)] = &model.CaptureInfo{}
		}
		currentTables = make([]model.TableID, 0, total)
		for i := 0; i < total/2; i++ {
			currentTables = append(currentTables, int64(100000+i))
		}
		replications = map[model.TableID]*ReplicationSet{}
		for i := 0; i < total/2; i++ {
			replications[int64(200000+i)] = &ReplicationSet{
				Primary: fmt.Sprint(i % captureCount),
			}
		}
		name = fmt.Sprintf("AddRemoveTable %d", total)
		return name, currentTables, captures, replications
	})
}
