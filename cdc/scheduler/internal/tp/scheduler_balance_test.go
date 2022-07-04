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
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestSchedulerBalanceCaptureOnline(t *testing.T) {
	t.Parallel()

	sched := newBalanceScheduler(time.Duration(0), 3)
	sched.random = nil

	// New capture "b" online
	captures := map[model.CaptureID]*CaptureStatus{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2}
	replications := map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks := sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].moveTable)
	require.Equal(t, tasks[0].moveTable.TableID, model.TableID(1))

	// New capture "b" online, but this time has capture is stopping
	captures["a"].State = CaptureStateStopping
	tasks = sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	// New capture "b" online, but this time it not pass check balance interval.
	sched.checkBalanceInterval = time.Hour
	captures = map[model.CaptureID]*CaptureStatus{"a": {}, "b": {}}
	currentTables = []model.TableID{1, 2}
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks = sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 0)
}

func TestSchedulerBalanceTaskLimit(t *testing.T) {
	t.Parallel()

	sched := newBalanceScheduler(time.Duration(0), 2)
	sched.random = nil

	// New capture "b" online
	captures := map[model.CaptureID]*CaptureStatus{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2, 3, 4}
	replications := map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
		3: {State: ReplicationSetStateReplicating, Primary: "a"},
		4: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks := sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 2)

	sched = newBalanceScheduler(time.Duration(0), 1)
	tasks = sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
}
