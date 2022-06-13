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
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSchedulerBalanceCaptureOnline(t *testing.T) {
	t.Parallel()

	sched := newBalanceScheduler(&config.SchedulerConfig{
		CheckBalanceInterval: config.TomlDuration(time.Duration(0)),
	})
	sched.random = nil

	// New capture "b" online
	captures := map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	currentTables := []model.TableID{1, 2}
	replications := map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks := sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].burstBalance.MoveTables, 1)
	require.Equal(t, tasks[0].burstBalance.MoveTables[0].TableID, model.TableID(1))

	// New capture "b" online, but this time it not pass check balance interval.
	sched.checkBalanceInterval = time.Hour
	captures = map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}}
	currentTables = []model.TableID{1, 2}
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
		2: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks = sched.Schedule(0, currentTables, captures, replications)
	require.Len(t, tasks, 0)

	// TODO revise balance algorithm and enable the test case.
	//
	// Capture "c" online, and more tables
	// captures = map[model.CaptureID]*model.CaptureInfo{"a": {}, "b": {}, "c": {}}
	// currentTables = []model.TableID{1, 2, 3, 4}
	// replications = map[model.TableID]*ReplicationSet{
	// 	1: {State: ReplicationSetStateReplicating, Primary: "a"},
	// 	2: {State: ReplicationSetStateReplicating, Primary: "b"},
	// 	3: {State: ReplicationSetStateReplicating, Primary: "a"},
	// 	4: {State: ReplicationSetStateReplicating, Primary: "b"},
	// }
	// tasks = b.Schedule(0, currentTables, captures, replications)
	// require.Len(t, tasks, 1)
	// require.Len(t, tasks[0].burstBalance.MoveTables, 1)
	// require.Equal(t, tasks[0].burstBalance.MoveTables[0].TableID, model.TableID(1))
}
