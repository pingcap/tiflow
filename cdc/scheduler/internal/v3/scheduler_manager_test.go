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

package v3

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewSchedulerManager(t *testing.T) {
	t.Parallel()

	m := newSchedulerManager(model.DefaultChangeFeedID("test-changefeed"),
		config.NewDefaultSchedulerConfig())
	require.NotNil(t, m)
	require.NotNil(t, m.schedulers[schedulerPriorityBasic])
	require.NotNil(t, m.schedulers[schedulerPriorityBalance])
	require.NotNil(t, m.schedulers[schedulerPriorityMoveTable])
	require.NotNil(t, m.schedulers[schedulerPriorityRebalance])
	require.NotNil(t, m.schedulers[schedulerPriorityDrainCapture])
}

func TestSchedulerManagerScheduler(t *testing.T) {
	t.Parallel()

	cfg := config.NewDefaultSchedulerConfig()
	cfg.MaxTaskConcurrency = 1
	m := newSchedulerManager(model.DefaultChangeFeedID("test-changefeed"), cfg)

	captures := map[model.CaptureID]*CaptureStatus{
		"a": {State: CaptureStateInitialized},
		"b": {State: CaptureStateInitialized},
	}
	currentTables := []model.TableID{1}

	// schedulerPriorityBasic bypasses task check.
	replications := map[model.TableID]*ReplicationSet{}
	runningTasks := map[model.TableID]*scheduleTask{1: {}}
	tasks := m.Schedule(0, currentTables, captures, replications, runningTasks)
	require.Len(t, tasks, 1)

	// No more task.
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks = m.Schedule(0, currentTables, captures, replications, runningTasks)
	require.Len(t, tasks, 0)

	// Move table is drop because of running tasks.
	m.MoveTable(1, "b")
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	tasks = m.Schedule(0, currentTables, captures, replications, runningTasks)
	require.Len(t, tasks, 0)

	// Move table can proceed after clean up tasks.
	m.MoveTable(1, "b")
	replications = map[model.TableID]*ReplicationSet{
		1: {State: ReplicationSetStateReplicating, Primary: "a"},
	}
	runningTasks = map[model.TableID]*scheduleTask{}
	tasks = m.Schedule(0, currentTables, captures, replications, runningTasks)
	require.Len(t, tasks, 1)
}
