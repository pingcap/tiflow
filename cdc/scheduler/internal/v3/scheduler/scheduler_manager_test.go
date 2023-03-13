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
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestNewSchedulerManager(t *testing.T) {
	t.Parallel()

	m := NewSchedulerManager(model.DefaultChangeFeedID("test-changefeed"),
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
	m := NewSchedulerManager(model.DefaultChangeFeedID("test-changefeed"), cfg)

	captures := map[model.CaptureID]*member.CaptureStatus{
		"a": {State: member.CaptureStateInitialized},
		"b": {State: member.CaptureStateInitialized},
	}
	currentSpans := []tablepb.Span{{TableID: 1}}
	// schedulerPriorityBasic bypasses task check.
	replications := mapToSpanMap(map[model.TableID]*replication.ReplicationSet{})
	runningTasks := mapToSpanMap(map[model.TableID]*replication.ScheduleTask{1: {}})
	tasks := m.Schedule(0, currentSpans, captures, replications, runningTasks)
	require.Len(t, tasks, 1)

	// No more task.
	replications = mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
	})
	tasks = m.Schedule(0, currentSpans, captures, replications, runningTasks)
	require.Len(t, tasks, 0)

	// Move table is dropped because of running tasks.
	m.MoveTable(tablepb.Span{TableID: 1}, "b")
	replications = mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
	})
	tasks = m.Schedule(0, currentSpans, captures, replications, runningTasks)
	require.Len(t, tasks, 0)

	// Move table can proceed after clean up tasks.
	m.MoveTable(tablepb.Span{TableID: 1}, "b")
	replications = mapToSpanMap(map[model.TableID]*replication.ReplicationSet{
		1: {State: replication.ReplicationSetStateReplicating, Primary: "a"},
	})
	runningTasks = spanz.NewBtreeMap[*replication.ScheduleTask]()
	tasks = m.Schedule(0, currentSpans, captures, replications, runningTasks)
	require.Len(t, tasks, 1)
}
