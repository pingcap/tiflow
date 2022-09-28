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
	"context"
	"testing"

	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/stretchr/testify/require"
)

func getMockDataForScheduler() *mockExecutorInfoProvider {
	return &mockExecutorInfoProvider{
		infos: map[model.ExecutorID]schedModel.ExecutorInfo{
			"executor-1": {
				ID: "executor-1",
				ResourceStatus: schedModel.ExecutorResourceStatus{
					Capacity: 100,
					Reserved: 40,
					Used:     60,
				},
			}, // available = 40
			"executor-2": {
				ID: "executor-2",
				ResourceStatus: schedModel.ExecutorResourceStatus{
					Capacity: 100,
					Reserved: 30,
					Used:     70,
				},
			}, // available = 30
			"executor-3": {
				ID: "executor-3",
				ResourceStatus: schedModel.ExecutorResourceStatus{
					Capacity: 100,
					Reserved: 70,
					Used:     30,
				},
			}, // available = 30
		},
	}
}

func getMockResourceConstraintForScheduler() PlacementConstrainer {
	return &MockPlacementConstrainer{ResourceList: map[resModel.ResourceKey]model.ExecutorID{
		{JobID: "fakeJob", ID: "resource-1"}: "executor-1",
		{JobID: "fakeJob", ID: "resource-2"}: "executor-2",
		{JobID: "fakeJob", ID: "resource-3"}: "executor-3",
		{JobID: "fakeJob", ID: "resource-4"}: "", // no constraint
	}}
}

func TestSchedulerByCost(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 35,
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-1"}, resp)
}

func TestSchedulerByConstraint(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost:              20,
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-2"}},
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-2"}, resp)
}

func TestSchedulerNoConstraint(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 35,
		// resource-4 has no constraint, so scheduling by cost is used.
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-4"}},
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-1"}, resp)
}

func TestSchedulerResourceOwnerNoCapacity(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 50,
		// resource-3 requires executor-3, but it does not have the capacity
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-3"}},
	})
	require.Error(t, err)
	require.Regexp(t, "CapacityNotEnoughError", err)
}

func TestSchedulerResourceNotFound(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 50,
		// resource-blah DOES NOT exist
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-blah"}},
	})
	require.Error(t, err)
	info, ok := ErrResourceNotFound.Convert(err)
	require.True(t, ok)
	require.Equal(t, "resource-blah", info.ResourceID)
}

func TestSchedulerByCostNoCapacity(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		// No executor has the capacity to run this
		Cost: 50,
	})
	require.Error(t, err)
	require.Regexp(t, "CapacityNotEnoughError", err)
}

func TestSchedulerConstraintConflict(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 10,
		ExternalResources: []resModel.ResourceKey{
			{
				JobID: "fakeJob",
				ID:    "resource-1",
			},
			{
				JobID: "fakeJob",
				ID:    "resource-2",
			},
		},
	})
	require.Error(t, err)
	require.Regexp(t, "ResourceConflictError", err)
}
