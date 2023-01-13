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
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
)

func getMockDataForScheduler() *mockExecutorInfoProvider {
	return &mockExecutorInfoProvider{
		infos: map[model.ExecutorID]schedModel.ExecutorInfo{
			"executor-1": {
				ID: "executor-1",
				Labels: label.Set{
					"type":     "test-type",
					"function": "test-function",
				},
			},
			"executor-2": {ID: "executor-2"},
			"executor-3": {ID: "executor-3"},
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

func TestSchedulerByConstraint(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-2"}},
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-2"}, resp)
}

func TestSchedulerNoResourceConstraint(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		// resource-4 has no constraint
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-4"}},
		Selectors: []*label.Selector{
			{
				Key:    "type",
				Target: "test-type",
				Op:     "eq",
			},
			{
				Key:    "function",
				Target: "test-function",
				Op:     "eq",
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-1"}, resp)
}

func TestSchedulerResourceNotFound(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		// resource-blah DOES NOT exist
		ExternalResources: []resModel.ResourceKey{{JobID: "fakeJob", ID: "resource-blah"}},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrResourceDoesNotExist))
}

func TestSchedulerConstraintConflict(t *testing.T) {
	sched := NewScheduler(
		getMockDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
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
	require.True(t, errors.Is(err, errors.ErrResourceConflict))
}

func TestSchedulerNoQualifiedExecutor(t *testing.T) {
	sched := NewScheduler(
		&mockExecutorInfoProvider{infos: map[model.ExecutorID]schedModel.ExecutorInfo{}},
		&MockPlacementConstrainer{},
	)

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{})
	require.ErrorIs(t, err, errors.ErrNoQualifiedExecutor)
}
