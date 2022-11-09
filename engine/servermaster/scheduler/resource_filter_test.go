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
	"github.com/stretchr/testify/require"
)

func mockResourceConstraintForFilter() PlacementConstrainer {
	return &MockPlacementConstrainer{ResourceList: map[resModel.ResourceKey]model.ExecutorID{
		{JobID: "fakeJob", ID: "resource-1"}: "executor-1",
		{JobID: "fakeJob", ID: "resource-2"}: "executor-2",
		{JobID: "fakeJob", ID: "resource-3"}: "executor-3",
		{JobID: "fakeJob", ID: "resource-4"}: "", // no constraint
		{JobID: "fakeJob", ID: "resource-5"}: "executor-5",
		{JobID: "fakeJob", ID: "resource-6"}: "executor-1",
	}}
}

func TestResourceFilterNormal(t *testing.T) {
	rf := newResourceFilter(mockResourceConstraintForFilter())
	ret, err := rf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{{
			ID:    "resource-1",
			JobID: "fakeJob",
		}},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.NoError(t, err)
	require.Equal(t, []model.ExecutorID{"executor-1"}, ret)

	ret, err = rf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{
			{
				ID:    "resource-1",
				JobID: "fakeJob",
			},
			{
				ID:    "resource-6",
				JobID: "fakeJob",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.NoError(t, err)
	require.Equal(t, []model.ExecutorID{"executor-1"}, ret)
}

func TestResourceFilterConflict(t *testing.T) {
	rf := newResourceFilter(mockResourceConstraintForFilter())
	_, err := rf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{
			{
				ID:    "resource-1",
				JobID: "fakeJob",
			},
			{
				ID:    "resource-2",
				JobID: "fakeJob",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrResourceConflict))
}

func TestResourceFilterResourceNotFound(t *testing.T) {
	rf := newResourceFilter(mockResourceConstraintForFilter())
	_, err := rf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{
			{
				ID:    "resource-7",
				JobID: "fakeJob",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrResourceDoesNotExist))
}

func TestResourceNoConstraint(t *testing.T) {
	rf := newResourceFilter(mockResourceConstraintForFilter())
	ret, err := rf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{
			{
				ID:    "resource-4",
				JobID: "fakeJob",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.NoError(t, err)
	require.Equal(t, []model.ExecutorID{"executor-1", "executor-2", "executor-3"}, ret)
}

func TestResourceExecutorGone(t *testing.T) {
	rf := newResourceFilter(mockResourceConstraintForFilter())
	_, err := rf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		ExternalResources: []resModel.ResourceKey{
			{
				ID:    "resource-5",
				JobID: "fakeJob",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFilterNoResult))
}
