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
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
)

func TestSelectorFilter(t *testing.T) {
	t.Parallel()

	provider := &mockExecutorInfoProvider{
		infos: map[model.ExecutorID]schedModel.ExecutorInfo{
			"executor-1": {
				ID: "executor-1",
				Labels: label.Set{
					"type":     "1",
					"function": "1",
				},
			},
			"executor-2": {
				ID: "executor-2",
				Labels: label.Set{
					"type":     "2",
					"function": "1",
				},
			},
			"executor-3": {
				ID: "executor-2",
				Labels: label.Set{
					"type":     "2",
					"function": "2",
				},
			},
		},
	}
	sf := newSelectorFilter(provider)

	ret, err := sf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		Selectors: []*label.Selector{
			{
				Key:    "type",
				Target: "2",
				Op:     "eq",
			},
			{
				Key:    "function",
				Target: "1",
				Op:     "eq",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.NoError(t, err)
	require.Equal(t, []model.ExecutorID{"executor-2"}, ret)

	_, err = sf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		Selectors: []*label.Selector{
			{
				Key:    "type",
				Target: "3",
				Op:     "eq",
			},
			{
				Key:    "function",
				Target: "1",
				Op:     "eq",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSelectorUnsatisfied))
}

func TestCandidateExecutorGone(t *testing.T) {
	t.Parallel()

	provider := &mockExecutorInfoProvider{
		infos: map[model.ExecutorID]schedModel.ExecutorInfo{
			"executor-1": {
				ID: "executor-1",
				Labels: label.Set{
					"type":     "1",
					"function": "1",
				},
			},
			"executor-2": {
				ID: "executor-2",
				Labels: label.Set{
					"type":     "2",
					"function": "1",
				},
			},
			"executor-3": {
				ID: "executor-2",
				Labels: label.Set{
					"type":     "2",
					"function": "2",
				},
			},
		},
	}
	sf := newSelectorFilter(provider)
	ret, err := sf.GetEligibleExecutors(context.Background(), &schedModel.SchedulerRequest{
		Selectors: []*label.Selector{
			{
				Key:    "function",
				Target: "1",
				Op:     "eq",
			},
		},
	}, []model.ExecutorID{"executor-1", "executor-2", "executor-3", "executor-4"})
	require.NoError(t, err)
	require.Equal(t, []model.ExecutorID{"executor-1", "executor-2"}, ret)
}
