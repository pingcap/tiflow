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
	"math/rand"

	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Scheduler is a full set of scheduling management, containing capacity provider,
// real scheduler and resource placement manager.
type Scheduler struct {
	infoProvider executorInfoProvider
	filters      []filter
}

// NewScheduler creates a new Scheduler instance
func NewScheduler(
	infoProvider executorInfoProvider,
	placementConstrainer PlacementConstrainer,
) *Scheduler {
	return &Scheduler{
		infoProvider: infoProvider,
		filters: []filter{
			newResourceFilter(placementConstrainer),
			newSelectorFilter(infoProvider),
		},
	}
}

// ScheduleTask tries to assign an executor to a given task.
func (s *Scheduler) ScheduleTask(
	ctx context.Context,
	request *schedModel.SchedulerRequest,
) (*schedModel.SchedulerResponse, error) {
	candidates, err := s.chainFilter(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, errors.ErrNoQualifiedExecutor.GenWithStackByArgs()
	}
	executorID := candidates[rand.Intn(len(candidates))]
	return &schedModel.SchedulerResponse{ExecutorID: executorID}, nil
}

// chainFilter runs the filter chain and returns a final candidate list.
func (s *Scheduler) chainFilter(
	ctx context.Context,
	request *schedModel.SchedulerRequest,
) (candidates []model.ExecutorID, retErr error) {
	infos := s.infoProvider.GetExecutorInfos()
	for id := range infos {
		candidates = append(candidates, id)
	}
	for _, filter := range s.filters {
		var err error
		candidates, err = filter.GetEligibleExecutors(ctx, request, candidates)
		if err != nil {
			// Note: filters are guaranteed to return errors when no suitable candidate is found.
			return nil, err
		}
	}
	return
}
