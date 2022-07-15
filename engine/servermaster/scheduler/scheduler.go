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

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Scheduler is a full set of scheduling management, containing capacity provider,
// real scheduler and resource placement manager.
type Scheduler struct {
	capacityProvider     CapacityProvider
	costScheduler        *CostScheduler
	placementConstrainer PlacementConstrainer
}

// NewScheduler creates a new Scheduler instance
func NewScheduler(
	capacityProvider CapacityProvider,
	placementConstrainer PlacementConstrainer,
) *Scheduler {
	return &Scheduler{
		capacityProvider:     capacityProvider,
		costScheduler:        NewRandomizedCostScheduler(capacityProvider),
		placementConstrainer: placementConstrainer,
	}
}

// ScheduleTask tries to assign an executor to a given task.
func (s *Scheduler) ScheduleTask(
	ctx context.Context,
	request *schedModel.SchedulerRequest,
) (*schedModel.SchedulerResponse, error) {
	if len(request.ExternalResources) == 0 {
		// There is no requirement for external resources.
		return s.scheduleByCostOnly(request)
	}

	constraint, err := s.getConstraint(ctx, request.ExternalResources)
	if err != nil {
		return nil, err
	}
	if constraint == "" {
		// No constraint is found
		return s.scheduleByCostOnly(request)
	}

	// Checks that the required executor has enough capacity to
	// run the task.
	if !s.checkCostAllows(request, constraint) {
		return nil, errors.ErrClusterResourceNotEnough.GenWithStackByArgs()
	}
	return &schedModel.SchedulerResponse{ExecutorID: constraint}, nil
}

func (s *Scheduler) scheduleByCostOnly(
	request *schedModel.SchedulerRequest,
) (*schedModel.SchedulerResponse, error) {
	target, ok := s.costScheduler.ScheduleByCost(request.Cost)
	if ok {
		return &schedModel.SchedulerResponse{
			ExecutorID: target,
		}, nil
	}
	return nil, errors.ErrClusterResourceNotEnough.GenWithStackByArgs()
}

func (s *Scheduler) checkCostAllows(
	request *schedModel.SchedulerRequest,
	target model.ExecutorID,
) bool {
	executorResc, ok := s.capacityProvider.CapacityForExecutor(target)
	if !ok {
		// Executor is gone.
		return false
	}
	remaining := executorResc.Remaining()
	return remaining >= request.Cost
}

func (s *Scheduler) getConstraint(
	ctx context.Context,
	resources []resourcemeta.ResourceKey,
) (model.ExecutorID, error) {
	var (
		lastResourceID resModel.ResourceID
		ret            model.ExecutorID
	)
	for _, resource := range resources {
		resourceID := resource.ID
		executorID, hasConstraint, err := s.placementConstrainer.GetPlacementConstraint(ctx, resource)
		if err != nil {
			if errors.ErrResourceDoesNotExist.Equal(err) {
				return "", schedModel.NewResourceNotFoundError(resourceID, err)
			}
			return "", err
		}
		if !hasConstraint {
			// TODO change this to Debug when this part of code
			// has been stabilized.
			log.Info("No constraint is found for resource",
				zap.String("resource-id", resourceID))
			continue
		}
		log.Info("Found resource constraint for resource",
			zap.String("resource-id", resourceID),
			zap.String("executor-id", string(executorID)))

		if ret != "" && ret != executorID {
			// Conflicting constraints.
			// We are forced to schedule the task to
			// two different executors, which is impossible.
			log.Warn("Conflicting resource constraints",
				zap.Any("resources", resources))
			return "", schedModel.NewResourceConflictError(
				resourceID, executorID,
				lastResourceID, ret)
		}
		ret = executorID
		lastResourceID = resourceID
	}
	return ret, nil
}
