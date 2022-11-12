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
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// PlacementConstrainer describes an object that provides
// the placement constraint for an external resource.
type PlacementConstrainer interface {
	GetPlacementConstraint(
		ctx context.Context,
		resourceKey resModel.ResourceKey,
	) (resModel.ExecutorID, bool, error)
}

// MockPlacementConstrainer uses a resource executor binding map to implement PlacementConstrainer
type MockPlacementConstrainer struct {
	ResourceList map[resModel.ResourceKey]model.ExecutorID
}

// GetPlacementConstraint implements PlacementConstrainer.GetPlacementConstraint
func (c *MockPlacementConstrainer) GetPlacementConstraint(
	_ context.Context,
	resourceKey resModel.ResourceKey,
) (resModel.ExecutorID, bool, error) {
	executorID, exists := c.ResourceList[resourceKey]
	if !exists {
		return "", false, errors.ErrResourceDoesNotExist.GenWithStackByArgs(resourceKey.ID)
	}
	if executorID == "" {
		return "", false, nil
	}
	return executorID, true, nil
}

// resourceFilter filters candidate executors by
// the request's resource requirements.
type resourceFilter struct {
	constrainer PlacementConstrainer
}

func newResourceFilter(constrainer PlacementConstrainer) *resourceFilter {
	return &resourceFilter{constrainer: constrainer}
}

const (
	noExecutorConstraint = ""
)

// getExecutor locates one unique executor onto which the task should be scheduled.
// If no placement requirement is found, noExecutorConstraint is returned.
func (f *resourceFilter) getExecutor(
	ctx context.Context,
	request *schedModel.SchedulerRequest,
) (model.ExecutorID, error) {
	var (
		lastResourceID resModel.ResourceID
		ret            model.ExecutorID
	)

	ret = noExecutorConstraint
	for _, resource := range request.ExternalResources {
		resourceID := resource.ID
		// GetPlacementConstraint() may incur database queries.
		// Note the performance.
		executorID, hasConstraint, err := f.constrainer.GetPlacementConstraint(ctx, resource)
		if err != nil {
			return noExecutorConstraint, err
		}
		if !hasConstraint {
			// The resource requires no special placement.
			log.Debug("No constraint is found for resource",
				zap.String("resource-id", resourceID))
			continue
		}
		log.Info("Found resource constraint for resource",
			zap.String("resource-id", resourceID),
			zap.String("executor-id", string(executorID)))

		if ret != noExecutorConstraint && ret != executorID {
			// Conflicting constraints.
			// We are forced to schedule the task to
			// two different executors, which is impossible.
			log.Warn("Conflicting resource constraints",
				zap.Any("resources", request.ExternalResources))
			return noExecutorConstraint, errors.ErrResourceConflict.
				GenWithStackByArgs(resourceID, lastResourceID, ret, executorID)
		}
		ret = executorID
		lastResourceID = resourceID
	}
	return ret, nil
}

func (f *resourceFilter) GetEligibleExecutors(
	ctx context.Context,
	request *schedModel.SchedulerRequest,
	candidates []model.ExecutorID,
) ([]model.ExecutorID, error) {
	if len(request.ExternalResources) == 0 {
		return candidates, nil
	}

	finalCandidate, err := f.getExecutor(ctx, request)
	if err != nil {
		return nil, err
	}

	if finalCandidate == noExecutorConstraint {
		// Any executor is good.
		return candidates, nil
	}

	// Check that finalCandidate is found in the input candidates.
	if slices.Index(candidates, finalCandidate) == -1 {
		return nil, errors.ErrFilterNoResult.GenWithStackByArgs("resource")
	}
	return []model.ExecutorID{finalCandidate}, nil
}
