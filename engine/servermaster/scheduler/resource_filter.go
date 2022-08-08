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
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
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

func (f *resourceFilter) GetEligibleExecutors(
	ctx context.Context,
	request *schedModel.SchedulerRequest,
	candidates []model.ExecutorID,
) ([]model.ExecutorID, error) {
	if len(request.ExternalResources) == 0 {
		return candidates, nil
	}

	var (
		lastResourceID  resModel.ResourceID
		finalExecutorID model.ExecutorID
	)

	finalExecutorID = noExecutorConstraint
	for _, resource := range request.ExternalResources {
		resourceID := resource.ID
		executorID, hasConstraint, err := f.constrainer.GetPlacementConstraint(ctx, resource)
		if err != nil {
			if errors.ErrResourceDoesNotExist.Equal(err) {
				return nil, ErrResourceNotFound.GenWithStack(&ResourceNotFoundError{
					ResourceID: resourceID,
					Details:    err.Error(),
				})
			}
			return nil, err
		}
		if !hasConstraint {
			log.Info("No constraint is found for resource",
				zap.String("resource-id", resourceID))
			continue
		}
		log.Info("Found resource constraint for resource",
			zap.String("resource-id", resourceID),
			zap.String("executor-id", string(executorID)))

		if finalExecutorID != noExecutorConstraint && finalExecutorID != executorID {
			// Conflicting constraints.
			// We are forced to schedule the task to
			// two different executors, which is impossible.
			log.Warn("Conflicting resource constraints",
				zap.Any("resources", request.ExternalResources))
			return nil, ErrResourceConflict.GenWithStack(&ResourceConflictError{
				ConflictingResources: [2]resModel.ResourceID{
					resourceID, lastResourceID,
				},
				AssignedExecutors: [2]model.ExecutorID{
					executorID, finalExecutorID,
				},
			})
		}
		finalExecutorID = executorID
		lastResourceID = resourceID
	}

	if finalExecutorID == noExecutorConstraint {
		return candidates, nil
	}

	// Check that finalExecutorID should be found in the input candidates.
	if slices.Index(candidates, finalExecutorID) == -1 {
		return nil, ErrFilterNoResult.GenWithStack(&FilterNoResultError{
			FilterName:      "resource",
			InputCandidates: candidates,
		})
	}
	return []model.ExecutorID{finalExecutorID}, nil
}
