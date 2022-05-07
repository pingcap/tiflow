package scheduler

import (
	"context"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

type Scheduler struct {
	capacityProvider     CapacityProvider
	costScheduler        *CostScheduler
	placementConstrainer PlacementConstrainer
}

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
		return nil, derror.ErrClusterResourceNotEnough.GenWithStackByArgs()
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
	return nil, derror.ErrClusterResourceNotEnough.GenWithStackByArgs()
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
	resources []resourcemeta.ResourceID,
) (model.ExecutorID, error) {
	var (
		lastResourceID resourcemeta.ResourceID
		ret            model.ExecutorID
	)
	for _, resourceID := range resources {
		executorID, hasConstraint, err := s.placementConstrainer.GetPlacementConstraint(ctx, resourceID)
		if err != nil {
			if derror.ErrResourceDoesNotExist.Equal(err) {
				return "", schedModel.NewResourceNotFoundError(resourceID, err)
			}
			return "", err
		}
		if !hasConstraint {
			// TODO change this to Debug when this part of code
			// has been stabilized.
			log.L().Info("No constraint is found for resource",
				zap.String("resource-id", resourceID))
			continue
		}
		log.L().Info("Found resource constraint for resource",
			zap.String("resource-id", resourceID),
			zap.String("executor-id", string(executorID)))

		if ret != "" && ret != executorID {
			// Conflicting constraints.
			// We are forced to schedule the task to
			// two different executors, which is impossible.
			log.L().Warn("Conflicting resource constraints",
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
