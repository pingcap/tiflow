package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/model"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

func getMockCapacityDataForScheduler() CapacityProvider {
	return &MockCapacityProvider{
		Capacities: map[model.ExecutorID]*schedModel.ExecutorResourceStatus{
			"executor-1": {
				Capacity: 100,
				Reserved: 40,
				Used:     60,
			}, // available = 40
			"executor-2": {
				Capacity: 100,
				Reserved: 30,
				Used:     70,
			}, // available = 30
			"executor-3": {
				Capacity: 100,
				Reserved: 70,
				Used:     30,
			}, // available = 30
		},
	}
}

func getMockResourceConstraintForScheduler() PlacementConstrainer {
	return &MockPlacementConstrainer{ResourceList: map[resourcemeta.ResourceID]model.ExecutorID{
		"resource-1": "executor-1",
		"resource-2": "executor-2",
		"resource-3": "executor-3",
		"resource-4": "", // no constraint
	}}
}

func TestSchedulerByCost(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 35,
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-1"}, resp)
}

func TestSchedulerByConstraint(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost:              20,
		ExternalResources: []resourcemeta.ResourceID{"resource-2"},
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-2"}, resp)
}

func TestSchedulerNoConstraint(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	resp, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 35,
		// resource-4 has no constraint, so scheduling by cost is used.
		ExternalResources: []resourcemeta.ResourceID{"resource-4"},
	})
	require.NoError(t, err)
	require.Equal(t, &schedModel.SchedulerResponse{ExecutorID: "executor-1"}, resp)
}

func TestSchedulerResourceOwnerNoCapacity(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 50,
		// resource-3 requires executor-3, but it does not have the capacity
		ExternalResources: []resourcemeta.ResourceID{"resource-3"},
	})
	require.Error(t, err)
	require.Regexp(t, ".*ErrClusterResourceNotEnough.*", err)
}

func TestSchedulerResourceNotFound(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 50,
		// resource-blah DOES NOT exist
		ExternalResources: []resourcemeta.ResourceID{"resource-blah"},
	})
	require.Error(t, err)
	require.Regexp(t, ".*Scheduler could not find resource resource-blah.*", err)
}

func TestSchedulerByCostNoCapacity(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		// No executor has the capacity to run this
		Cost: 50,
	})
	require.Error(t, err)
	require.Regexp(t, ".*ErrClusterResourceNotEnough.*", err)
}

func TestSchedulerConstraintConflict(t *testing.T) {
	sched := NewScheduler(
		getMockCapacityDataForScheduler(),
		getMockResourceConstraintForScheduler())

	_, err := sched.ScheduleTask(context.Background(), &schedModel.SchedulerRequest{
		Cost: 10,
		ExternalResources: []resourcemeta.ResourceID{
			"resource-1",
			"resource-2",
		},
	})
	require.Error(t, err)
	require.Regexp(t, ".*Scheduler could not assign executor due to conflicting.*", err)
}
