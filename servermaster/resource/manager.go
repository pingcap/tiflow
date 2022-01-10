package resource

import (
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
)

// RescMgr manages the resources of the clusters.
type RescMgr interface {
	// Register registers new executor, it is called when an executor joins
	Register(id model.ExecutorID, addr string, capacity model.RescUnit)

	// Unregister is called when an executor exits
	Unregister(id model.ExecutorID)

	// Allocate allocates executor resources to given tasks
	Allocate(tasks []*pb.ScheduleTask) (bool, *pb.TaskSchedulerResponse)

	// Update updates executor resource usage and running status
	Update(id model.ExecutorID, use model.RescUnit, status model.ExecutorStatus) error
}

type ExecutorResource struct {
	ID     model.ExecutorID
	Status model.ExecutorStatus

	Capacity model.RescUnit
	Reserved model.RescUnit
	Used     model.RescUnit
	Addr     string
}
