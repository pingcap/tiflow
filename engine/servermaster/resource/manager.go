package resource

import (
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/servermaster/scheduler"
)

// RescMgr manages the resources of the clusters.
type RescMgr interface {
	// CapacityProvider is embedded to guarantee that any RescMgr can be used
	// to provide capacity info to scheduler.Scheduler.
	scheduler.CapacityProvider

	// Register registers new executor, it is called when an executor joins
	Register(id model.ExecutorID, addr string, capacity model.RescUnit)

	// Unregister is called when an executor exits
	Unregister(id model.ExecutorID)

	// Update updates executor resource usage and running status
	Update(id model.ExecutorID, used, reserved model.RescUnit, status model.ExecutorStatus) error
}

type ExecutorResource struct {
	ID     model.ExecutorID
	Status model.ExecutorStatus

	// Capacity of the resource in this executor.
	Capacity model.RescUnit
	// Reserved resource in this node, meaning the max resource possible to use.
	// It's supposed to be the total cost of running tasks in this executor.
	Reserved model.RescUnit
	// Actually used resource in this node. It's supposed to be less than the reserved resource.
	// But if the estimated reserved is not accurate, `Used` might be larger than `Reserved`.
	Used model.RescUnit
	Addr string
}
