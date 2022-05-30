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

package resource

import (
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
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

// ExecutorResource defines the capacity usage of an executor
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
