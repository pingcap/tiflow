package runtime

import (
	"math/rand"

	"github.com/hanfei1991/microcosm/model"
)

// TaskRescUnit defines the minimum resource size of a task.
// For example, a DM replication stream from one single upstream MySQL, or a
// changefeed that replicates one or more tables in TiCDC.
type TaskRescUnit interface {
	// GetType returns workload type of this resource unit
	GetType() model.WorkloadType

	// GetCapacity returns the required resource capacity of this unit
	GetCapacity() model.RescUnit

	// GetUsage returns current resource usage of this unit
	GetUsage() model.RescUnit
}

// SimpleTRU defines a simple task resource unit, which is only used in test
type SimpleTRU struct {
	wt model.WorkloadType
}

// n NewSimpleTRU returns a new SimpleTRU
func NewSimpleTRU(wt model.WorkloadType) *SimpleTRU {
	return &SimpleTRU{wt: wt}
}

// GetType implements TaskRescUnit.GetType
func (stru *SimpleTRU) GetType() model.WorkloadType {
	return stru.wt
}

// GetType implements TaskRescUnit.GetCapacity
func (stru *SimpleTRU) GetCapacity() model.RescUnit {
	return model.RescUnit(3)
}

// GetType implements TaskRescUnit.GetUsage
func (stru *SimpleTRU) GetUsage() model.RescUnit {
	return model.RescUnit(1 + rand.Intn(3))
}
