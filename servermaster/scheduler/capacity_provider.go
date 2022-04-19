package scheduler

import (
	"github.com/hanfei1991/microcosm/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

// CapacityProvider describes an object providing capacity info for
// each executor.
type CapacityProvider interface {
	CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus
	CapacityForExecutor(executor model.ExecutorID) *schedModel.ExecutorResourceStatus
}

// MockCapacityProvider mocks a CapacityProvider for unit tests.
type MockCapacityProvider struct {
	Capacities map[model.ExecutorID]*schedModel.ExecutorResourceStatus
}

func (p *MockCapacityProvider) CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus {
	return p.Capacities
}

func (p *MockCapacityProvider) CapacityForExecutor(executor model.ExecutorID) *schedModel.ExecutorResourceStatus {
	return p.Capacities[executor]
}
