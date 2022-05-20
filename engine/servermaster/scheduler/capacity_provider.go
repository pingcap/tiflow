package scheduler

import (
	"github.com/hanfei1991/microcosm/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

// CapacityProvider describes an object providing capacity info for
// each executor.
//
// NOTE: The current design of CapacityProvider is a naive one, without taking
// into consideration the effect of scheduling on the current capacities.
// But we are limited to a naive implementation until we finalize a comprehensive
// plan on computation resource reporting and a policy on overselling.
//
// TODO revise the design of this interface.
type CapacityProvider interface {
	// CapacitiesForAllExecutors returns a map of resource statuses for all
	// executors.
	CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus

	// CapacityForExecutor returns the resource status for a given executor.
	// If the executor is not found, (nil, false) is returned.
	CapacityForExecutor(executor model.ExecutorID) (*schedModel.ExecutorResourceStatus, bool)
}

// MockCapacityProvider mocks a CapacityProvider for unit tests.
type MockCapacityProvider struct {
	Capacities map[model.ExecutorID]*schedModel.ExecutorResourceStatus
}

// CapacitiesForAllExecutors implements CapacityProvider.CapacitiesForAllExecutors
func (p *MockCapacityProvider) CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus {
	return p.Capacities
}

// CapacityForExecutor implements CapacityProvider.CapacityForExecutor
func (p *MockCapacityProvider) CapacityForExecutor(executor model.ExecutorID) (*schedModel.ExecutorResourceStatus, bool) {
	status, ok := p.Capacities[executor]
	return status, ok
}
