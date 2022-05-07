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
	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
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

func (p *MockCapacityProvider) CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus {
	return p.Capacities
}

func (p *MockCapacityProvider) CapacityForExecutor(executor model.ExecutorID) (*schedModel.ExecutorResourceStatus, bool) {
	status, ok := p.Capacities[executor]
	return status, ok
}
