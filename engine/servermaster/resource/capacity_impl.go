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
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// CapRescMgr implements ResourceMgr interface, and it uses node capacity as
// alloction algorithm
// TODO Unit tests for this struct.
type CapRescMgr struct {
	mu        sync.RWMutex
	r         *rand.Rand // random generator for choosing node
	executors map[model.ExecutorID]*ExecutorResource
}

// NewCapRescMgr creates a new CapRescMgr instance
func NewCapRescMgr() *CapRescMgr {
	return &CapRescMgr{
		r:         rand.New(rand.NewSource(time.Now().UnixNano())),
		executors: make(map[model.ExecutorID]*ExecutorResource),
	}
}

// Register implements RescMgr.Register
func (m *CapRescMgr) Register(id model.ExecutorID, addr string, capacity model.RescUnit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executors[id] = &ExecutorResource{
		ID:       id,
		Capacity: capacity,
		Addr:     addr,
	}
	log.L().Info("executor resource is registered",
		zap.String("executor-id", string(id)), zap.Int("capacity", int(capacity)))
}

// Unregister implements RescMgr.Unregister
func (m *CapRescMgr) Unregister(id model.ExecutorID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.executors, id)
	log.L().Info("executor resource is unregistered",
		zap.String("executor-id", string(id)))
}

// Update implements RescMgr.Update
func (m *CapRescMgr) Update(id model.ExecutorID, used, reserved model.RescUnit, status model.ExecutorStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exec, ok := m.executors[id]
	if !ok {
		return errors.ErrUnknownExecutorID.GenWithStackByArgs(id)
	}
	exec.Used = used
	exec.Reserved = reserved
	exec.Status = status
	return nil
}

// CapacitiesForAllExecutors implements scheduler.CapacityProvider.
// The returned value is a deep copy, so there is no risk of accidental sharing.
// Note the O(n) complexity.
func (m *CapRescMgr) CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ret := make(map[model.ExecutorID]*schedModel.ExecutorResourceStatus, len(m.executors))
	// Note the complexity O(n), where n = count(executors).
	// Currently this complexity does not seem to be a problem, because
	// scheduling happens only sporadically, and the number of executors
	// is limited to <= 100.
	for executorID, resc := range m.executors {
		resourceStatus := &schedModel.ExecutorResourceStatus{
			Capacity: resc.Capacity,
			Reserved: resc.Reserved,
			Used:     resc.Used,
		}
		ret[executorID] = resourceStatus
	}
	return ret
}

// CapacityForExecutor implements scheduler.CapacityProvider.
func (m *CapRescMgr) CapacityForExecutor(executor model.ExecutorID) (*schedModel.ExecutorResourceStatus, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	resc, exists := m.executors[executor]
	if !exists {
		return nil, false
	}

	return &schedModel.ExecutorResourceStatus{
		Capacity: resc.Capacity,
		Reserved: resc.Reserved,
		Used:     resc.Used,
	}, true
}
