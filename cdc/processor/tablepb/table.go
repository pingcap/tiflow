// Copyright 2020 PingCAP, Inc.
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

package tablepb

import (
	"sync/atomic"

	"github.com/pingcap/tiflow/cdc/model"
)

// Load TableState with THREAD-SAFE
func (s *TableState) Load() TableState {
	return TableState(atomic.LoadInt32((*int32)(s)))
}

// Store TableState with THREAD-SAFE
func (s *TableState) Store(new TableState) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

// TablePipeline is a pipeline which capture the change log from tikv in a table
type TablePipeline interface {
	// ID returns the ID of source table and mark table
	ID() (tableID int64)
	// Name returns the quoted schema and table name
	Name() string
	// ResolvedTs returns the resolved ts in this table pipeline
	ResolvedTs() model.Ts
	// CheckpointTs returns the checkpoint ts in this table pipeline
	CheckpointTs() model.Ts
	// UpdateBarrierTs updates the barrier ts in this table pipeline
	UpdateBarrierTs(ts model.Ts)
	// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
	AsyncStop() bool

	// Start the sink consume data from the given `ts`
	Start(ts model.Ts)

	// Stats returns statistic for a table.
	Stats() Stats
	// State returns the state of this table pipeline
	State() TableState
	// Cancel stops this table pipeline immediately and destroy all resources created by this table pipeline
	Cancel()
	// Wait waits for table pipeline destroyed
	Wait()
	// MemoryConsumption return the memory consumption in bytes
	MemoryConsumption() uint64

	// RemainEvents return the amount of kv events remain in sorter.
	RemainEvents() int64
}
