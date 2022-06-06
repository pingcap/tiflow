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

package pipeline

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
)

const (
	// TODO determine a reasonable default value
	// This is part of sink performance optimization
	resolvedTsInterpolateInterval = 200 * time.Millisecond
)

// TableState is state of the table pipeline
type TableState int32

// TableState for table pipeline
const (
	// TableStatePreparing indicate that the table is preparing connecting to regions
	TableStatePreparing TableState = iota
	// TableStatePrepared means the first `Resolved Ts` is received.
	TableStatePrepared
	// TableStateReplicating means that sink is consuming data from the sorter, and replicating it to downstream
	TableStateReplicating
	// TableStateStopping means the table is stopping, but not guaranteed yet.
	TableStateStopping
	// TableStateStopped means sink stop all works.
	TableStateStopped
	// TableStateAbsent means the table not found
	TableStateAbsent
)

var tableStatusStringMap = map[TableState]string{
	TableStatePreparing:   "Preparing",
	TableStatePrepared:    "Prepared",
	TableStateReplicating: "Replicating",
	TableStateStopping:    "Stopping",
	TableStateStopped:     "Stopped",
	TableStateAbsent:      "Absent",
}

func (s TableState) String() string {
	return tableStatusStringMap[s]
}

// Load TableState with THREAD-SAFE
func (s *TableState) Load() TableState {
	return TableState(atomic.LoadInt32((*int32)(s)))
}

// Store TableState with THREAD-SAFE
func (s *TableState) Store(new TableState) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

// TableMeta is the metadata of a table.
type TableMeta struct {
	TableID      model.TableID
	CheckpointTs model.Ts
	ResolvedTs   model.Ts
	State        TableState
}

// TablePipeline is a pipeline which capture the change log from tikv in a table
type TablePipeline interface {
	// ID returns the ID of source table and mark table
	ID() (tableID, markTableID int64)
	// Name returns the quoted schema and table name
	Name() string
	// ResolvedTs returns the resolved ts in this table pipeline
	ResolvedTs() model.Ts
	// CheckpointTs returns the checkpoint ts in this table pipeline
	CheckpointTs() model.Ts
	// UpdateBarrierTs updates the barrier ts in this table pipeline
	UpdateBarrierTs(ts model.Ts)
	// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
	AsyncStop(targetTs model.Ts) bool

	// Start the sink consume data from the given `ts`
	Start(ts model.Ts) bool

	// Workload returns the workload of this table
	Workload() model.WorkloadInfo
	// State returns the state of this table pipeline
	State() TableState
	// Cancel stops this table pipeline immediately and destroy all resources created by this table pipeline
	Cancel()
	// Wait waits for table pipeline destroyed
	Wait()
	// MemoryConsumption return the memory consumption in bytes
	MemoryConsumption() uint64
}

// TODO find a better name or avoid using an interface
// We use an interface here for ease in unit testing.
type tableFlowController interface {
	Consume(msg *model.PolymorphicEvent, size uint64, blockCallBack func(batch bool) error) error
	Release(resolvedTs uint64)
	Abort()
	GetConsumption() uint64
}

var workload = model.WorkloadInfo{Workload: 1}

// Assume 1KB per row in upstream TiDB, it takes about 250 MB (1024*4*64) for
// replicating 1024 tables in the worst case.
const defaultOutputChannelSize = 64
