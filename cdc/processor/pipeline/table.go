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
	"time"

	"github.com/pingcap/tiflow/cdc/model"
)

const (
	// TODO determine a reasonable default value
	// This is part of sink performance optimization
	resolvedTsInterpolateInterval = 200 * time.Millisecond
)

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
	// Workload returns the workload of this table
	Workload() model.WorkloadInfo
	// Status returns the status of this table pipeline
	Status() TableStatus
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
