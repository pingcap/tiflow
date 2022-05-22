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

package runtime

import (
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

// TaskStatus defines an interface to manage common fields of a task
type TaskStatus interface {
	GetUnit() libModel.WorkerType
	GetTask() string
	GetStage() metadata.TaskStage
}

// DefaultTaskStatus implements TaskStatus interface
type DefaultTaskStatus struct {
	Unit  libModel.WorkerType
	Task  string
	Stage metadata.TaskStage
}

// NewTaskStatus create a task status by unit.
func NewTaskStatus(unit libModel.WorkerType, task string, stage metadata.TaskStage) TaskStatus {
	var taskStatus TaskStatus
	defaultTaskStatus := DefaultTaskStatus{
		Unit:  unit,
		Task:  task,
		Stage: stage,
	}
	switch unit {
	case lib.WorkerDMDump:
		taskStatus = &DumpStatus{DefaultTaskStatus: defaultTaskStatus}
	case lib.WorkerDMLoad:
		taskStatus = &LoadStatus{DefaultTaskStatus: defaultTaskStatus}
	case lib.WorkerDMSync:
		taskStatus = &SyncStatus{DefaultTaskStatus: defaultTaskStatus}
	case 0:
		taskStatus = &defaultTaskStatus
	}
	return taskStatus
}

// NewOfflineStatus is used when jobmaster receives a worker offline.
// No need to serialize.
func NewOfflineStatus(taskID string) TaskStatus {
	return NewTaskStatus(0, taskID, metadata.StageUnscheduled)
}

// GetUnit implements TaskStatus.GetUnit
func (s *DefaultTaskStatus) GetUnit() libModel.WorkerType {
	return s.Unit
}

// GetTask implements TaskStatus.GetTask
func (s *DefaultTaskStatus) GetTask() string {
	return s.Task
}

// GetStage implements TaskStatus.GetStage
func (s *DefaultTaskStatus) GetStage() metadata.TaskStage {
	return s.Stage
}

// DumpStatus records necessary information of a dump unit
type DumpStatus struct {
	DefaultTaskStatus
	// copy from tiflow/dm/dm/proto/dmworker.proto:DumpStatus
	TotalTables       int64
	CompletedTables   float64
	FinishedBytes     float64
	FinishedRows      float64
	EstimateTotalRows float64
}

// LoadStatus records necessary information of a load unit
type LoadStatus struct {
	DefaultTaskStatus
	// copy from tiflow/dm/dm/proto/dmworker.proto:LoadStatus
	FinishedBytes  int64
	TotalBytes     int64
	Progress       string
	MetaBinlog     string
	MetaBinlogGTID string
}

// SyncStatus records necessary information of a sync unit
type SyncStatus struct {
	DefaultTaskStatus
	// copy from tiflow/dm/dm/proto/dmworker.proto:SyncStatus
	TotalEvents      int64
	TotalTps         int64
	RecentTps        int64
	MasterBinlog     string
	MasterBinlogGtid string
	SyncerBinlog     string
	SyncerBinlogGtid string
	BlockingDDLs     []string
	// TODO: add sharding group
	// ShardingGroup unresolvedGroups = 9; // sharding groups which current are un-resolved
	Synced              bool
	BinlogType          string
	SecondsBehindMaster int64
	BlockDDLOwner       string
	ConflictMsg         string
}

// UnmarshalTaskStatus unmarshal a task status.
func UnmarshalTaskStatus(data []byte) (TaskStatus, error) {
	var typ struct {
		Unit libModel.WorkerType
	}
	if err := json.Unmarshal(data, &typ); err != nil {
		return nil, errors.Trace(err)
	}
	switch typ.Unit {
	case lib.WorkerDMDump, lib.WorkerDMLoad, lib.WorkerDMSync:
	default:
		return nil, errors.Errorf("unknown unit: %d", typ.Unit)
	}

	taskStatus := NewTaskStatus(typ.Unit, "", 0)
	err := json.Unmarshal(data, taskStatus)
	return taskStatus, errors.Trace(err)
}

// MarshalTaskStatus returns the JSON encoding of task status.
func MarshalTaskStatus(taskStatus TaskStatus) ([]byte, error) {
	return json.Marshal(taskStatus)
}
