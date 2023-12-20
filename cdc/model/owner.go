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

package model

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// AdminJobType represents for admin job type, both used in owner and processor
type AdminJobType int

// AdminJob holds an admin job
type AdminJob struct {
	CfID                  ChangeFeedID
	Type                  AdminJobType
	Error                 *RunningError
	OverwriteCheckpointTs uint64
}

// All AdminJob types
const (
	AdminNone AdminJobType = iota
	AdminStop
	AdminResume
	AdminRemove
	AdminFinish
)

// String implements fmt.Stringer interface.
func (t AdminJobType) String() string {
	switch t {
	case AdminNone:
		return "noop"
	case AdminStop:
		return "stop changefeed"
	case AdminResume:
		return "resume changefeed"
	case AdminRemove:
		return "remove changefeed"
	case AdminFinish:
		return "finish changefeed"
	}
	return "unknown"
}

// IsStopState returns whether changefeed is in stop state with give admin job
func (t AdminJobType) IsStopState() bool {
	switch t {
	case AdminStop, AdminRemove, AdminFinish:
		return true
	}
	return false
}

// DDLJobEntry is the DDL job entry.
type DDLJobEntry struct {
	Job    *timodel.Job
	OpType OpType
	CRTs   uint64
}

// TaskPosition records the process information of a capture
type TaskPosition struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	//
	// Deprecated: only used in API. TODO: remove API usage.
	CheckPointTs uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	//
	// Deprecated: only used in API. TODO: remove API usage.
	ResolvedTs uint64 `json:"resolved-ts"`
	// The count of events were synchronized. This is updated by corresponding processor.
	//
	// Deprecated: only used in API. TODO: remove API usage.
	Count uint64 `json:"count"`

	// Error when changefeed error happens
	Error *RunningError `json:"error"`
	// Warning when module error happens
	Warning *RunningError `json:"warning"`
}

// Marshal returns the json marshal format of a TaskStatus
func (tp *TaskPosition) Marshal() (string, error) {
	data, err := json.Marshal(tp)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (tp *TaskPosition) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, tp)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// String implements fmt.Stringer interface.
func (tp *TaskPosition) String() string {
	data, _ := tp.Marshal()
	return data
}

// Clone returns a deep clone of TaskPosition
func (tp *TaskPosition) Clone() *TaskPosition {
	ret := &TaskPosition{
		CheckPointTs: tp.CheckPointTs,
		ResolvedTs:   tp.ResolvedTs,
		Count:        tp.Count,
	}
	if tp.Error != nil {
		ret.Error = &RunningError{
			Time:    tp.Error.Time,
			Addr:    tp.Error.Addr,
			Code:    tp.Error.Code,
			Message: tp.Error.Message,
		}
	}
	if tp.Warning != nil {
		ret.Warning = &RunningError{
			Time:    tp.Warning.Time,
			Addr:    tp.Warning.Addr,
			Code:    tp.Warning.Code,
			Message: tp.Warning.Message,
		}
	}
	return ret
}

// All TableOperation flags
const (
	// Move means after the delete operation, the table will be re added.
	// This field is necessary since we must persist enough information to
	// restore complete table operation in case of processor or owner crashes.
	OperFlagMoveTable uint64 = 1 << iota
)

// All TableOperation status
const (
	OperDispatched uint64 = iota
	OperProcessed
	OperFinished
)

// TableOperation records the current information of a table migration
type TableOperation struct {
	Delete bool   `json:"delete"`
	Flag   uint64 `json:"flag,omitempty"`
	// if the operation is a delete operation, BoundaryTs is checkpoint ts
	// if the operation is an add operation, BoundaryTs is start ts
	BoundaryTs uint64 `json:"boundary_ts"`
	Status     uint64 `json:"status,omitempty"`
}

// TableProcessed returns whether the table has been processed by processor
func (o *TableOperation) TableProcessed() bool {
	return o.Status == OperProcessed || o.Status == OperFinished
}

// TableApplied returns whether the table has finished the startup procedure.
// Returns true if table has been processed by processor and resolved ts reaches global resolved ts.
func (o *TableOperation) TableApplied() bool {
	return o.Status == OperFinished
}

// Clone returns a deep-clone of the struct
func (o *TableOperation) Clone() *TableOperation {
	if o == nil {
		return nil
	}
	clone := *o
	return &clone
}

// TableReplicaInfo records the table replica info
type TableReplicaInfo struct {
	StartTs Ts `json:"start-ts"`
}

// Clone clones a TableReplicaInfo
func (i *TableReplicaInfo) Clone() *TableReplicaInfo {
	if i == nil {
		return nil
	}
	clone := *i
	return &clone
}

// TaskStatus records the task information of a capture.
//
// Deprecated: only used in API. TODO: remove API usage.
type TaskStatus struct {
	Tables       map[TableID]*TableReplicaInfo `json:"tables"`
	Operation    map[TableID]*TableOperation   `json:"operation"`
	AdminJobType AdminJobType                  `json:"admin-job-type"`
	ModRevision  int64                         `json:"-"`
}

// String implements fmt.Stringer interface.
func (ts *TaskStatus) String() string {
	data, _ := ts.Marshal()
	return data
}

// Marshal returns the json marshal format of a TaskStatus
func (ts *TaskStatus) Marshal() (string, error) {
	data, err := json.Marshal(ts)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (ts *TaskStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, ts)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// Clone returns a deep-clone of the struct
func (ts *TaskStatus) Clone() *TaskStatus {
	clone := *ts
	tables := make(map[TableID]*TableReplicaInfo, len(ts.Tables))
	for tableID, table := range ts.Tables {
		tables[tableID] = table.Clone()
	}
	clone.Tables = tables
	operation := make(map[TableID]*TableOperation, len(ts.Operation))
	for tableID, opt := range ts.Operation {
		operation[tableID] = opt.Clone()
	}
	clone.Operation = operation
	return &clone
}

// TableID is the ID of the table
type TableID = tablepb.TableID

// Ts is the timestamp with a logical count
type Ts = tablepb.Ts

// ProcessorsInfos maps from capture IDs to TaskStatus
type ProcessorsInfos map[CaptureID]*TaskStatus

// String implements fmt.Stringer interface.
func (p ProcessorsInfos) String() string {
	s := "{"
	for id, sinfo := range p {
		s += fmt.Sprintf("%s: %+v,", id, *sinfo)
	}

	s += "}"

	return s
}

// ChangeFeedStatus stores information about a ChangeFeed
// It is stored in etcd.
type ChangeFeedStatus struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
	// minTableBarrierTs is the minimum commitTs of all DDL events and is only
	// used to check whether there is a pending DDL job at the checkpointTs when
	// initializing the changefeed.
	MinTableBarrierTs uint64 `json:"min-table-barrier-ts"`
	// TODO: remove this filed after we don't use ChangeFeedStatus to
	// control processor. This is too ambiguous.
	AdminJobType AdminJobType `json:"admin-job-type"`
}

// Marshal returns json encoded string of ChangeFeedStatus, only contains necessary fields stored in storage
func (status *ChangeFeedStatus) Marshal() (string, error) {
	data, err := json.Marshal(status)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *ChangeFeedStatus from json marshal byte slice
func (status *ChangeFeedStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, status)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// ProcInfoSnap holds most important replication information of a processor
type ProcInfoSnap struct {
	CfID      ChangeFeedID `json:"changefeed-id"`
	CaptureID string       `json:"capture-id"`
}
