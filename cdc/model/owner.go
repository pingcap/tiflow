// Copyright 2019 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/errors"
)

// AdminJobType represents for admin job type, both used in owner and processor
type AdminJobType int

// AdminJob holds an admin job
type AdminJob struct {
	CfID string
	Type AdminJobType
}

// All AdminJob types
const (
	AdminNone AdminJobType = iota
	AdminStop
	AdminResume
	AdminRemove
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
	}
	return "unknown"
}

// TaskPosition records the process information of a capture
type TaskPosition struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	CheckPointTs uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	ResolvedTs uint64 `json:"resolved-ts"`
	// The count of events were synchronized. This is updated by corresponding processor.
	Count uint64 `json:"count"`
}

// Marshal returns the json marshal format of a TaskStatus
func (tp *TaskPosition) Marshal() (string, error) {
	data, err := json.Marshal(tp)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (tp *TaskPosition) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, tp)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

// String implements fmt.Stringer interface.
func (tp *TaskPosition) String() string {
	data, _ := tp.Marshal()
	return data
}

// TableOperation records the current information of a table migration
type TableOperation struct {
	TableID TableID `json:"table_id"`
	Delete  bool    `json:"delete"`
	// if the operation is a delete operation, BoundaryTs is checkpoint ts
	// if the operation is a add operation, BoundaryTs is start ts
	BoundaryTs uint64 `json:"boundary_ts"`
	Done       bool   `json:"done"`
}

// Clone returns a deep-clone of the struct
func (o *TableOperation) Clone() *TableOperation {
	if o == nil {
		return nil
	}
	clone := *o
	return &clone
}

// TaskStatus records the task information of a capture
type TaskStatus struct {
	// Table information list, containing tables that processor should process, updated by ownrer, processor is read only.
	Tables       map[TableID]Ts    `json:"tables"`
	Operation    []*TableOperation `json:"operation"`
	AdminJobType AdminJobType      `json:"admin-job-type"`
	ModRevision  int64             `json:"-"`
}

// String implements fmt.Stringer interface.
func (ts *TaskStatus) String() string {
	data, _ := ts.Marshal()
	return data
}

// RemoveTable remove the table in TableInfos.
func (ts *TaskStatus) RemoveTable(id TableID) (Ts, bool) {
	if startTs, exist := ts.Tables[id]; exist {
		delete(ts.Tables, id)
		return startTs, true
	}
	return 0, false
}

// SomeOperationsUnapplied returns true if there are some operations not applied
func (ts *TaskStatus) SomeOperationsUnapplied() bool {
	for _, o := range ts.Operation {
		if !o.Done {
			return true
		}
	}
	return false
}

// AppliedTs returns a Ts which less or equal to the ts boundary of any unapplied operation
func (ts *TaskStatus) AppliedTs() Ts {
	appliedTs := uint64(math.MaxUint64)
	for _, o := range ts.Operation {
		if !o.Done {
			if appliedTs > o.BoundaryTs {
				appliedTs = o.BoundaryTs
			}
		}
	}
	return appliedTs
}

// Snapshot takes a snapshot of `*TaskStatus` and returns a new `*ProcInfoSnap`
func (ts *TaskStatus) Snapshot(cfID ChangeFeedID, captureID CaptureID, checkpointTs Ts) *ProcInfoSnap {
	snap := &ProcInfoSnap{
		CfID:      cfID,
		CaptureID: captureID,
		Tables:    make(map[TableID]Ts, len(ts.Tables)),
	}
	for tableID, startTs := range ts.Tables {
		ts := checkpointTs
		if ts < startTs {
			ts = startTs
		}
		snap.Tables[tableID] = ts
	}
	return snap
}

// Marshal returns the json marshal format of a TaskStatus
func (ts *TaskStatus) Marshal() (string, error) {
	data, err := json.Marshal(ts)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (ts *TaskStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, ts)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

// Clone returns a deep-clone of the struct
func (ts *TaskStatus) Clone() *TaskStatus {
	clone := *ts
	tables := make(map[TableID]Ts, len(ts.Tables))
	for tableID, startTs := range ts.Tables {
		tables[tableID] = startTs
	}
	clone.Tables = tables
	operation := make([]*TableOperation, 0, len(ts.Operation))
	for _, opt := range ts.Operation {
		operation = append(operation, opt.Clone())
	}
	clone.Operation = operation
	return &clone
}

// CaptureID is the type for capture ID
type CaptureID = string

// ChangeFeedID is the type for change feed ID
type ChangeFeedID = string

// TableID is the ID of the table
type TableID = int64

// SchemaID is the ID of the schema
type SchemaID = int64

// Ts is the timestamp with a logical count
type Ts = uint64

// ProcessorsInfos maps from capture IDs to TaskStatus
type ProcessorsInfos map[CaptureID]*TaskStatus

// ChangeFeedDDLState is the type for change feed status
type ChangeFeedDDLState int

const (
	// ChangeFeedUnknown stands for all unknown status
	ChangeFeedUnknown ChangeFeedDDLState = iota
	// ChangeFeedSyncDML means DMLs are being processed
	ChangeFeedSyncDML
	// ChangeFeedWaitToExecDDL means we are waiting to execute a DDL
	ChangeFeedWaitToExecDDL
	// ChangeFeedExecDDL means a DDL is being executed
	ChangeFeedExecDDL
	// ChangeFeedDDLExecuteFailed means that an error occurred when executing a DDL
	ChangeFeedDDLExecuteFailed
)

// String implements fmt.Stringer interface.
func (p ProcessorsInfos) String() string {
	s := "{"
	for id, sinfo := range p {
		s += fmt.Sprintf("%s: %+v,", id, *sinfo)
	}

	s += "}"

	return s
}

// String implements fmt.Stringer interface.
func (s ChangeFeedDDLState) String() string {
	switch s {
	case ChangeFeedSyncDML:
		return "SyncDML"
	case ChangeFeedWaitToExecDDL:
		return "WaitToExecDDL"
	case ChangeFeedExecDDL:
		return "ExecDDL"
	case ChangeFeedDDLExecuteFailed:
		return "DDLExecuteFailed"
	}
	return "Unknown"
}

// ChangeFeedStatus stores information about a ChangeFeed
type ChangeFeedStatus struct {
	ResolvedTs   uint64       `json:"resolved-ts"`
	CheckpointTs uint64       `json:"checkpoint-ts"`
	AdminJobType AdminJobType `json:"admin-job-type"`
}

// Marshal returns json encoded string of ChangeFeedStatus, only contains necessary fields stored in storage
func (status *ChangeFeedStatus) Marshal() (string, error) {
	data, err := json.Marshal(status)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *ChangeFeedStatus from json marshal byte slice
func (status *ChangeFeedStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, status)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

// ProcInfoSnap holds most important replication information of a processor
type ProcInfoSnap struct {
	CfID      string         `json:"changefeed-id"`
	CaptureID string         `json:"capture-id"`
	Tables    map[TableID]Ts `json:"-"`
}
