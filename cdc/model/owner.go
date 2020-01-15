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

	"github.com/pingcap/errors"
)

// ProcessTableInfo contains the info about tables that processor need to process.
type ProcessTableInfo struct {
	ID      uint64 `json:"id"`
	StartTs uint64 `json:"start-ts"`
}

// TableLock is used when applying table re-assignment to a processor.
// There are two kinds of locks, P-lock and C-lock. P-lock is set by owner when
// owner removes one or more tables from one processor. C-lock is a pair to
// P-lock and set by processor to indicate that the processor has synchronized
// the checkpoint and won't synchronize the removed table any more.
type TableLock struct {
	// Ts is the create timestamp of lock, it is used to pair P-lock and C-lock
	Ts uint64 `json:"ts"`
	// CreatorID is the lock creator ID
	CreatorID string `json:"creator-id"`
	// CheckpointTs is used in C-lock only, it records the table synchronization checkpoint
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

// TableLockStatus for the table lock in TaskStatus
type TableLockStatus int

// Table lock status
const (
	TableNoLock TableLockStatus = iota + 1
	TablePLock
	TablePLockCommited
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

// TaskStatus records the process information of a capture
type TaskStatus struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	CheckPointTs uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	ResolvedTs uint64 `json:"resolved-ts"`
	// Table information list, containing tables that processor should process, updated by ownrer, processor is read only.
	// TODO change to be a map for easy update.
	TableInfos   []*ProcessTableInfo `json:"table-infos"`
	TablePLock   *TableLock          `json:"table-p-lock"`
	TableCLock   *TableLock          `json:"table-c-lock"`
	AdminJobType AdminJobType        `json:"admin-job-type"`
	ModRevision  int64               `json:"-"`
}

// String implements fmt.Stringer interface.
func (ts *TaskStatus) String() string {
	data, _ := ts.Marshal()
	return string(data)
}

// RemoveTable remove the table in TableInfos.
func (ts *TaskStatus) RemoveTable(id uint64) (*ProcessTableInfo, bool) {
	for idx, table := range ts.TableInfos {
		if table.ID == id {
			last := ts.TableInfos[len(ts.TableInfos)-1]
			removedTable := ts.TableInfos[idx]

			ts.TableInfos[idx] = last
			ts.TableInfos = ts.TableInfos[:len(ts.TableInfos)-1]

			return removedTable, true
		}
	}

	return nil, false
}

// Snapshot takes a snapshot of `*TaskStatus` and returns a new `*ProcInfoSnap`
func (ts *TaskStatus) Snapshot(cfID ChangeFeedID, captureID CaptureID) *ProcInfoSnap {
	snap := &ProcInfoSnap{
		CfID:      cfID,
		CaptureID: captureID,
		Tables:    make([]ProcessTableInfo, 0, len(ts.TableInfos)),
	}
	for _, tbl := range ts.TableInfos {
		ts := ts.CheckPointTs
		if ts < tbl.StartTs {
			ts = tbl.StartTs
		}
		snap.Tables = append(snap.Tables, ProcessTableInfo{
			ID:      tbl.ID,
			StartTs: ts,
		})
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
	infos := make([]*ProcessTableInfo, 0, len(ts.TableInfos))
	for _, ti := range ts.TableInfos {
		c := *ti
		infos = append(infos, &c)
	}
	clone.TableInfos = infos
	if ts.TablePLock != nil {
		pLock := *ts.TablePLock
		clone.TablePLock = &pLock
	}
	if ts.TableCLock != nil {
		cLock := *ts.TableCLock
		clone.TableCLock = &cLock
	}
	return &clone
}

// CaptureID is the type for capture ID
type CaptureID = string

// ChangeFeedID is the type for change feed ID
type ChangeFeedID = string

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
	CfID      string
	CaptureID string
	Tables    []ProcessTableInfo
}
