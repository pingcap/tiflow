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

// TableLockStatus for the table lock in ProcessorInfo
type TableLockStatus int

// Table lock status
const (
	TableNoLock TableLockStatus = iota + 1
	TablePLock
	TablePLockCommited
)

// ProcessorInfo records the process information of a capture
type ProcessorInfo struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	CheckPointTs uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	ResolvedTs uint64 `json:"resolved-ts"`
	// Table information list, containing tables that processor should process, updated by ownrer, processor is read only.
	// TODO change to be a map for easy update.
	TableInfos  []*ProcessTableInfo `json:"table-infos"`
	TablePLock  *TableLock          `json:"table-p-lock"`
	TableCLock  *TableLock          `json:"table-c-lock"`
	ModRevision int64               `json:"-"`
}

// String implements fmt.Stringer interface.
func (pi *ProcessorInfo) String() string {
	data, _ := pi.Marshal()
	return string(data)
}

// RemoveTable remove the table in TableInfos.
func (pi *ProcessorInfo) RemoveTable(id uint64) (*ProcessTableInfo, bool) {
	for idx, table := range pi.TableInfos {
		if table.ID == id {
			last := pi.TableInfos[len(pi.TableInfos)-1]
			removedTable := pi.TableInfos[idx]

			pi.TableInfos[idx] = last
			pi.TableInfos = pi.TableInfos[:len(pi.TableInfos)-1]

			return removedTable, true
		}
	}

	return nil, false
}

// Snapshot takes a snapshot of `*ProcessorInfo` and returns a new `*ProcInfoSnap`
func (pi *ProcessorInfo) Snapshot(cfID ChangeFeedID, captureID CaptureID) *ProcInfoSnap {
	snap := &ProcInfoSnap{
		CfID:      cfID,
		CaptureID: captureID,
		Tables:    make([]ProcessTableInfo, 0, len(pi.TableInfos)),
	}
	for _, tbl := range pi.TableInfos {
		ts := pi.CheckPointTs
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

// Marshal returns the json marshal format of a ProcessorInfo
func (pi *ProcessorInfo) Marshal() (string, error) {
	data, err := json.Marshal(pi)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *ProcessorInfo from json marshal byte slice
func (pi *ProcessorInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, pi)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

// Clone returns a deep-clone of the struct
func (pi *ProcessorInfo) Clone() *ProcessorInfo {
	clone := *pi
	infos := make([]*ProcessTableInfo, 0, len(pi.TableInfos))
	for _, ti := range pi.TableInfos {
		c := *ti
		infos = append(infos, &c)
	}
	clone.TableInfos = infos
	if pi.TablePLock != nil {
		pLock := *pi.TablePLock
		clone.TablePLock = &pLock
	}
	if pi.TableCLock != nil {
		cLock := *pi.TableCLock
		clone.TableCLock = &cLock
	}
	return &clone
}

// CaptureID is the type for capture ID
type CaptureID = string

// ChangeFeedID is the type for change feed ID
type ChangeFeedID = string

// ProcessorsInfos maps from capture IDs to ProcessorInfo
type ProcessorsInfos map[CaptureID]*ProcessorInfo

// ChangeFeedStatus is the type for change feed status
type ChangeFeedStatus int

const (
	// ChangeFeedUnknown stands for all unknown status
	ChangeFeedUnknown ChangeFeedStatus = iota
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
func (s ChangeFeedStatus) String() string {
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

// ChangeFeedInfo stores information about a ChangeFeed
type ChangeFeedInfo struct {
	SinkURI      string `json:"sink-uri"`
	ResolvedTs   uint64 `json:"resolved-ts"`
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

// Marshal returns json encoded string of ChangeFeedInfo, only contains necessary fields stored in storage
func (info *ChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(info)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, info)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

// ProcInfoSnap holds most important replication information of a processor
type ProcInfoSnap struct {
	CfID      string
	CaptureID string
	Tables    []ProcessTableInfo
}
