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

	"github.com/pingcap/errors"
)

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

// SubChangeFeedInfo records the process information of a capture
type SubChangeFeedInfo struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	CheckPointTs uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	ResolvedTs uint64 `json:"resolved-ts"`
	// Table information list, containing tables that processor should process, updated by ownrer, processor is read only.
	TableInfos []*ProcessTableInfo `json:"table-infos"`
	TablePLock *TableLock          `json:"table-p-lock"`
	TableCLock *TableLock          `json:"table-c-lock"`
}

// Marshal returns the json marshal format of a SubChangeFeedInfo
func (scfi *SubChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(scfi)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *SubChangeFeedInfo from json marshal byte slice
func (scfi *SubChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, scfi)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

type CaptureID = string
type ChangeFeedID = string
type ProcessorsInfos = map[CaptureID]*SubChangeFeedInfo

type ChangeFeedStatus int

const (
	ChangeFeedUnknown ChangeFeedStatus = iota
	ChangeFeedSyncDML
	ChangeFeedWaitToExecDDL
	ChangeFeedExecDDL
	ChangeFeedDDLExecuteFailed
)

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
// partial fileds are stored in etcd, we may refine this later
type ChangeFeedInfo struct {
	Status       ChangeFeedStatus `json:"-"`
	SinkURI      string           `json:"sink-uri"`
	ResolvedTs   uint64           `json:"resolved-ts"`
	CheckpointTs uint64           `json:"checkpoint-ts"`
	TargetTs     uint64           `json:"-"`

	ProcessorInfos  ProcessorsInfos `json:"-"`
	DDLCurrentIndex int             `json:"-"`
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
