// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"github.com/pingcap/errors"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON use to specify the time format
func (t *JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(*t).Format("2006-01-02 15:04:05.000"))
	return []byte(stamp), nil
}

func (t *JSONTime) UnmarshalJSON(data []byte) error {
	// We need to trim the quotation marks before passing the data to `time.Parse`.
	data = bytes.TrimLeft(data, "\"")
	data = bytes.TrimRight(data, "\"")
	time, err := time.Parse("2006-01-02 15:04:05.000", string(data))
	if err != nil {
		return errors.Trace(err)
	}
	*t = JSONTime(time)
	return nil
}

// HTTPError of cdc http api
type HTTPError struct {
	Error string `json:"error_msg"`
	Code  string `json:"error_code"`
}

// NewHTTPError wrap a err into HTTPError
func NewHTTPError(err error) HTTPError {
	errCode, _ := cerror.RFCCode(err)
	return HTTPError{
		Error: err.Error(),
		Code:  string(errCode),
	}
}

// ServerStatus holds some common information of a server
type ServerStatus struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

// ChangefeedCommonInfo holds some common usage information of a changefeed
type ChangefeedCommonInfo struct {
	ID             string        `json:"id"`
	FeedState      FeedState     `json:"state"`
	CheckpointTSO  uint64        `json:"checkpoint_tso"`
	CheckpointTime JSONTime      `json:"checkpoint_time"`
	RunningError   *RunningError `json:"error"`
}

// ChangefeedDetail holds detail info of a changefeed
type ChangefeedDetail struct {
	ID             string              `json:"id"`
	SinkURI        string              `json:"sink_uri"`
	CreateTime     JSONTime            `json:"create_time"`
	StartTs        uint64              `json:"start_ts"`
	TargetTs       uint64              `json:"target_ts"`
	CheckpointTSO  uint64              `json:"checkpoint_tso"`
	CheckpointTime JSONTime            `json:"checkpoint_time"`
	Engine         SortEngine          `json:"sort_engine"`
	FeedState      FeedState           `json:"state"`
	RunningError   *RunningError       `json:"error"`
	ErrorHis       []int64             `json:"error_history"`
	CreatorVersion string              `json:"creator_version"`
	TaskStatus     []CaptureTaskStatus `json:"task_status"`
}

// ChangefeedConfig use to create a changefeed
type ChangefeedConfig struct {
	ID       string `json:"changefeed_id"`
	StartTS  uint64 `json:"start_ts"`
	TargetTS uint64 `json:"target_ts"`
	SinkURI  string `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone string `json:"timezone" default:"system"`
	// if true, force to replicate some ineligible tables
	ForceReplicate        bool               `json:"force_replicate" default:"false"`
	IgnoreIneligibleTable bool               `json:"ignore_ineligible_table" default:"false"`
	FilterRules           []string           `json:"filter_rules"`
	IgnoreTxnStartTs      []uint64           `json:"ignore_txn_start_ts"`
	MounterWorkerNum      int                `json:"mounter_worker_num" default:"16"`
	SinkConfig            *config.SinkConfig `json:"sink_config"`
}

// ProcessorCommonInfo holds the common info of a processor
type ProcessorCommonInfo struct {
	CfID      string `json:"changefeed_id"`
	CaptureID string `json:"capture_id"`
}

// ProcessorDetail holds the detail info of a processor
type ProcessorDetail struct {
	// The maximum event CommitTs that has been synchronized.
	CheckPointTs uint64 `json:"checkpoint_ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized.
	ResolvedTs uint64 `json:"resolved_ts"`
	// all table ids that this processor are replicating
	Tables []int64 `json:"table_ids"`
	// Error code when error happens
	Error *RunningError `json:"error"`
}

// CaptureTaskStatus holds TaskStatus of a capture
type CaptureTaskStatus struct {
	CaptureID string `json:"capture_id"`
	// Table list, containing tables that processor should process
	Tables    []int64                     `json:"table_ids"`
	Operation map[TableID]*TableOperation `json:"table_operations"`
}

func (s *CaptureTaskStatus) ToCoreTaskStatus() *TaskStatus {
	ret := &TaskStatus{
		Tables:    map[TableID]*TableReplicaInfo{},
		Operation: map[TableID]*TableOperation{},
		// AdminJobType cannot be filled in here due to lack of information
		AdminJobType: 0,
	}

	for _, tableID := range s.Tables {
		ret.Tables[tableID] = &TableReplicaInfo{
			StartTs:     0,
			MarkTableID: 0,
		}
	}
	for tableID, op := range s.Operation {
		ret.Operation[tableID] = &TableOperation{
			Delete:     op.Delete,
			Flag:       op.Flag,
			BoundaryTs: op.BoundaryTs,
			Status:     op.Status,
		}
	}
	return ret
}

// Capture holds common information of a capture in cdc
type Capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is_owner"`
	AdvertiseAddr string `json:"address"`
}
