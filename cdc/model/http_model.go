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
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const timeFormat = `"2006-01-02 15:04:05.000"`

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON used to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := time.Time(t).Format(timeFormat)
	return []byte(stamp), nil
}

// UnmarshalJSON is used to parse time.Time from bytes. The builtin json.Unmarshal function cannot unmarshal
// a date string formatted as "2006-01-02 15:04:05.000", so we must implement a customized unmarshal function.
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	tm, err := time.Parse(timeFormat, string(data))
	if err != nil {
		return err
	}

	*t = JSONTime(tm)
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

// Liveness is the liveness status of a capture.
// Liveness can only be changed from alive to stopping, and no way back.
type Liveness int32

const (
	// LivenessCaptureAlive means the capture is alive, and ready to serve.
	LivenessCaptureAlive Liveness = 0
	// LivenessCaptureStopping means the capture is in the process of graceful shutdown.
	LivenessCaptureStopping Liveness = 1
)

// Store the given liveness. Returns true if it success.
func (l *Liveness) Store(v Liveness) bool {
	return atomic.CompareAndSwapInt32(
		(*int32)(l), int32(LivenessCaptureAlive), int32(v))
}

// Load the liveness.
func (l *Liveness) Load() Liveness {
	return Liveness(atomic.LoadInt32((*int32)(l)))
}

func (l *Liveness) String() string {
	switch *l {
	case LivenessCaptureAlive:
		return "Alive"
	case LivenessCaptureStopping:
		return "Stopping"
	default:
		return "unknown"
	}
}

// ServerStatus holds some common information of a server
type ServerStatus struct {
	Version   string   `json:"version"`
	GitHash   string   `json:"git_hash"`
	ID        string   `json:"id"`
	ClusterID string   `json:"cluster_id"`
	Pid       int      `json:"pid"`
	IsOwner   bool     `json:"is_owner"`
	Liveness  Liveness `json:"liveness"`
}

// ChangefeedCommonInfo holds some common usage information of a changefeed
type ChangefeedCommonInfo struct {
	UpstreamID     uint64        `json:"upstream_id"`
	Namespace      string        `json:"namespace"`
	ID             string        `json:"id"`
	FeedState      FeedState     `json:"state"`
	CheckpointTSO  uint64        `json:"checkpoint_tso"`
	CheckpointTime JSONTime      `json:"checkpoint_time"`
	RunningError   *RunningError `json:"error"`
}

// MarshalJSON use to marshal ChangefeedCommonInfo
func (c ChangefeedCommonInfo) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedCommonInfo
	if c.FeedState == StateNormal {
		c.RunningError = nil
	}
	if c.FeedState == StateUnInitialized {
		c.FeedState = StateNormal
	}
	if c.FeedState == StatePending {
		c.FeedState = StateWarning
	}
	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
}

// ChangefeedDetail holds detail info of a changefeed
type ChangefeedDetail struct {
	UpstreamID     uint64              `json:"upstream_id"`
	Namespace      string              `json:"namespace"`
	ID             string              `json:"id"`
	SinkURI        string              `json:"sink_uri"`
	CreateTime     JSONTime            `json:"create_time"`
	StartTs        uint64              `json:"start_ts"`
	ResolvedTs     uint64              `json:"resolved_ts"`
	TargetTs       uint64              `json:"target_ts"`
	CheckpointTSO  uint64              `json:"checkpoint_tso"`
	CheckpointTime JSONTime            `json:"checkpoint_time"`
	Engine         SortEngine          `json:"sort_engine,omitempty"`
	FeedState      FeedState           `json:"state"`
	RunningError   *RunningError       `json:"error"`
	ErrorHis       []int64             `json:"error_history"`
	CreatorVersion string              `json:"creator_version"`
	TaskStatus     []CaptureTaskStatus `json:"task_status,omitempty"`
}

// MarshalJSON use to marshal ChangefeedDetail
func (c ChangefeedDetail) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedDetail
	if c.FeedState == StateNormal {
		c.RunningError = nil
	}
	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
}

// ChangefeedConfig use to create a changefeed
type ChangefeedConfig struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed_id"`
	StartTS   uint64 `json:"start_ts"`
	TargetTS  uint64 `json:"target_ts"`
	SinkURI   string `json:"sink_uri"`
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
	Namespace string `json:"namespace"`
	CfID      string `json:"changefeed_id"`
	CaptureID string `json:"capture_id"`
}

// ProcessorDetail holds the detail info of a processor
type ProcessorDetail struct {
	// All table ids that this processor are replicating.
	Tables []int64 `json:"table_ids"`
}

// CaptureTaskStatus holds TaskStatus of a capture
type CaptureTaskStatus struct {
	CaptureID string `json:"capture_id"`
	// Table list, containing tables that processor should process
	Tables    []int64                     `json:"table_ids"`
	Operation map[TableID]*TableOperation `json:"table_operations"`
}

// Capture holds common information of a capture in cdc
type Capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is_owner"`
	AdvertiseAddr string `json:"address"`
	ClusterID     string `json:"cluster_id"`
}

// DrainCaptureRequest is request for manual `DrainCapture`
type DrainCaptureRequest struct {
	CaptureID string `json:"capture_id"`
}

// DrainCaptureResp is response for manual `DrainCapture`
type DrainCaptureResp struct {
	CurrentTableCount int `json:"current_table_count"`
}
