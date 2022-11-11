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

package dm

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/tiflow/dm/pb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Defines topics here
const (
	OperateTask  p2p.Topic = "OperateTask"
	QueryStatus  p2p.Topic = "QueryStatus"
	StopWorker   p2p.Topic = "StopWorker"
	GetJobCfg    p2p.Topic = "GetJobCfg"
	Binlog       p2p.Topic = "Binlog"
	BinlogSchema p2p.Topic = "BinlogSchema"

	// internal
	BinlogTask       p2p.Topic = "BinlogTask"
	BinlogSchemaTask p2p.Topic = "BinlogSchemaTask"
)

// OperateType represents internal operate type in DM
// TODO: use OperateType in lib or move OperateType to lib.
type OperateType int

// These op may updated in later pr.
const (
	None OperateType = iota
	Create
	Pause
	Resume
	Update
	Delete
	// internal
	Deleting
)

var typesStringify = [...]string{
	0:        "",
	Create:   "Create",
	Pause:    "Pause",
	Resume:   "Resume",
	Update:   "Update",
	Delete:   "Delete",
	Deleting: "Deleting",
}

var toOperateType map[string]OperateType

func init() {
	toOperateType = make(map[string]OperateType, len(typesStringify))
	for i, s := range typesStringify {
		toOperateType[s] = OperateType(i)
	}
}

// String implements fmt.Stringer interface
func (op OperateType) String() string {
	if int(op) >= len(typesStringify) || op < 0 {
		return fmt.Sprintf("Unknown OperateType %d", op)
	}
	return typesStringify[op]
}

// MarshalJSON marshals the enum as a quoted json string
func (op OperateType) MarshalJSON() ([]byte, error) {
	return json.Marshal(op.String())
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (op *OperateType) UnmarshalJSON(b []byte) error {
	var (
		j  string
		ok bool
	)
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*op, ok = toOperateType[j]
	if !ok {
		return errors.Errorf("Unknown OperateType %s", j)
	}
	return nil
}

// OperateTaskMessage is operate task message
type OperateTaskMessage struct {
	Task string
	Op   OperateType
}

// StopWorkerMessage is stop worker message
type StopWorkerMessage struct {
	Task string
}

// QueryStatusRequest is query status request
type QueryStatusRequest struct {
	Task string
}

// ProcessError copies pb.ProcessError expect for JSON tag.
type ProcessError struct {
	ErrCode    int32  `json:"error_code,omitempty"`
	ErrClass   string `json:"error_class,omitempty"`
	ErrScope   string `json:"error_scope,omitempty"`
	ErrLevel   string `json:"error_level,omitempty"`
	Message    string `json:"message,omitempty"`
	RawCause   string `json:"raw_cause,omitempty"`
	Workaround string `json:"workaround,omitempty"`
}

// ProcessResult copies pb.ProcessResult expect for JSON tag.
type ProcessResult struct {
	IsCanceled bool            `protobuf:"varint,1,opt,name=isCanceled,proto3" json:"is_canceled,omitempty"`
	Errors     []*ProcessError `protobuf:"bytes,2,rep,name=errors,proto3" json:"errors,omitempty"`
	Detail     []byte          `protobuf:"bytes,3,opt,name=detail,proto3" json:"detail,omitempty"`
}

// NewProcessResultFromPB converts ProcessResult from pb.ProcessResult.
func NewProcessResultFromPB(result *pb.ProcessResult) *ProcessResult {
	if result == nil {
		return nil
	}
	ret := &ProcessResult{
		IsCanceled: result.IsCanceled,
		Detail:     result.Detail,
	}
	for _, err := range result.Errors {
		ret.Errors = append(ret.Errors, &ProcessError{
			ErrCode:    err.ErrCode,
			ErrClass:   err.ErrClass,
			ErrScope:   err.ErrScope,
			ErrLevel:   err.ErrLevel,
			Message:    err.Message,
			RawCause:   err.RawCause,
			Workaround: err.Workaround,
		})
	}
	return ret
}

// QueryStatusResponse is query status response
type QueryStatusResponse struct {
	ErrorMsg string                `json:"error_message"`
	Unit     frameModel.WorkerType `json:"unit"`
	Stage    metadata.TaskStage    `json:"stage"`
	Result   *ProcessResult        `json:"result"`
	Status   json.RawMessage       `json:"status"`
}

// BinlogRequest is binlog request
type BinlogRequest pb.HandleErrorRequest

// BinlogResponse is binlog response
type BinlogResponse struct {
	ErrorMsg string
	// taskID -> task response
	Results map[string]*CommonTaskResponse
}

// BinlogTaskRequest is binlog task request
type BinlogTaskRequest pb.HandleWorkerErrorRequest

// BinlogSchemaRequest is binlog schema request
type BinlogSchemaRequest pb.OperateSchemaRequest

// BinlogSchemaResponse is binlog schema response
type BinlogSchemaResponse struct {
	ErrorMsg string
	// taskID -> task response
	Results map[string]*CommonTaskResponse
}

// BinlogSchemaTaskRequest is binlog schema task request
type BinlogSchemaTaskRequest pb.OperateWorkerSchemaRequest

// CommonTaskResponse is common task response
type CommonTaskResponse struct {
	ErrorMsg string
	Msg      string
}
