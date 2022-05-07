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

package model

import (
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
)

type WorkerStatusCode int32

// Among these statuses, only WorkerStatusCreated is used by the framework
// for now. The rest are for the business logic to use.
// TODO think about whether to manage the transition of the statuses.
// TODO: need a FSM graph
const (
	WorkerStatusNormal = WorkerStatusCode(iota + 1)
	WorkerStatusCreated
	WorkerStatusInit
	WorkerStatusError
	WorkerStatusFinished
	WorkerStatusStopped
)

// TODO: using reflect to generate it more generally
// related to some implement of gorm
var WorkerUpdateColumns = []string{
	"updated_at",
	"project_id",
	"job_id",
	"id",
	"type",
	"status",
	"errmsg",
	"ext_bytes",
}

// TODO: refine me, merge orm model to WorkerStatus will cause some confuse
type WorkerStatus struct {
	ormModel.Model
	ProjectID    string           `json:"project-id" gorm:"column:project_id;type:varchar(64) not null"`
	JobID        string           `json:"job-id" gorm:"column:job_id;type:varchar(64) not null;uniqueIndex:uidx_id,priority:1;index:idx_st,priority:1"`
	ID           string           `json:"id" gorm:"column:id;type:varchar(64) not null;uniqueIndex:uidx_id,priority:2"`
	Type         int              `json:"type" gorm:"column:type;type:tinyint not null"`
	Code         WorkerStatusCode `json:"code" gorm:"column:status;type:tinyint not null;index:idx_st,priority:2"`
	ErrorMessage string           `json:"error-message" gorm:"column:errmsg;type:varchar(128)"`

	// ExtBytes carries the serialized form of the Ext field, which is used in
	// business logic only.
	// Business logic can parse the raw bytes and decode into business Go object
	ExtBytes []byte `json:"ext-bytes" gorm:"column:ext_bytes;type:blob"`
}

// HasSignificantChange indicates whether `s` has significant changes worth persisting.
func (s *WorkerStatus) HasSignificantChange(other *WorkerStatus) bool {
	return s.Code != other.Code || s.ErrorMessage != other.ErrorMessage
}

// InTerminateState returns whether worker is in a terminate state, including
// finished, stopped, error.
func (s *WorkerStatus) InTerminateState() bool {
	switch s.Code {
	case WorkerStatusFinished, WorkerStatusStopped, WorkerStatusError:
		return true
	default:
		return false
	}
}

func (s *WorkerStatus) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *WorkerStatus) Unmarshal(bytes []byte) error {
	if err := json.Unmarshal(bytes, s); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Map is used for update the orm model
func (s *WorkerStatus) Map() map[string]interface{} {
	return map[string]interface{}{
		"project_id": s.ProjectID,
		"job_id":     s.JobID,
		"id":         s.ID,
		"type":       s.Type,
		"status":     s.Code,
		"errmsg":     s.ErrorMessage,
		"ext_bytes":  s.ExtBytes,
	}
}

func EncodeWorkerStatusKey(masterID string, workerID string) string {
	return adapter.WorkerKeyAdapter.Encode(masterID, workerID)
}
