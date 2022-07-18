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

	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

// WorkerStatusCode represents worker running status in master worker framework
// TODO: add fsm of WorkerStatusCode
type WorkerStatusCode int8

// Among these statuses, only WorkerStatusCreated is used by the framework
// for now. The rest are for the business logic to use.
// TODO think about whether to manage the transition of the statuses.
// TODO: need a FSM graph
// NOTICE: DO NOT CHANGE the previous status code
// Modify the WorkerStatus.Code comment IF you add some new status code
const (
	WorkerStatusNormal   = WorkerStatusCode(1)
	WorkerStatusCreated  = WorkerStatusCode(2)
	WorkerStatusInit     = WorkerStatusCode(3)
	WorkerStatusError    = WorkerStatusCode(4)
	WorkerStatusFinished = WorkerStatusCode(5)
	WorkerStatusStopped  = WorkerStatusCode(6)
	// extend the status code here
)

// WorkerUpdateColumns is used in gorm update.
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

// WorkerStatus records worker information, including master id, worker id,
// worker type, project id(tenant), worker status(used in master worker framework),
// error message and ext bytes(passed from business logic) in metastore.
// TODO: refine me, merge orm model to WorkerStatus will cause some confuse
type WorkerStatus struct {
	ormModel.Model
	ProjectID    tenant.ProjectID `json:"project-id" gorm:"column:project_id;type:varchar(64) not null"`
	JobID        MasterID         `json:"job-id" gorm:"column:job_id;type:varchar(64) not null;uniqueIndex:uidx_wid,priority:1;index:idx_wst,priority:1"`
	ID           WorkerID         `json:"id" gorm:"column:id;type:varchar(64) not null;uniqueIndex:uidx_wid,priority:2"`
	Type         WorkerType       `json:"type" gorm:"column:type;type:smallint not null;comment:JobManager(1),CvsJobMaster(2),FakeJobMaster(3),DMJobMaster(4),CDCJobMaster(5),CvsTask(6),FakeTask(7),DMTask(8),CDCTask(9),WorkerDMDump(10),WorkerDMLoad(11),WorkerDMSync(12)"`
	Code         WorkerStatusCode `json:"code" gorm:"column:status;type:tinyint not null;index:idx_wst,priority:2;comment:Normal(1),Created(2),Init(3),Error(4),Finished(5),Stopped(6)"`
	ErrorMessage string           `json:"error-message" gorm:"column:errmsg;type:text"`

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

// Marshal returns the JSON encoding of WorkerStatus.
func (s *WorkerStatus) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// Unmarshal parses the JSON-encoded data and stores the result into a WorkerStatus
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
