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

// JobOpStatus represents the expected status of a job, note this status diffs
// from the worker status defined in framework model, the relationship of these
// two status system is as follows.
// - JobOpStatusCanceling, no mapping
// - JobOpStatusCanceled maps to WorkerStatusStopped
type JobOpStatus int8

// Defines all JobOpStatus
const (
	// noop is reserved for some unexpected scenario, such as job operation of
	// a nonexistent job
	JobOpStatusNoop      = JobOpStatus(1)
	JobOpStatusCanceling = JobOpStatus(2)
	JobOpStatusCanceled  = JobOpStatus(3)
)

// JobOpUpdateColumns is used in gorm update.
// TODO: using reflect to generate it more generally
// related to some implement of gorm
var JobOpUpdateColumns = []string{
	"updated_at",
	"op",
	"job_id",
}

// JobOp stores job operation recoreds
type JobOp struct {
	Model
	Op    JobOpStatus `gorm:"type:tinyint not null;index:idx_job_op,priority:2;comment:Canceling(1),Canceled(2)"`
	JobID string      `gorm:"type:varchar(128) not null;uniqueIndex:uk_job_id"`
}

// Map is used for update in orm model
func (op *JobOp) Map() map[string]interface{} {
	return map[string]interface{}{
		"op": op.Op,
	}
}
