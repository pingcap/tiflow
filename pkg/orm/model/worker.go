package model

import (
	"encoding/json"

	"github.com/pingcap/errors"
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

// TODO: check how to use this struct
type WorkerMeta struct {
	Model
	ProjectID    string           `gorm:"column:project_id;type:char(36) not null"`
	JobID        string           `gorm:"column:job_id;type:char(36) not null;uniqueIndex:uidx_id,priority:1;index:idx_st,priority:1"`
	ID           string           `gorm:"column:id;type:char(36) not null;uniqueIndex:uidx_id,priority:2"`
	Type         int              `gorm:"column:type;type:tinyint not null"`
	Status       WorkerStatusCode `json:"code" gorm:"column:status;type:tinyint not null;index:idx_st,priority:2"`
	ErrorMessage string           `json:"error-message" gorm:"column:errmsg;type:varchar(128)"`

	// ExtBytes carries the serialized form of the Ext field, which is used in
	// business logic only.
	// Business logic can parse the raw bytes and decode into business Go object
	ExtBytes []byte `json:"ext-bytes" gorm:"column:ext_bytes;type:blob"`
}

// HasSignificantChange indicates whether `s` has significant changes worth persisting.
func (s *WorkerMeta) HasSignificantChange(other *WorkerMeta) bool {
	return s.Status != other.Status || s.ErrorMessage != other.ErrorMessage
}

// InTerminateState returns whether worker is in a terminate state, including
// finished, stopped, error.
func (s *WorkerMeta) InTerminateState() bool {
	switch s.Status {
	case WorkerStatusFinished, WorkerStatusStopped, WorkerStatusError:
		return true
	default:
		return false
	}
}

func (s *WorkerMeta) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *WorkerMeta) Unmarshal(bytes []byte) error {
	if err := json.Unmarshal(bytes, s); err != nil {
		return errors.Trace(err)
	}
	return nil
}
