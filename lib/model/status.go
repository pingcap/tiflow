package model

import (
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/pkg/adapter"
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

type WorkerStatus struct {
	Code         WorkerStatusCode `json:"code"`
	ErrorMessage string           `json:"error-message"`

	// ExtBytes carries the serialized form of the Ext field, which is used in
	// business logic only.
	// Business logic can parse the raw bytes and decode into business Go object
	ExtBytes []byte `json:"ext-bytes"`
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

func EncodeWorkerStatusKey(masterID string, workerID string) string {
	return adapter.WorkerKeyAdapter.Encode(masterID, workerID)
}
