package runtime

import (
	"time"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
)

// TODO: expose this config in lib
var HeartbeatInterval = 3 * time.Second

// WorkerStage represents the stage of a worker.
//          ,──────────────.      ,────────────.      ,─────────────.     ,──────────────.
//          │WorkerCreating│      │WorkerOnline│      │WorkerOffline│     │WorkerFinished│
//          `──────┬───────'      `─────┬──────'      `──────┬──────'     `──────┬───────'
//                 │                    │                    │                   │
//   CreateWorker  │                    │                    │                   │
// ───────────────►│                    │                    │                   │
//                 │  OnWorkerOnline    │                    │                   │
//                 ├───────────────────►│                    │                   │
//                 │                    │  OnWorkerOffline   │                   │
//                 │                    ├───────────────────►│                   │
//                 │                    │                    │                   │
//                 │                    │                    │                   │
//                 │                    │  OnWorkerFinished  │                   │
//                 │                    ├────────────────────┼──────────────────►│
//                 │                    │                    │                   │
//                 │  OnWorkerOffline/OnWorkerDispacth       │                   │
//                 ├────────────────────┬───────────────────►│                   │
//                 │                    │                    │                   │
//                 │                    │                    │                   │
//                 │                    │                    │                   │
//                 │                    │                    │                   │
//                 │  OnWorkerFinished  │                    │                   │
//                 ├────────────────────┼────────────────────┼──────────────────►│
//                 │                    │                    │                   │
//                 │                    │                    │                   │
type WorkerStage int

// All available WorkerStage
const (
	WorkerCreating WorkerStage = iota
	WorkerOnline
	WorkerFinished
	WorkerOffline
	// WorkerDestroying
)

// WorkerStatus manages worker state machine
type WorkerStatus struct {
	TaskID string
	ID     libModel.WorkerID
	Unit   lib.WorkerType
	Stage  WorkerStage
	// only use when creating, change to updatedTime if needed.
	createdTime time.Time
}

// IsOffline checks whether worker stage is offline
func (w *WorkerStatus) IsOffline() bool {
	return w.Stage == WorkerOffline
}

// CreateFailed checks whether the worker creation is failed
func (w *WorkerStatus) CreateFailed() bool {
	return w.Stage == WorkerCreating && w.createdTime.Add(2*HeartbeatInterval).Before(time.Now())
}

// RunAsExpected returns whether a worker is running.
// Currently, we regard worker run as expected except it is offline.
func (w *WorkerStatus) RunAsExpected() bool {
	return w.Stage == WorkerOnline || w.Stage == WorkerCreating || w.Stage == WorkerFinished
}

// InitWorkerStatus creates a new worker status and initializes it
func InitWorkerStatus(taskID string, unit lib.WorkerType, id libModel.WorkerID) WorkerStatus {
	workerStatus := NewWorkerStatus(taskID, unit, id, WorkerCreating)
	workerStatus.createdTime = time.Now()
	return workerStatus
}

// NewWorkerStatus creates a new WorkerStatus instance
func NewWorkerStatus(taskID string, unit lib.WorkerType, id libModel.WorkerID, stage WorkerStage) WorkerStatus {
	return WorkerStatus{
		TaskID: taskID,
		ID:     id,
		Unit:   unit,
		Stage:  stage,
	}
}
