package runtime

import (
	"github.com/hanfei1991/microcosm/lib"
)

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

const (
	WorkerCreating WorkerStage = iota
	WorkerOnline
	WorkerFinished
	WorkerOffline
	// WorkerDestroying
)

type WorkerStatus struct {
	TaskID string
	ID     lib.WorkerID
	Unit   lib.WorkerType
	Stage  WorkerStage
}

func (w *WorkerStatus) IsOffline() bool {
	return w.Stage == WorkerOffline
}

// currently, we regard worker run as expected except it is offline.
func (w *WorkerStatus) RunAsExpected() bool {
	return w.Stage == WorkerOnline || w.Stage == WorkerCreating || w.Stage == WorkerFinished
}

func NewWorkerStatus(taskID string, unit lib.WorkerType, id lib.WorkerID, stage WorkerStage) WorkerStatus {
	return WorkerStatus{
		TaskID: taskID,
		ID:     id,
		Unit:   unit,
		Stage:  stage,
	}
}
