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

package runtime

import (
	"time"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
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

const (
	WorkerCreating WorkerStage = iota
	WorkerOnline
	WorkerFinished
	WorkerOffline
	// WorkerDestroying
)

type WorkerStatus struct {
	TaskID string
	ID     libModel.WorkerID
	Unit   lib.WorkerType
	Stage  WorkerStage
	// only use when creating, change to updatedTime if needed.
	createdTime time.Time
}

func (w *WorkerStatus) IsOffline() bool {
	return w.Stage == WorkerOffline
}

func (w *WorkerStatus) CreateFailed() bool {
	return w.Stage == WorkerCreating && w.createdTime.Add(2*HeartbeatInterval).Before(time.Now())
}

// currently, we regard worker run as expected except it is offline.
func (w *WorkerStatus) RunAsExpected() bool {
	return w.Stage == WorkerOnline || w.Stage == WorkerCreating || w.Stage == WorkerFinished
}

func InitWorkerStatus(taskID string, unit lib.WorkerType, id libModel.WorkerID) WorkerStatus {
	workerStatus := NewWorkerStatus(taskID, unit, id, WorkerCreating)
	workerStatus.createdTime = time.Now()
	return workerStatus
}

func NewWorkerStatus(taskID string, unit lib.WorkerType, id libModel.WorkerID, stage WorkerStage) WorkerStatus {
	return WorkerStatus{
		TaskID: taskID,
		ID:     id,
		Unit:   unit,
		Stage:  stage,
	}
}
