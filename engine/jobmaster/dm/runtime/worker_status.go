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
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// HeartbeatInterval is heartbeat interval for checking worker stage
// TODO: expose this config in lib
var HeartbeatInterval = 3 * time.Second

/*
         ,──────────────.      ,────────────.      ,─────────────.     ,──────────────.
         │WorkerCreating│      │WorkerOnline│      │WorkerOffline│     │WorkerFinished│
         `──────┬───────'      `─────┬──────'      `──────┬──────'     `──────┬───────'
                │                    │                    │                   │
  CreateWorker  │                    │                    │                   │
───────────────►│                    │                    │                   │
                │  OnWorkerOnline    │                    │                   │
                ├───────────────────►│                    │                   │
                │                    │  OnWorkerOffline   │                   │
                │                    ├───────────────────►│                   │
                │                    │                    │                   │
                │                    │                    │                   │
                │                    │  OnWorkerFinished  │                   │
                │                    ├────────────────────┼──────────────────►│
                │                    │                    │                   │
                │  OnWorkerOffline/OnWorkerDispacth       │                   │
                ├────────────────────┬───────────────────►│                   │
                │                    │                    │                   │
                │                    │                    │                   │
                │                    │                    │                   │
                │                    │                    │                   │
                │  OnWorkerFinished  │                    │                   │
                ├────────────────────┼────────────────────┼──────────────────►│
                │                    │                    │                   │
                │                    │                    │                   │
*/

// WorkerStage represents the stage of a worker.
type WorkerStage int

// All available WorkerStage
const (
	WorkerCreating WorkerStage = iota + 1
	WorkerOnline
	WorkerFinished
	WorkerOffline
	// WorkerDestroying
)

var typesStringify = [...]string{
	0:              "",
	WorkerCreating: "Creating",
	WorkerOnline:   "Online",
	WorkerFinished: "Finished",
	WorkerOffline:  "Offline",
}

var toWorkerStage map[string]WorkerStage

func init() {
	toWorkerStage = make(map[string]WorkerStage, len(typesStringify))
	for i, s := range typesStringify {
		toWorkerStage[s] = WorkerStage(i)
	}
}

// String implements fmt.Stringer interface
func (ws WorkerStage) String() string {
	if int(ws) >= len(typesStringify) || ws < 0 {
		return fmt.Sprintf("Unknown WorkerStage %d", ws)
	}
	return typesStringify[ws]
}

// MarshalJSON marshals the enum as a quoted json string
func (ws WorkerStage) MarshalJSON() ([]byte, error) {
	return json.Marshal(ws.String())
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (ws *WorkerStage) UnmarshalJSON(b []byte) error {
	var (
		j  string
		ok bool
	)
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*ws, ok = toWorkerStage[j]
	if !ok {
		return errors.Errorf("Unknown WorkerStage %s", j)
	}
	return nil
}

// WorkerStatus manages worker state machine
type WorkerStatus struct {
	TaskID         string
	ID             frameModel.WorkerID
	Unit           framework.WorkerType
	Stage          WorkerStage
	CfgModRevision uint64
	// only use when creating, change to updatedTime if needed.
	createdTime time.Time
}

// IsOffline checks whether worker stage is offline
func (w *WorkerStatus) IsOffline() bool {
	return w.Stage == WorkerOffline
}

// IsTombStone returns whether the worker is tombstone, which means we don't need to stop it.
func (w *WorkerStatus) IsTombStone() bool {
	return w.Stage == WorkerOffline || w.Stage == WorkerFinished || w.CreateFailed()
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
func InitWorkerStatus(taskID string, unit framework.WorkerType, id frameModel.WorkerID) WorkerStatus {
	workerStatus := NewWorkerStatus(taskID, unit, id, WorkerCreating, 0)
	workerStatus.createdTime = time.Now()
	return workerStatus
}

// NewWorkerStatus creates a new WorkerStatus instance
func NewWorkerStatus(taskID string, unit framework.WorkerType, id frameModel.WorkerID, stage WorkerStage, cfgModRevision uint64) WorkerStatus {
	return WorkerStatus{
		TaskID:         taskID,
		ID:             id,
		Unit:           unit,
		Stage:          stage,
		CfgModRevision: cfgModRevision,
	}
}
