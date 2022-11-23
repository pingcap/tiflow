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

package metadata

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// UnitState represents the state of units.
type UnitState struct {
	// taskID -> sequence of finished status
	FinishedUnitStatus map[string][]*FinishedTaskStatus
	CurrentUnitStatus  map[string]*UnitStatus
}

// UnitStateStore is the meta store for UnitState.
type UnitStateStore struct {
	// rmwLock is used to prevent concurrent read-modify-write to the state.
	rmwLock sync.Mutex
	*frameworkMetaStore
}

func (f *UnitStateStore) createState() state {
	return &UnitState{
		FinishedUnitStatus: map[string][]*FinishedTaskStatus{},
		CurrentUnitStatus:  map[string]*UnitStatus{},
	}
}

func (f *UnitStateStore) key() string {
	return adapter.DMUnitStateAdapter.Encode()
}

// ReadModifyWrite reads the state, modifies it, and writes it back.
func (f *UnitStateStore) ReadModifyWrite(
	ctx context.Context,
	action func(*UnitState) error,
) error {
	f.rmwLock.Lock()
	defer f.rmwLock.Unlock()

	s, err := f.Get(ctx)
	if err != nil {
		if errors.Cause(err) == ErrStateNotFound {
			s = f.createState()
		} else {
			return err
		}
	}
	err = action(s.(*UnitState))
	if err != nil {
		return err
	}
	return f.Put(ctx, s)
}

// NewUnitStateStore creates a new UnitStateStore.
func NewUnitStateStore(kvClient metaModel.KVClient) *UnitStateStore {
	ret := &UnitStateStore{
		frameworkMetaStore: newJSONFrameworkMetaStore(kvClient),
	}
	ret.frameworkMetaStore.stateFactory = ret
	return ret
}

// TaskStatus defines the running task status.
type TaskStatus struct {
	Unit             frameModel.WorkerType
	Task             string
	Stage            TaskStage
	CfgModRevision   uint64
	StageUpdatedTime time.Time
}

// FinishedTaskStatus wraps the TaskStatus with FinishedStatus.
// It only used when a task is finished.
type FinishedTaskStatus struct {
	TaskStatus
	Result   *pb.ProcessResult
	Status   json.RawMessage
	Duration time.Duration
}

// UnitStatus defines the unit status.
type UnitStatus struct {
	Unit        frameModel.WorkerType
	Task        string
	CreatedTime time.Time
}
