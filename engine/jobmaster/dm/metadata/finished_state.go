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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// FinishedState represents the state of finished units.
type FinishedState struct {
	state

	// taskID -> sequence of finished status
	FinishedUnitStatus map[string][]*FinishedTaskStatus
}

type FinishedStateStore struct {
	// rmwLock is used to prevent concurrent read-modify-write to the state.
	rmwLock sync.Mutex
	*frameworkMetaStore
}

func (f *FinishedStateStore) createState() state {
	return &FinishedState{FinishedUnitStatus: map[string][]*FinishedTaskStatus{}}
}

func (f *FinishedStateStore) key() string {
	return adapter.DMFinishedStateAdapter.Encode()
}

func (f *FinishedStateStore) ReadModifyWrite(
	ctx context.Context,
	action func(*FinishedState) error,
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
	err = action(s.(*FinishedState))
	if err != nil {
		return err
	}
	return f.Put(ctx, s)
}

// NewFinishedStateStore creates a new FinishedStateStore.
func NewFinishedStateStore(kvClient metaModel.KVClient) *FinishedStateStore {
	ret := &FinishedStateStore{
		frameworkMetaStore: newJSONFrameworkMetaStore(kvClient),
	}
	ret.frameworkMetaStore.stateFactory = ret
	return ret
}

// TaskStatus defines the running task status.
type TaskStatus struct {
	Unit           frameModel.WorkerType
	Task           string
	Stage          TaskStage
	CfgModRevision uint64
}

// FinishedTaskStatus wraps the TaskStatus with FinishedStatus.
// It only used when a task is finished.
type FinishedTaskStatus struct {
	TaskStatus
	Result *pb.ProcessResult
	Status json.RawMessage
}
