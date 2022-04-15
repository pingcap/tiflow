// Copyright 2020 PingCAP, Inc.
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

package orchestrator

import (
	"context"

	"github.com/pingcap/tiflow/pkg/orchestrator/util"
)

// Reactor is a stateful transform of states.
// It models Owner and Processor, which reacts according to updates in Etcd.
type Reactor interface {
	Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error)
}

// DataPatch represents an update of state
type DataPatch interface {
	Patch(valueMap map[util.EtcdKey][]byte, changedSet map[util.EtcdKey]struct{}) error
}

// ReactorState models the Etcd state of a reactor
type ReactorState interface {
	// Update is called by EtcdWorker to notify the Reactor of a latest change to the Etcd state.
	Update(key util.EtcdKey, value []byte, isInit bool) error

	// GetPatches is called by EtcdWorker, and should return many slices of data patches that represents the changes
	// that a Reactor wants to apply to Etcd.
	// a slice of DataPatch will be committed as one ETCD txn
	GetPatches() [][]DataPatch
}

// SingleDataPatch represents an update to a given Etcd key
type SingleDataPatch struct {
	Key util.EtcdKey
	// Func should be a pure function that returns a new value given the old value.
	// The function is called each time the EtcdWorker initiates an Etcd transaction.
	Func func(old []byte) (newValue []byte, changed bool, err error)
}

// Patch implements the DataPatch interface
func (s *SingleDataPatch) Patch(valueMap map[util.EtcdKey][]byte, changedSet map[util.EtcdKey]struct{}) error {
	value := valueMap[s.Key]
	newValue, changed, err := s.Func(value)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	changedSet[s.Key] = struct{}{}
	if newValue == nil {
		delete(valueMap, s.Key)
	} else {
		valueMap[s.Key] = newValue
	}
	return nil
}
