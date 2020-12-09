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
	"errors"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

// Reactor is a stateful transform of states.
// It models Owner and Processor, which reacts according to updates in Etcd.
type Reactor interface {
	Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error)
}

var (
	// ErrEtcdTryAgain is used by a PatchFunc to force a transaction abort.
	ErrEtcdTryAgain = errors.New("ErrEtcdTryAgain")
	// ErrEtcdIgnore is used by a PatchFunc to signal that the reactor no longer wishes to update Etcd.
	ErrEtcdIgnore = errors.New("ErrEtcdIgnore")
	// ErrReactorFinished is used by reactor to signal a **normal** exit.
	ErrReactorFinished = errors.New("ErrReactorFinished")
)

// PatchFunc should be a pure function that returns a new value given the old value.
// The function is called each time the EtcdWorker initiates an Etcd transaction.
type PatchFunc = func(old []byte) (newValue []byte, err error)

// DataPatch represents an update to a given Etcd key
type DataPatch struct {
	Key util.EtcdRelKey
	Fun PatchFunc
}

// ReactorState models e Etcd state of a reactor
type ReactorState interface {
	// Update is called by EtcdWorker to notify the Reactor of a latest change to the Etcd state.
	Update(key util.EtcdRelKey, value []byte)

	// GetPatches is called by EtcdWorker, and should return a slice of data patches that represents the changes
	// that a Reactor wants to apply to Etcd.
	GetPatches() []*DataPatch
}
