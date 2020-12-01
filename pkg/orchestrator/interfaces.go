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
)

type Reactor interface {
	Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error)
}

type patchFunc = func (old []byte) (newValue []byte, err error)

var (
	EtcdTryAgain = errors.New("EtcdTryAgain")
	EtcdIgnore = errors.New("EtcdIgnore")
)

type DataPatch struct {
	key []byte
	fun patchFunc
}

type ReactorState interface {
	Update(key []byte, value []byte)
	GetPatches() []*DataPatch
}
