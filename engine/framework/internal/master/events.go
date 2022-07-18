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

package master

import frameModel "github.com/pingcap/tiflow/engine/framework/model"

type masterEventType int32

const (
	workerOnlineEvent = masterEventType(iota + 1)
	workerOfflineEvent
	workerStatusUpdatedEvent
	workerDispatchFailedEvent
)

type beforeHookType = func() (ok bool)

type masterEvent struct {
	Tp         masterEventType
	Handle     WorkerHandle
	WorkerID   frameModel.WorkerID
	Err        error
	beforeHook beforeHookType
}
