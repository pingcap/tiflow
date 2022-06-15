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

package utils

import (
	"context"

	"github.com/pingcap/tiflow/engine/executor/worker/internal"
	"github.com/pingcap/tiflow/engine/framework/eventloop"
)

type worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() internal.RunnableID
	Close(ctx context.Context) error
	NotifyExit(ctx context.Context, errIn error) error
}

// Wrapper is a compatibility layer to use current
// implementations of workers as the new runtime.Runnable.
type Wrapper struct {
	worker
	*eventloop.Runner[worker]
}

// WrapWorker wraps a framework.Worker so that it can be used by
// the runtime.
func WrapWorker(worker worker) *Wrapper {
	return &Wrapper{
		worker: worker,
		Runner: eventloop.NewRunner(worker, worker.ID()),
	}
}
