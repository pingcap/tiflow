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

package taskutil

import (
	"github.com/pingcap/tiflow/engine/framework/internal/eventloop"
)

// Wrapper is a compatibility layer to use current
// implementations of workers as the new runtime.Runnable.
type Wrapper struct {
	eventloop.Task
	*eventloop.Runner[eventloop.Task]
}

// WrapWorker wraps a framework.Worker so that it can be used by
// the runtime.
func WrapWorker(worker eventloop.Task) *Wrapper {
	return &Wrapper{
		Task:   worker,
		Runner: eventloop.NewRunner(worker),
	}
}
