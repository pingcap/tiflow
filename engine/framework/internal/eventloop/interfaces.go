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

package eventloop

import (
	"context"
)

// Task defines the common methods used to drive both workers
// and masters.
// It will replace the current `Worker` interface in further refactors.
type Task interface {
	// Init is called before entering the loop calling Poll.
	Init(ctx context.Context) error

	// Poll should be called periodically until an error is returned.
	Poll(ctx context.Context) error

	// NotifyExit does necessary bookkeeping job for exiting,
	// such as notifying a master.
	// errIn describes the reason for exiting.
	NotifyExit(ctx context.Context, errIn error) error

	// Close does necessary clean up.
	// TODO `ctx` is for compatibility, remove it.
	Close(ctx context.Context) error

	// Stop is called when a task is canceled
	Stop(ctx context.Context) error

	// ID returns an identifier for the Task.
	ID() string
}
