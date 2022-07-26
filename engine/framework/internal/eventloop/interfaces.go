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
	// Note before notifying master, master or worker resource must be cleaned
	// up, except for heartbeat message handler.
	// errIn describes the reason for exiting.
	// TODO: find a better way for the exiting logic. The implement of NotifyExit
	// is a little tricky but we can ensure that after step-2 master can recreate
	// worker without any concern.
	// 1. Close worker impl
	// 2. Cleanup other resources, including base master and base worker
	// 3. Send finished heartbeat to master, and waits for pong message or timeout
	// 4. Cleanup heartbeat message handler
	NotifyExit(ctx context.Context, errIn error) error

	// Close does necessary clean up.
	// TODO `ctx` is for compatibility, remove it.
	Close(ctx context.Context) error

	// ID returns an identifier for the Task.
	ID() string
}
