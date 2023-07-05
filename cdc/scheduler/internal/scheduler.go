// Copyright 2021 PingCAP, Inc.
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

package internal

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
)

const (
	// CheckpointCannotProceed is a placeholder indicating that the
	// Owner should not advance the global checkpoint TS just yet.
	CheckpointCannotProceed = model.Ts(0)
)

// Scheduler is an interface for scheduling tables.
// Since in our design, we do not record checkpoints per table,
// how we calculate the global watermarks (checkpoint-ts and resolved-ts)
// is heavily coupled with how tables are scheduled.
// That is why we have a scheduler interface that also reports the global watermarks.
type Scheduler interface {
	// Tick is called periodically from the owner, and returns
	// updated global watermarks.
	// It is not thread-safe.
	Tick(
		ctx context.Context,
		// Latest global checkpoint of the changefeed
		checkpointTs model.Ts,
		// All tables that SHOULD be replicated (or started) at the current checkpoint.
		currentTables []model.TableID,
		// All captures that are alive according to the latest Etcd states.
		aliveCaptures map[model.CaptureID]*model.CaptureInfo,
		// barrier contains the barrierTs of those tables that have
		// ddl jobs that need to be replicated. The Scheduler will
		// broadcast the barrierTs to all captures through the Heartbeat.
		barrier *schedulepb.BarrierWithMinTs,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable requests that a table be moved to target.
	// It is thread-safe.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance triggers a rebalance operation.
	// It is thread-safe
	Rebalance()

	// DrainCapture is used to drop all tables situated at the target capture
	// It is thread-safe.
	DrainCapture(target model.CaptureID) (int, error)

	// Close scheduler and release resource.
	// It is not thread-safe.
	Close(ctx context.Context)
}

// Query is for scheduler related owner job.
// at the moment, only for `DrainCapture`, we can use this to handle all manual schedule task.
// TODO: refactor `MoveTable` use Query to access the scheduler
type Query struct {
	CaptureID model.CaptureID

	Resp interface{}
}
