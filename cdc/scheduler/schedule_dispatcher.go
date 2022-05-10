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

package scheduler

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

const (
	// CheckpointCannotProceed is a placeholder indicating that the
	// Owner should not advance the global checkpoint TS just yet.
	CheckpointCannotProceed = model.Ts(0)
)

// ScheduleDispatcher is an interface for a table scheduler used in Owner.
type ScheduleDispatcher interface {
	// Tick is called periodically to update the SchedulerDispatcher on the latest state of replication.
	// This function should NOT be assumed to be thread-safe. No concurrent calls allowed.
	Tick(
		ctx context.Context,
		// Latest global checkpoint of the changefeed
		checkpointTs model.Ts,
		// All tables that SHOULD be replicated (or started) at the current checkpoint.
		currentTables []model.TableID,
		// All captures that are alive according to the latest Etcd states.
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable requests that a table be moved to target.
	// It should be thread-safe.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance triggers a rebalance operation.
	// It should be thread-safe
	Rebalance()
}
