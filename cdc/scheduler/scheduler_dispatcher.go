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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
)

// ScheduleDispatcher is an interface for a table scheduler used in Owner.
type ScheduleDispatcher interface {
	// Tick is called periodically to update the SchedulerDispatcher on the latest state of replication.
	// This function should NOT be assumed to be thread-safe. No concurrent calls allowed.
	Tick(
		ctx context.Context,
		checkpointTs model.Ts, // Latest global checkpoint of the changefeed
		currentTables []model.TableID, // All tables that SHOULD be replicated (or started) at the current checkpoint.
		captures map[model.CaptureID]*model.CaptureInfo, // All captures that are alive according to the latest Etcd states.
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable requests that a table be moved to target.
	// It should be thread-safe.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance triggers a rebalance operation.
	// It should be thread-safe
	Rebalance()
}

// ScheduleDispatcherCommunicator is an interface for the BaseScheduleDispatcher to
// send commands to Processors. The owner of a BaseScheduleDispatcher should provide
// an implementation of ScheduleDispatcherCommunicator to supply BaseScheduleDispatcher
// some methods to specify its behavior.
type ScheduleDispatcherCommunicator interface {
	// DispatchTable should send a dispatch command to the Processor.
	DispatchTable(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		tableID model.TableID,
		captureID model.CaptureID,
		isDelete bool, // True when we want to remove a table from the capture.
	) (done bool, err error)

	// Announce announces to the specified capture that the current node has become the Owner.
	Announce(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		captureID model.CaptureID) (done bool, err error)
}

const (
	// CheckpointCannotProceed is a placeholder indicating that the
	// Owner should not advance the global checkpoint TS just yet.
	CheckpointCannotProceed = model.Ts(0)
)
