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

// Agent is an interface for an object inside Processor that is responsible
// for receiving commands from the Owner.
// Ideally the processor should drive the Agent by Tick.
type Agent interface {
	// Tick is called periodically by the processor to drive the Agent's internal logic.
	Tick(ctx context.Context) error

	// LastSentCheckpointTs returns the last checkpoint-ts already sent to the Owner.
	LastSentCheckpointTs() (checkpointTs model.Ts)
}

// ProcessorMessenger implements how messages should be sent to the owner,
// and should be able to know whether there are any messages not yet acknowledged
// by the owner.
type ProcessorMessenger interface {
	// FinishTableOperation notifies the owner that a table operation has finished.
	FinishTableOperation(ctx context.Context, tableID model.TableID) (bool, error)
	// SyncTaskStatuses informs the owner of the processor's current internal state.
	SyncTaskStatuses(ctx context.Context, running, adding, removing []model.TableID) (bool, error)
	// SendCheckpoint sends the owner the processor's local watermarks, i.e., checkpoint-ts and resolved-ts.
	SendCheckpoint(ctx context.Context, checkpointTs model.Ts, resolvedTs model.Ts) (bool, error)

	// Barrier returns whether there is a pending message not yet acknowledged by the owner.
	Barrier(ctx context.Context) (done bool)
	// OnOwnerChanged is called when the owner is changed.
	OnOwnerChanged(ctx context.Context, newOwnerCaptureID model.CaptureID)
	// Close closes the messenger and does the necessary cleanup.
	Close() error
}
