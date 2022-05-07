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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/base/protocol"
	"github.com/pingcap/tiflow/pkg/context"
)

// ProcessorAgent is a data structure in the Processor that serves as a bridge with
// the Owner.
//
// ProcessorAgent has a BaseAgent embedded in it, which handles the high-level logic of receiving
// commands from the Owner. It also implements ProcessorMessenger interface, which
// provides the BaseAgent with the necessary methods to send messages to the Owner.
//
// The reason for this design is to decouple the scheduling algorithm with the underlying
// RPC server/client.
//
// Note that Agent is not thread-safe, and it is not necessary for it to be thread-safe.
type ProcessorAgent interface {
	Agent
	ProcessorMessenger
}

// Agent is an interface for an object inside Processor that is responsible
// for receiving commands from the Owner.
// Ideally the processor should drive the Agent by Tick.
type Agent interface {
	// Tick is called periodically by the processor to drive the Agent's internal logic.
	Tick(ctx context.Context) error

	// GetLastSentCheckpointTs returns the last checkpoint-ts already sent to the Owner.
	GetLastSentCheckpointTs() (checkpointTs model.Ts)
}

// TableExecutor is an abstraction for "Processor".
//
// This interface is so designed that it would be the least problematic
// to adapt the current Processor implementation to it.
// TODO find a way to make the semantics easier to understand.
type TableExecutor interface {
	AddTable(ctx context.Context, tableID model.TableID) (done bool, err error)
	RemoveTable(ctx context.Context, tableID model.TableID) (done bool, err error)
	IsAddTableFinished(ctx context.Context, tableID model.TableID) (done bool)
	IsRemoveTableFinished(ctx context.Context, tableID model.TableID) (done bool)

	// GetAllCurrentTables should return all tables that are being run,
	// being added and being removed.
	//
	// NOTE: two subsequent calls to the method should return the same
	// result, unless there is a call to AddTable, RemoveTable, IsAddTableFinished
	// or IsRemoveTableFinished in between two calls to this method.
	GetAllCurrentTables() []model.TableID

	// GetCheckpoint returns the local checkpoint-ts and resolved-ts of
	// the processor. Its calculation should take into consideration all
	// tables that would have been returned if GetAllCurrentTables had been
	// called immediately before.
	GetCheckpoint() (checkpointTs, resolvedTs model.Ts)
}

// ProcessorMessenger implements how messages should be sent to the owner,
// and should be able to know whether there are any messages not yet acknowledged
// by the owner.
type ProcessorMessenger interface {
	// FinishTableOperation notifies the owner that a table operation has finished.
	FinishTableOperation(
		ctx context.Context, tableID model.TableID, epoch protocol.ProcessorEpoch,
	) (done bool, err error)
	// SyncTaskStatuses informs the owner of the processor's current internal state.
	SyncTaskStatuses(
		ctx context.Context, epoch protocol.ProcessorEpoch,
		adding, removing, running []model.TableID,
	) (done bool, err error)
	// SendCheckpoint sends the owner the processor's local watermarks,
	// i.e., checkpoint-ts and resolved-ts.
	SendCheckpoint(
		ctx context.Context, checkpointTs model.Ts, resolvedTs model.Ts,
	) (done bool, err error)
	// Barrier returns whether there is a pending message not yet acknowledged by the owner.
	Barrier(ctx context.Context) (done bool)
	// OnOwnerChanged is called when the owner is changed.
	OnOwnerChanged(
		ctx context.Context, newOwnerCaptureID model.CaptureID, newOwnerRevision int64,
	)
	// Close closes the messenger and does the necessary cleanup.
	Close() error
}
