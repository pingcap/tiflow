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

package internal

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

// TableExecutor is an abstraction for "Processor".
//
// This interface is so designed that it would be the least problematic
// to adapt the current Processor implementation to it.
// TODO find a way to make the semantics easier to understand.
type TableExecutor interface {
	AddTable(ctx context.Context, tableID model.TableID, startTs model.Ts) (done bool, err error)
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
