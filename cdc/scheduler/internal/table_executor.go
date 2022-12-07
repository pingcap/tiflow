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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

// TableExecutor is an abstraction for "Processor".
//
// This interface is so designed that it would be the least problematic
// to adapt the current Processor implementation to it.
// TODO find a way to make the semantics easier to understand.
type TableExecutor interface {
	// AddTable add a new table with `startTs`
	// if `isPrepare` is true, the 1st phase of the 2 phase scheduling protocol.
	// if `isPrepare` is false, the 2nd phase.
	AddTable(
		ctx context.Context, span tablepb.Span, startTs model.Ts, isPrepare bool,
	) (done bool, err error)

	// IsAddTableFinished make sure the requested table is in the proper status
	IsAddTableFinished(span tablepb.Span, isPrepare bool) (done bool)

	// RemoveTable remove the table, return true if the table is already removed
	RemoveTable(span tablepb.Span) (done bool)
	// IsRemoveTableFinished convince the table is fully stopped.
	// return false if table is not stopped
	// return true and corresponding checkpoint otherwise.
	IsRemoveTableFinished(span tablepb.Span) (model.Ts, bool)

	// GetTableCount should return the number of tables that are being run,
	// being added and being removed.
	//
	// NOTE: two subsequent calls to the method should return the same
	// result, unless there is a call to AddTable, RemoveTable, IsAddTableFinished
	// or IsRemoveTableFinished in between two calls to this method.
	GetTableCount() int

	// GetCheckpoint returns the local checkpoint-ts and resolved-ts of
	// the processor. Its calculation should take into consideration all
	// tables that would have been returned if GetTableCount had been
	// called immediately before.
	GetCheckpoint() (checkpointTs, resolvedTs model.Ts)

	// GetTableStatus return the checkpoint and resolved ts for the given table
	GetTableStatus(span tablepb.Span) tablepb.TableStatus
}
