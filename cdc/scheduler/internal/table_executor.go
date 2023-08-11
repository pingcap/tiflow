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
	// AddTable add a new table with `Checkpoint.CheckpointTs`
	// if `isPrepare` is true, the 1st phase of the 2 phase scheduling protocol.
	// if `isPrepare` is false, the 2nd phase.
	AddTable(
		ctx context.Context, tableID model.TableID, checkpoint tablepb.Checkpoint, isPrepare bool,
	) (done bool, err error)

	// IsAddTableFinished make sure the requested table is in the proper status
	IsAddTableFinished(tableID model.TableID, isPrepare bool) (done bool)

	// RemoveTable remove the table, return true if the table is already removed
	RemoveTable(tableID model.TableID) (done bool)
	// IsRemoveTableFinished convince the table is fully stopped.
	// return false if table is not stopped
	// return true and corresponding checkpoint otherwise.
	IsRemoveTableFinished(tableID model.TableID) (model.Ts, bool)

	// GetTableStatus return the checkpoint and resolved ts for the given table.
	GetTableStatus(tableID model.TableID, collectStat bool) tablepb.TableStatus
}
