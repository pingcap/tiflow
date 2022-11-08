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

package sinkmanager

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
)

// Manager manages the sink of a processor.
// NOTICE: All methods of Manager are thread-safe.
type Manager interface {
	// UpdateBarrierTs updates the barrier ts of all tables in the sink manager.
	UpdateBarrierTs(ts model.Ts)
	// UpdateReceivedSorterResolvedTs updates the resolved ts received from the sorter for the table.
	// It used to advance the progress if RedoLog is enabled.
	UpdateReceivedSorterResolvedTs(tableID model.TableID, ts model.Ts)
	// AddTable adds a table(TableSink) to the sink manager.
	// Sink manager will create a new table sink and start it.
	AddTable(tableID model.TableID, startTs model.Ts, targetTs model.Ts)
	// RemoveTable removes a table(TableSink) from the sink manager.
	// Sink manager will stop the table sink and remove it.
	RemoveTable(tableID model.TableID) error
	// GetTableStats returns the state of the table.
	GetTableStats(tableID model.TableID) (pipeline.Stats, error)
	// Close closes all workers.
	Close() error
}
