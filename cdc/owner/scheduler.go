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

package owner

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
)

// scheduler is an interface for scheduling tables.
// Since in our design, we do not record checkpoints per table,
// how we calculate the global watermarks (checkpoint-ts and resolved-ts)
// is heavily coupled with how tables are scheduled.
// That is why we have a scheduler interface that also reports the global watermarks.
type scheduler interface {
	// Tick is called periodically from the owner, and returns
	// updated global watermarks.
	Tick(
		state *orchestrator.ChangefeedReactorState,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable is used to trigger manual table moves.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance is used to trigger manual workload rebalances.
	Rebalance()
}
