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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/base"
	"github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/orchestrator"
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
		ctx context.Context,
		state *orchestrator.ChangefeedReactorState,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable is used to trigger manual table moves.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance is used to trigger manual workload rebalances.
	Rebalance()

	// Close closes the scheduler and releases resources.
	Close(ctx context.Context)
}

type schedulerV2Wraper struct {
	*base.SchedulerV2
}

func (s *schedulerV2Wraper) Tick(
	ctx context.Context,
	state *orchestrator.ChangefeedReactorState,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (checkpoint, resolvedTs model.Ts, err error) {
	return s.SchedulerV2.Tick(ctx, state.Status.CheckpointTs, currentTables, captures)
}

func (s *schedulerV2Wraper) Close(ctx context.Context) {
	s.SchedulerV2.Close(ctx)
}

// newSchedulerV2FromCtx creates a new schedulerV2 from context.
// This function is factored out to facilitate unit testing.
func newSchedulerV2FromCtx(ctx context.Context, startTs uint64) (scheduler, error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ownerRev := ctx.GlobalVars().OwnerRevision
	ret, err := base.NewSchedulerV2(
		ctx, changeFeedID, startTs, messageServer, messageRouter, ownerRev)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &schedulerV2Wraper{SchedulerV2: ret}, nil
}

func newScheduler(ctx context.Context, startTs uint64) (scheduler, error) {
	return newSchedulerV2FromCtx(ctx, startTs)
}
