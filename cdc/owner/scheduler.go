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
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/base"
	"github.com/pingcap/tiflow/pkg/context"
)

// newSchedulerV2FromCtx creates a new schedulerV2 from context.
// This function is factored out to facilitate unit testing.
func newSchedulerV2FromCtx(
	ctx context.Context, startTs uint64,
) (scheduler.Scheduler, error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ownerRev := ctx.GlobalVars().OwnerRevision
	ret, err := base.NewSchedulerV2(
		ctx, changeFeedID, startTs, messageServer, messageRouter, ownerRev)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func newScheduler(ctx context.Context, startTs uint64) (scheduler.Scheduler, error) {
	return newSchedulerV2FromCtx(ctx, startTs)
}
