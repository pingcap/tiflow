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

package scheduler

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
)

// TableExecutor is an abstraction for "Processor".
//
// This interface is so designed that it would be the least problematic
// to adapt the current Processor implementation to it.
// TODO find a way to make the semantics easier to understand.
type TableExecutor internal.TableExecutor

// Scheduler is an interface for scheduling tables.
// Since in our design, we do not record checkpoints per table,
// how we calculate the global watermarks (checkpoint-ts and resolved-ts)
// is heavily coupled with how tables are scheduled.
// That is why we have a scheduler interface that also reports the global watermarks.
type Scheduler internal.Scheduler

// InfoProvider is the interface to get information about the internal states of the scheduler.
// We need this interface so that we can provide the information through HTTP API.
type InfoProvider internal.InfoProvider

// Agent is an interface for an object inside Processor that is responsible
// for receiving commands from the Owner.
// Ideally the processor should drive the Agent by Tick.
//
// Note that Agent is not thread-safe
type Agent internal.Agent

// CheckpointCannotProceed is a placeholder indicating that the
// Owner should not advance the global checkpoint TS just yet.
const CheckpointCannotProceed = internal.CheckpointCannotProceed

// NewAgent returns processor agent.
func NewAgent(
	ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	etcdClient *etcd.CDCEtcdClient,
	executor TableExecutor,
	changefeedID model.ChangeFeedID,
) (Agent, error) {
	return base.NewAgent(
		ctx, messageServer, messageRouter, etcdClient, executor, changefeedID)
}

// NewScheduler returns owner scheduler.
func NewScheduler(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	checkpointTs model.Ts,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	ownerRevision int64,
) (Scheduler, error) {
	return base.NewSchedulerV2(
		ctx, changeFeedID, checkpointTs, messageServer, messageRouter, ownerRevision)
}
