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
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	v3 "github.com/pingcap/tiflow/cdc/scheduler/internal/v3"
	v3agent "github.com/pingcap/tiflow/cdc/scheduler/internal/v3/agent"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/prometheus/client_golang/prometheus"
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

// Query is for open api can access the scheduler
type Query internal.Query

// Agent is an interface for an object inside Processor that is responsible
// for receiving commands from the Owner.
// Ideally the processor should drive the Agent by Tick.
//
// Note that Agent is not thread-safe
type Agent internal.Agent

// CheckpointCannotProceed is a placeholder indicating that the
// Owner should not advance the global checkpoint TS just yet.
const CheckpointCannotProceed = internal.CheckpointCannotProceed

// NewAgent returns two-phase agent.
func NewAgent(
	ctx context.Context,
	captureID model.CaptureID,
	liveness *model.Liveness,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	etcdClient etcd.CDCEtcdClient,
	executor TableExecutor,
	changefeedID model.ChangeFeedID,
	changefeedEpoch uint64,
) (Agent, error) {
	return v3agent.NewAgent(
		ctx, captureID, liveness, changefeedID,
		messageServer, messageRouter, etcdClient, executor, changefeedEpoch,
	)
}

// NewScheduler returns two-phase scheduler.
func NewScheduler(
	ctx context.Context,
	captureID model.CaptureID,
	changeFeedID model.ChangeFeedID,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	ownerRevision int64,
	changefeedEpoch uint64,
	cfg *config.SchedulerConfig,
	pdClock pdutil.Clock,
	redoMetaManager redo.MetaManager,
) (Scheduler, error) {
	return v3.NewCoordinator(
		ctx, captureID, changeFeedID,
		messageServer, messageRouter, ownerRevision, changefeedEpoch, cfg, pdClock, redoMetaManager)
}

// InitMetrics registers all metrics used in scheduler
func InitMetrics(registry *prometheus.Registry) {
	v3.InitMetrics(registry)
}
