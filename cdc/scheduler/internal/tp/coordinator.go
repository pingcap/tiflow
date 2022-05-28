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

package tp

import (
	"context"
	"log"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

type scheduler interface {
	Name() string
	Schedule(
		checkpointTs model.Ts,
		currentTables []model.TableID,
		aliveCaptures map[model.CaptureID]*model.CaptureInfo,
		captureTables map[model.CaptureID]*CaptureStatus,
	) []*scheduleTask
}

var _ internal.Scheduler = (*coordinator)(nil)

type coordinator struct {
	version      string
	revision     schedulepb.OwnerRevision
	trans        transport
	scheduler    []scheduler
	replicationM *replicationManager
	captureM     *captureManager
}

// NewCoordinator returns a two phase scheduler.
func NewCoordinator(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	checkpointTs model.Ts,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	ownerRevision int64,
	cfg *config.SchedulerConfig,
) (internal.Scheduler, error) {
	trans, err := newTransport(ctx, changeFeedID, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	revision := schedulepb.OwnerRevision{Revision: ownerRevision}
	return &coordinator{
		version:      version.ReleaseSemver(),
		revision:     revision,
		trans:        trans,
		replicationM: newReplicationManager(cfg.MaxTaskConcurrency),
		captureM:     newCaptureManager(revision, cfg.HeartbeatTick),
	}, nil
}

func (c *coordinator) Tick(
	ctx context.Context,
	// Latest global checkpoint of the changefeed
	checkpointTs model.Ts,
	// All tables that SHOULD be replicated (or started) at the current checkpoint.
	currentTables []model.TableID,
	// All captures that are alive according to the latest Etcd states.
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	err = c.poll(ctx, checkpointTs, currentTables, aliveCaptures)
	if err != nil {
		return internal.CheckpointCannotProceed, internal.CheckpointCannotProceed, errors.Trace(err)
	}
	return internal.CheckpointCannotProceed, internal.CheckpointCannotProceed, nil
}

func (c *coordinator) MoveTable(tableID model.TableID, target model.CaptureID) {}

func (c *coordinator) Rebalance() {}

func (c *coordinator) Close(ctx context.Context) {}

// ===========

func (c *coordinator) poll(
	ctx context.Context, checkpointTs model.Ts, currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) error {
	recvMsgs, err := c.recvMsgs(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	sentMsgs := c.captureM.Tick()
	msgs := c.captureM.Poll(aliveCaptures, recvMsgs)
	sentMsgs = append(sentMsgs, msgs...)
	if c.captureM.CheckAllCaptureInitialized() {
		// Skip polling replication manager as not all capture are initialized.
		err := c.sendMsgs(ctx, sentMsgs)
		return errors.Trace(err)
	}

	// Handling received messages to advance replication set.
	msgs, err = c.replicationM.HandleMessage(recvMsgs)
	if err != nil {
		return errors.Trace(err)
	}
	sentMsgs = append(sentMsgs, msgs...)

	// Generate schedule tasks based on the current status.
	captureTables := c.captureM.CaptureTableSets()
	allTasks := make([]*scheduleTask, 0)
	for _, sched := range c.scheduler {
		tasks := sched.Schedule(checkpointTs, currentTables, aliveCaptures, captureTables)
		allTasks = append(allTasks, tasks...)
	}

	// Handling generated schedule tasks.
	msgs, err = c.replicationM.HandleTasks(allTasks)
	if err != nil {
		return errors.Trace(err)
	}
	sentMsgs = append(sentMsgs, msgs...)

	// Send new messages.
	err = c.sendMsgs(ctx, sentMsgs)
	if err != nil {
		return errors.Trace(err)
	}

	// checkpoint calculation
	return nil
}

func (c *coordinator) recvMsgs(ctx context.Context) ([]*schedulepb.Message, error) {
	recvMsgs, err := c.trans.Recv(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := 0
	for _, val := range recvMsgs {
		// Filter stale messages.
		if val.Header.OwnerRevision == c.revision {
			recvMsgs[n] = val
			n++
		}
	}
	return recvMsgs[:n], nil
}

func (c *coordinator) sendMsgs(ctx context.Context, msgs []*schedulepb.Message) error {
	for i := range msgs {
		m := msgs[i]
		m.Header = &schedulepb.Message_Header{
			Version:       c.version,
			OwnerRevision: c.revision,
		}
		// Correctness check.
		if len(m.To) == 0 || m.MsgType == schedulepb.MsgUnknown {
			log.Panic("invalid message no destination or unknown message type",
				zap.Any("message", m))
		}
	}
	return c.trans.Send(ctx, msgs)
}
