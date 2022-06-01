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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

const checkpointCannotProceed = internal.CheckpointCannotProceed

type scheduler interface {
	Name() string
	Schedule(
		checkpointTs model.Ts,
		currentTables []model.TableID,
		aliveCaptures map[model.CaptureID]*model.CaptureInfo,
		replications map[model.TableID]*ReplicationSet,
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
		scheduler:    []scheduler{newBalancer()},
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
	return c.poll(ctx, checkpointTs, currentTables, aliveCaptures)
}

func (c *coordinator) MoveTable(tableID model.TableID, target model.CaptureID) {}

func (c *coordinator) Rebalance() {}

func (c *coordinator) Close(ctx context.Context) {}

// ===========

func (c *coordinator) poll(
	ctx context.Context, checkpointTs model.Ts, currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	recvMsgs, err := c.recvMsgs(ctx)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}

	var msgBuf []*schedulepb.Message
	c.captureM.HandleMessage(recvMsgs)
	msgs := c.captureM.Tick(c.replicationM.ReplicationSets())
	msgBuf = append(msgBuf, msgs...)
	msgs = c.captureM.HandleAliveCaptureUpdate(aliveCaptures)
	msgBuf = append(msgBuf, msgs...)
	if !c.captureM.CheckAllCaptureInitialized() {
		// Skip handling messages and tasks for replication manager,
		// as not all capture are initialized.
		return checkpointCannotProceed, checkpointCannotProceed, c.sendMsgs(ctx, msgBuf)
	}

	// Handle capture membership changes.
	if changes := c.captureM.TakeChanges(); changes != nil {
		msgs, err = c.replicationM.HandleCaptureChanges(changes, checkpointTs)
		if err != nil {
			return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
		}
		msgBuf = append(msgBuf, msgs...)
	}

	// Handle received messages to advance replication set.
	msgs, err = c.replicationM.HandleMessage(recvMsgs)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	// Generate schedule tasks based on the current status.
	replications := c.replicationM.ReplicationSets()
	allTasks := make([]*scheduleTask, 0)
	for _, sched := range c.scheduler {
		tasks := sched.Schedule(checkpointTs, currentTables, aliveCaptures, replications)
		if len(tasks) != 0 {
			log.Info("tpscheduler: new schedule task",
				zap.Int("task", len(tasks)),
				zap.String("scheduler", sched.Name()))
		}
		allTasks = append(allTasks, tasks...)
	}

	// Handle generated schedule tasks.
	msgs, err = c.replicationM.HandleTasks(allTasks)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	// Send new messages.
	err = c.sendMsgs(ctx, msgBuf)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}

	// Checkpoint calculation
	newCheckpointTs, newResolvedTs = c.replicationM.AdvanceCheckpoint(currentTables)
	return newCheckpointTs, newResolvedTs, nil
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
