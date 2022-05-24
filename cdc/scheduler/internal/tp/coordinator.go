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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
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
	trans        transport
	scheduler    []scheduler
	replicationM *replicationManager
	captureM     *captureManager
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
	recvMsgs, err := c.trans.Recv(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	sentMsgs, hasInit := c.captureM.poll(aliveCaptures, recvMsgs)
	if !hasInit {
		err := c.trans.Send(ctx, sentMsgs)
		return errors.Trace(err)
	}

	captureTables := c.captureM.captureTableSets()
	allTasks := make([]*scheduleTask, 0)
	for _, sched := range c.scheduler {
		tasks := sched.Schedule(checkpointTs, currentTables, aliveCaptures, captureTables)
		allTasks = append(allTasks, tasks...)
	}
	msgs, err := c.replicationM.poll(
		ctx, checkpointTs, currentTables, aliveCaptures, recvMsgs, allTasks)
	if err != nil {
		return errors.Trace(err)
	}
	sentMsgs = append(sentMsgs, msgs...)
	err = c.trans.Send(ctx, sentMsgs)
	if err != nil {
		return errors.Trace(err)
	}

	// checkpoint calcuation
	return nil
}
