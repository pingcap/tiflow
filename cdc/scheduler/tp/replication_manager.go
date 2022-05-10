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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/tp/schedulepb"
)

type callback func(model.TableID)

// burstBalance for changefeed set up or unplaned TiCDC node failure.
// TiCDC needs to balance intrrupted tables as soon as possible.
type burstBalance struct {
	tables map[model.TableID]model.CaptureID

	done callback
}

type moveTable struct {
	tableID       model.TableID
	sourceCapture model.CaptureID
	destCapture   model.CaptureID

	done callback
}

type addTable struct {
	tableID   model.TableID
	captureID model.CaptureID

	done callback
}

type deleteTable struct {
	tableID   model.TableID
	captureID model.CaptureID

	done callback
}

type scheduleTask struct {
	moveTable   *moveTable
	addTable    *addTable
	deleteTable *deleteTable
}

type replicationManager struct {
	tables       map[model.TableID]ReplicationSet
	runningTasks map[model.TableID]*scheduleTask
	captures     map[model.CaptureID]captureStatus
}

func (s *replicationManager) captureTableSets() map[model.CaptureID]captureStatus {
	return s.captures
}

func (s *replicationManager) poll(
	ctx context.Context,
	// Latest global checkpoint of the changefeed
	checkpointTs model.Ts,
	// All tables that SHOULD be replicated (or started) at the current checkpoint.
	currentTables []model.TableID,
	// All captures that are alive according to the latest Etcd states.
	captures map[model.CaptureID]*model.CaptureInfo,
	msgs []*schedulepb.Message,
	tasks []*scheduleTask,
) ([]*schedulepb.Message, error) {
	// s.handleMessage(msgs)
	//
	// s.handleTasks(tasks)
	//
	// s.sendMessages(msgs)

	return nil, nil
}

func (s *replicationManager) handleMessage(msg []*schedulepb.Message) {
	// s.handleMessageSync()
	// s.handleMessageCheckpoint()
	// s.handleMessageDispatchTableResponse()
}

func (s *replicationManager) handleMessageSync(msg *schedulepb.Sync) {
	// TODO: build s.tables from Sync message.
}

func (s *replicationManager) handleMessageDispatchTableResponse(msg *schedulepb.DispatchTableResponse) {
	// TODO: update s.tables from DispatchTableResponse message.
}

func (s *replicationManager) handleMessageCheckpoint(msg *schedulepb.Checkpoint) {
	// TODO: update s.tables from Checkpoint message.
}

// ========

func (s *replicationManager) handleTasks(tasks []*scheduleTask) {
	// s.handleTaskAddTable(nil)
	// s.handleTaskMoveTable(nil)
	// s.handleTaskDeleteTable(nil)
}

func (s *replicationManager) handleTaskMoveTable(task *moveTable) error {
	// TODO: update s.runingTasks and s.tables.
	return nil
}

func (s *replicationManager) handleTaskAddTable(task *addTable) error {
	// TODO: update s.runingTasks and s.tables.
	return nil
}

func (s *replicationManager) handleTaskDeleteTable(task *deleteTable) error {
	// TODO: update s.runingTasks and s.tables.
	return nil
}
