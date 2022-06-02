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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ scheduler = &burstBalanceScheduler{}

type burstBalanceScheduler struct{}

func newBurstBalanceScheduler() *burstBalanceScheduler {
	return &burstBalanceScheduler{}
}

func (b *burstBalanceScheduler) Name() string {
	return string(schedulerTypeBurstBalance)
}

func (b *burstBalanceScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
) []*scheduleTask {
	newTables := make([]model.TableID, 0)
	intersectionTable := make(map[model.TableID]struct{})
	captureTables := make(map[model.CaptureID][]model.TableID)
	for _, tableID := range currentTables {
		rep, ok := replications[tableID]
		if !ok {
			newTables = append(newTables, tableID)
			continue
		}
		if rep.State == ReplicationSetStateAbsent {
			newTables = append(newTables, tableID)
		}
		intersectionTable[tableID] = struct{}{}
		if rep.Primary != "" {
			captureTables[rep.Primary] = append(captureTables[rep.Primary], tableID)
		}
		if rep.Secondary != "" {
			captureTables[rep.Secondary] = append(captureTables[rep.Secondary], tableID)
		}
	}
	rmTables := make([]model.TableID, 0)
	for tableID := range replications {
		_, ok := intersectionTable[tableID]
		if !ok {
			rmTables = append(rmTables, tableID)
		}
	}

	captureIDs := make([]model.CaptureID, 0, len(captures))
	for captureID := range captures {
		captureIDs = append(captureIDs, captureID)
	}
	// TODO support table re-balance when adding a new capture.
	tasks := make([]*scheduleTask, 0)
	if len(newTables) != 0 {
		tasks = append(
			tasks, newBurstBalanceAddTables(checkpointTs, newTables, captureIDs))
		if len(newTables) == len(currentTables) {
			return tasks
		}
	}
	if len(rmTables) > 0 {
		tasks = append(
			tasks, newBurstBalanceRemoveTables(checkpointTs, rmTables, replications))
	}

	return tasks
}

func newBurstBalanceAddTables(
	checkpointTs model.Ts, newTables []model.TableID, captureIDs []model.CaptureID,
) *scheduleTask {
	idx := 0
	tables := make(map[model.TableID]model.CaptureID)
	for _, tableID := range newTables {
		tables[tableID] = captureIDs[idx]
		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return &scheduleTask{burstBalance: &burstBalance{
		AddTables:    tables,
		CheckpointTs: checkpointTs,
	}}
}

func newBurstBalanceRemoveTables(
	checkpointTs model.Ts, rmTables []model.TableID, replications map[model.TableID]*ReplicationSet,
) *scheduleTask {
	tables := make(map[model.TableID]model.CaptureID)
	for _, tableID := range rmTables {
		rep := replications[tableID]
		if rep.Primary != "" {
			tables[tableID] = rep.Primary
		} else if rep.Secondary != "" {
			tables[tableID] = rep.Secondary
		} else {
			log.Warn("tpscheduler: primary or secondary not found for removed table",
				zap.Any("table", rep))
		}
	}
	return &scheduleTask{burstBalance: &burstBalance{
		RemoveTables: tables,
		CheckpointTs: checkpointTs,
	}}
}
