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
	"github.com/pingcap/tiflow/cdc/model"
)

var _ scheduler = &rebalanceScheduler{}

type rebalanceScheduler struct {
	rebalance bool
}

func newRebalanceScheduler() *rebalanceScheduler {
	return &rebalanceScheduler{
		rebalance: false,
	}
}

func (b *rebalanceScheduler) Name() string {
	return string(schedulerTypeRebalance)
}

func (b *rebalanceScheduler) trigger() {
	b.rebalance = true
}

func (b *rebalanceScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*CaptureStatus,
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
		tasks = append(tasks, newBurstBalanceAddTables(newTables, captureIDs))
		if len(newTables) == len(currentTables) {
			return tasks
		}
	}
	if len(rmTables) > 0 {
		tasks = append(tasks, newBurstBalanceRemoveTables(rmTables, replications))
	}

	return tasks
}
