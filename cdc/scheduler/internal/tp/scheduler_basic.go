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
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ scheduler = &basicScheduler{}

// The basic scheduler for adding and removing tables, it tries to keep
// every table get replicated.
//
// It handles the following scenario:
// 1. Initial table dispatch.
// 2. DDL CREATE/DROP/TRUNCATE TABLE
// 3. Capture offline.
type basicScheduler struct {
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
}

func newBasicScheduler() *basicScheduler {
	return &basicScheduler{
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (b *basicScheduler) Name() string {
	return "basic-scheduler"
}

func (b *basicScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
	_ bool,
) []*scheduleTask {
	tasks := make([]*scheduleTask, 0)
	tablesLenEqual := len(currentTables) == len(replications)
	tablesAllFind := true
	newTables := make([]model.TableID, 0)
	for _, tableID := range currentTables {
		rep, ok := replications[tableID]
		if !ok {
			newTables = append(newTables, tableID)
			// The table ID is not in the replication means the two sets are
			// not identical.
			tablesAllFind = false
			continue
		}
		if rep.State == ReplicationSetStateAbsent {
			newTables = append(newTables, tableID)
		}
	}

	// Build add table tasks.
	if len(newTables) > 0 {
		captureIDs := make([]model.CaptureID, 0, len(captures))
		for captureID := range captures {
			captureIDs = append(captureIDs, captureID)
		}
		const logTableIDThreshold = 50
		tableField := zap.Skip()
		if len(newTables) < logTableIDThreshold {
			tableField = zap.Int64s("tableIDs", newTables)
		}
		log.Info("tpscheduler: burst add table",
			tableField, zap.Strings("captureIDs", captureIDs))
		tasks = append(
			tasks, newBurstBalanceAddTables(checkpointTs, newTables, captureIDs))
		if len(newTables) == len(currentTables) {
			// The initial balance, if new tables and current tables are equal.
			return tasks
		}
	}

	// Build remove table tasks.
	// For most of the time, remove tables are unlikely to happen.
	//
	// Fast path for check whether two sets are identical:
	// If the length of currentTables and replications are equal,
	// and for all tables in currentTables have a record in replications.
	if !tablesLenEqual || !tablesAllFind {
		// The two sets are not identical. We need to find removed tables.
		intersectionTable := make(map[model.TableID]struct{}, len(currentTables))
		for _, tableID := range currentTables {
			_, ok := replications[tableID]
			if !ok {
				continue
			}
			intersectionTable[tableID] = struct{}{}
		}
		rmTables := make([]model.TableID, 0)
		for tableID := range replications {
			_, ok := intersectionTable[tableID]
			if !ok {
				rmTables = append(rmTables, tableID)
			}
		}
		if len(rmTables) > 0 {
			tasks = append(tasks, newBurstBalanceRemoveTables(rmTables, replications))
		}
	}
	return tasks
}

// newBurstBalanceAddTables add each new table to captures in a round-robin way.
func newBurstBalanceAddTables(
	checkpointTs model.Ts, newTables []model.TableID, captureIDs []model.CaptureID,
) *scheduleTask {
	idx := 0
	tables := make([]addTable, 0, len(newTables))
	for _, tableID := range newTables {
		tables = append(tables, addTable{
			TableID:      tableID,
			CaptureID:    captureIDs[idx],
			CheckpointTs: checkpointTs,
		})
		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return &scheduleTask{burstBalance: &burstBalance{
		AddTables: tables,
	}}
}

func newBurstBalanceRemoveTables(
	rmTables []model.TableID, replications map[model.TableID]*ReplicationSet,
) *scheduleTask {
	tables := make([]removeTable, 0, len(rmTables))
	for _, tableID := range rmTables {
		rep := replications[tableID]
		var captureID model.CaptureID
		if rep.Primary != "" {
			captureID = rep.Primary
		} else if rep.Secondary != "" {
			captureID = rep.Secondary
		} else {
			log.Warn("tpscheduler: primary or secondary not found for removed table",
				zap.Any("table", rep))
			continue
		}
		tables = append(tables, removeTable{
			TableID:   tableID,
			CaptureID: captureID,
		})
	}
	return &scheduleTask{burstBalance: &burstBalance{
		RemoveTables: tables,
	}}
}
