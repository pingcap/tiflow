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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
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
	batchSize    int
	changefeedID model.ChangeFeedID
}

func newBasicScheduler(batchSize int, changefeed model.ChangeFeedID) *basicScheduler {
	return &basicScheduler{
		batchSize:    batchSize,
		changefeedID: changefeed,
	}
}

func (b *basicScheduler) Name() string {
	return "basic-scheduler"
}

func (b *basicScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications map[model.TableID]*replication.ReplicationSet,
) []*replication.ScheduleTask {
	tasks := make([]*replication.ScheduleTask, 0)
	tablesLenEqual := len(currentTables) == len(replications)
	tablesAllFind := true
	newTables := make([]model.TableID, 0)
	for _, tableID := range currentTables {
		if len(newTables) >= b.batchSize {
			break
		}
		rep, ok := replications[tableID]
		if !ok {
			newTables = append(newTables, tableID)
			// The table ID is not in the replication means the two sets are
			// not identical.
			tablesAllFind = false
			continue
		}
		if rep.State == replication.ReplicationSetStateAbsent {
			newTables = append(newTables, tableID)
		}
	}

	// Build add table tasks.
	if len(newTables) > 0 {
		captureIDs := make([]model.CaptureID, 0, len(captures))
		for captureID, status := range captures {
			if status.State == member.CaptureStateStopping {
				log.Warn("schedulerv3: capture is stopping, "+
					"skip the capture when add new table",
					zap.String("namespace", b.changefeedID.Namespace),
					zap.String("changefeed", b.changefeedID.ID),
					zap.Any("captureStatus", status))
				continue
			}
			captureIDs = append(captureIDs, captureID)
		}

		if len(captureIDs) == 0 {
			// this should never happen, if no capture can be found
			// the changefeed cannot make progress
			// for a cluster with n captures, n should be at least 2
			// only n - 1 captures can be in the `stopping` at the same time.
			log.Warn("schedulerv3: cannot found capture when add new table",
				zap.String("namespace", b.changefeedID.Namespace),
				zap.String("changefeed", b.changefeedID.ID),
				zap.Any("allCaptureStatus", captures))
			return tasks
		}
		log.Info("schedulerv3: burst add table",
			zap.String("namespace", b.changefeedID.Namespace),
			zap.String("changefeed", b.changefeedID.ID),
			zap.Strings("captureIDs", captureIDs),
			zap.Int64s("tableIDs", newTables))
		tasks = append(
			tasks, newBurstAddTables(b.changefeedID, checkpointTs, newTables, captureIDs))
	}

	// Build remove table tasks.
	// For most of the time, remove tables are unlikely to happen.
	//
	// Fast path for check whether two sets are identical:
	// If the length of currentTables and replications are equal,
	// and for all tables in currentTables have a record in replications.
	if !(tablesLenEqual && tablesAllFind) {
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

		removeTableTasks := newBurstRemoveTables(rmTables, replications, b.changefeedID)
		if removeTableTasks != nil {
			tasks = append(tasks, removeTableTasks)
		}
	}
	return tasks
}

// newBurstAddTables add each new table to captures in a round-robin way.
func newBurstAddTables(
	changefeedID model.ChangeFeedID,
	checkpointTs model.Ts, newTables []model.TableID, captureIDs []model.CaptureID,
) *replication.ScheduleTask {
	idx := 0
	tables := make([]replication.AddTable, 0, len(newTables))
	for _, tableID := range newTables {
		targetCapture := captureIDs[idx]
		tables = append(tables, replication.AddTable{
			TableID:      tableID,
			CaptureID:    targetCapture,
			CheckpointTs: checkpointTs,
		})
		log.Info("schedulerv3: burst add table",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("captureID", targetCapture),
			zap.Any("tableID", tableID))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return &replication.ScheduleTask{
		BurstBalance: &replication.BurstBalance{
			AddTables: tables,
		},
	}
}

func newBurstRemoveTables(
	rmTables []model.TableID, replications map[model.TableID]*replication.ReplicationSet,
	changefeedID model.ChangeFeedID,
) *replication.ScheduleTask {
	tables := make([]replication.RemoveTable, 0, len(rmTables))
	for _, tableID := range rmTables {
		rep := replications[tableID]
		var captureID model.CaptureID
		for id := range rep.Captures {
			captureID = id
			break
		}
		if captureID == "" {
			log.Warn("schedulerv3: primary or secondary not found for removed table,"+
				"this may happen if the capture shutdown",
				zap.String("namespace", changefeedID.Namespace),
				zap.String("changefeed", changefeedID.ID),
				zap.Any("table", rep))
			continue
		}
		tables = append(tables, replication.RemoveTable{
			TableID:   tableID,
			CaptureID: captureID,
		})
		log.Info("schedulerv3: burst remove table",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("captureID", captureID),
			zap.Any("tableID", tableID))
	}

	if len(tables) == 0 {
		return nil
	}

	return &replication.ScheduleTask{
		BurstBalance: &replication.BurstBalance{
			RemoveTables: tables,
		},
	}
}
