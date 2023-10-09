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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
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
	currentSpans []tablepb.Span,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.BtreeMap[*replication.ReplicationSet],
) []*replication.ScheduleTask {
	tasks := make([]*replication.ScheduleTask, 0)
	tablesLenEqual := len(currentSpans) == replications.Len()
	tablesAllFind := true
	newSpans := make([]tablepb.Span, 0)
	for _, span := range currentSpans {
		if len(newSpans) >= b.batchSize {
			break
		}
		rep, ok := replications.Get(span)
		if !ok {
			newSpans = append(newSpans, span)
			// The table ID is not in the replication means the two sets are
			// not identical.
			tablesAllFind = false
			continue
		}
		if rep.State == replication.ReplicationSetStateAbsent {
			newSpans = append(newSpans, span)
		}
	}

	// Build add table tasks.
	if len(newSpans) > 0 {
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
		tasks = append(
			tasks, newBurstAddTables(b.changefeedID, checkpointTs, newSpans, captureIDs))
	}

	// Build remove table tasks.
	// For most of the time, remove tables are unlikely to happen.
	//
	// Fast path for check whether two sets are identical:
	// If the length of currentTables and replications are equal,
	// and for all tables in currentTables have a record in replications.
	if !(tablesLenEqual && tablesAllFind) {
		// The two sets are not identical. We need to find removed tables.
		intersectionTable := spanz.NewBtreeMap[struct{}]()
		for _, span := range currentSpans {
			_, ok := replications.Get(span)
			if ok {
				intersectionTable.ReplaceOrInsert(span, struct{}{})
			}
		}
		rmSpans := make([]tablepb.Span, 0)
		replications.Ascend(func(span tablepb.Span, value *replication.ReplicationSet) bool {
			ok := intersectionTable.Has(span)
			if !ok {
				rmSpans = append(rmSpans, span)
			}
			return true
		})

		removeTableTasks := newBurstRemoveTables(rmSpans, replications, b.changefeedID)
		if removeTableTasks != nil {
			tasks = append(tasks, removeTableTasks)
		}
	}
	return tasks
}

// newBurstAddTables add each new table to captures in a round-robin way.
func newBurstAddTables(
	changefeedID model.ChangeFeedID,
	checkpointTs model.Ts, newSpans []tablepb.Span, captureIDs []model.CaptureID,
) *replication.ScheduleTask {
	idx := 0
	tables := make([]replication.AddTable, 0, len(newSpans))
	for _, span := range newSpans {
		targetCapture := captureIDs[idx]
		tables = append(tables, replication.AddTable{
			Span:         span,
			CaptureID:    targetCapture,
			CheckpointTs: checkpointTs,
		})
		log.Info("schedulerv3: burst add table",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("captureID", targetCapture),
			zap.Any("tableID", span.TableID))

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
	rmSpans []tablepb.Span, replications *spanz.BtreeMap[*replication.ReplicationSet],
	changefeedID model.ChangeFeedID,
) *replication.ScheduleTask {
	tables := make([]replication.RemoveTable, 0, len(rmSpans))
	for _, span := range rmSpans {
		rep := replications.GetV(span)
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
			Span:      span,
			CaptureID: captureID,
		})
		log.Info("schedulerv3: burst remove table",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("captureID", captureID),
			zap.Any("tableID", span.TableID))
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
