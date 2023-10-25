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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

var _ scheduler = &moveTableScheduler{}

type moveTableScheduler struct {
	mu    sync.Mutex
	tasks *spanz.BtreeMap[*replication.ScheduleTask]

	changefeedID model.ChangeFeedID
}

func newMoveTableScheduler(changefeed model.ChangeFeedID) *moveTableScheduler {
	return &moveTableScheduler{
		tasks:        spanz.NewBtreeMap[*replication.ScheduleTask](),
		changefeedID: changefeed,
	}
}

func (m *moveTableScheduler) Name() string {
	return "move-table-scheduler"
}

func (m *moveTableScheduler) addTask(span tablepb.Span, target model.CaptureID) bool {
	// previous triggered task not accepted yet, decline the new manual move table request.
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok := m.tasks.Has(span); ok {
		return false
	}
	m.tasks.ReplaceOrInsert(span, &replication.ScheduleTask{
		MoveTable: &replication.MoveTable{
			Span:        span,
			DestCapture: target,
		},
		Accept: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.tasks.Delete(span)
		},
	})
	return true
}

func (m *moveTableScheduler) Schedule(
	_ model.Ts,
	currentSpans []tablepb.Span,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.BtreeMap[*replication.ReplicationSet],
) []*replication.ScheduleTask {
	m.mu.Lock()
	defer m.mu.Unlock()

	// FIXME: moveTableScheduler is broken in the sense of range level replication.
	// It is impossible for users to pass valid start key and end key.

	result := make([]*replication.ScheduleTask, 0)

	if m.tasks.Len() == 0 {
		return result
	}

	if len(captures) == 0 {
		return result
	}

	allSpans := spanz.NewSet()
	for _, span := range currentSpans {
		allSpans.Add(span)
	}

	toBeDeleted := []tablepb.Span{}
	m.tasks.Ascend(func(span tablepb.Span, task *replication.ScheduleTask) bool {
		// table may not in the all current tables
		// if it was removed after manual move table triggered.
		if !allSpans.Contain(span) {
			log.Warn("schedulerv3: move table ignored, since the table cannot found",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.String("captureID", task.MoveTable.DestCapture))
			toBeDeleted = append(toBeDeleted, span)
			return true
		}

		// the target capture may offline after manual move table triggered.
		status, ok := captures[task.MoveTable.DestCapture]
		if !ok {
			log.Info("schedulerv3: move table ignored, since the target capture cannot found",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.String("captureID", task.MoveTable.DestCapture))
			toBeDeleted = append(toBeDeleted, span)
			return true
		}
		if status.State != member.CaptureStateInitialized {
			log.Warn("schedulerv3: move table ignored, target capture is not initialized",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.String("captureID", task.MoveTable.DestCapture),
				zap.Any("state", status.State))
			toBeDeleted = append(toBeDeleted, span)
			return true
		}

		rep, ok := replications.Get(span)
		if !ok {
			log.Warn("schedulerv3: move table ignored, table not found in the replication set",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.String("captureID", task.MoveTable.DestCapture))
			toBeDeleted = append(toBeDeleted, span)
			return true
		}
		// only move replicating table.
		if rep.State != replication.ReplicationSetStateReplicating {
			log.Info("schedulerv3: move table ignored, since the table is not replicating now",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("span", span.String()),
				zap.String("captureID", task.MoveTable.DestCapture),
				zap.Any("replicationState", rep.State))
			toBeDeleted = append(toBeDeleted, span)
		}
		return true
	})
	for _, span := range toBeDeleted {
		m.tasks.Delete(span)
	}

	m.tasks.Ascend(func(span tablepb.Span, value *replication.ScheduleTask) bool {
		result = append(result, value)
		return true
	})

	return result
}
