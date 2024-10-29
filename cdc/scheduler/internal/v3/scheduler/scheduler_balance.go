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
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

var _ scheduler = &balanceScheduler{}

// The scheduler for balancing tables among all captures.
type balanceScheduler struct {
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool

	maxTaskConcurrency int
	changefeedID       model.ChangeFeedID
}

func newBalanceScheduler(interval time.Duration, concurrency int, changefeedID model.ChangeFeedID) *balanceScheduler {
	return &balanceScheduler{
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: interval,
		maxTaskConcurrency:   concurrency,
		changefeedID:         changefeedID,
	}
}

func (b *balanceScheduler) Name() string {
	return "balance-scheduler"
}

func (b *balanceScheduler) Schedule(
	_ model.Ts,
	_ []tablepb.Span,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.BtreeMap[*replication.ReplicationSet],
) []*replication.ScheduleTask {
	if !b.forceBalance {
		now := time.Now()
		if now.Sub(b.lastRebalanceTime) < b.checkBalanceInterval {
			// skip balance.
			return nil
		}
		b.lastRebalanceTime = now
	}

	for _, capture := range captures {
		if capture.State == member.CaptureStateStopping {
			log.Debug("schedulerv3: capture is stopping, premature to balance table",
				zap.String("namespace", b.changefeedID.Namespace),
				zap.String("changefeed", b.changefeedID.ID))
			return nil
		}
	}

	tasks := buildBalanceMoveTables(
		b.random, captures, replications, b.maxTaskConcurrency, b.changefeedID)
	b.forceBalance = len(tasks) != 0
	return tasks
}

func buildBalanceMoveTables(
	random *rand.Rand,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.BtreeMap[*replication.ReplicationSet],
	maxTaskConcurrency int,
	changeFeedID model.ChangeFeedID,
) []*replication.ScheduleTask {
	moves := newBalanceMoveTables(
		random, captures, replications, maxTaskConcurrency, changeFeedID)
	tasks := make([]*replication.ScheduleTask, 0, len(moves))
	for i := 0; i < len(moves); i++ {
		// No need for accept callback here.
		tasks = append(tasks, &replication.ScheduleTask{MoveTable: &moves[i]})
	}
	return tasks
}
