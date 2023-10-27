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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
)

type scheduler interface {
	Name() string
	Schedule(
		checkpointTs model.Ts,
		currentSpans []tablepb.Span,
		aliveCaptures map[model.CaptureID]*member.CaptureStatus,
		replications *spanz.BtreeMap[*replication.ReplicationSet],
	) []*replication.ScheduleTask
}

// schedulerPriority is the priority of each scheduler.
// Lower value has higher priority.
type schedulerPriority int

const (
	// schedulerPriorityBasic has the highest priority.
	schedulerPriorityBasic schedulerPriority = iota
	// schedulerPriorityDrainCapture has higher priority than other schedulers.
	schedulerPriorityDrainCapture
	schedulerPriorityMoveTable
	schedulerPriorityRebalance
	schedulerPriorityBalance
	schedulerPriorityMax
)
