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

type scheduler interface {
	Name() string
	Schedule(
		checkpointTs model.Ts,
		currentTables []model.TableID,
		aliveCaptures map[model.CaptureID]*model.CaptureInfo,
		replications map[model.TableID]*ReplicationSet,
	) []*scheduleTask
}

type schedulerType int

const (
	schedulerTypeBasic        schedulerType = 0
	schedulerTypeBalance      schedulerType = 1
	schedulerTypeMoveTable    schedulerType = 2
	schedulerTypeRebalance    schedulerType = 3
	schedulerTypeDrainCapture schedulerType = 4
)

var schedulerTypeName = map[int]string{
	0: "basic-scheduler",
	1: "balance-scheduler",
	2: "move-table-scheduler",
	3: "rebalance-scheduler",
	4: "drain-capture-scheduler",
}

func (s schedulerType) String() string {
	return schedulerTypeName[int(s)]
}
