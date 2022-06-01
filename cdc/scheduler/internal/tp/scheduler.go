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
		aliveCaptures map[model.CaptureID]*CaptureStatus,
		replications map[model.TableID]*ReplicationSet,
	) []*scheduleTask
}

type schedulerType string

const (
	schedulerTypeBurstBalance schedulerType = "burst-balance-scheduler"
	schedulerTypeMoveTable    schedulerType = "move-table-scheduler"
	schedulerTypeRebalance    schedulerType = "rebalance-scheduler"
)
