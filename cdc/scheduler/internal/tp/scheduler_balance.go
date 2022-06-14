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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

var _ scheduler = &balanceScheduler{}

// The scheduler for balancing tables of each captures.
type balanceScheduler struct {
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
}

func newBalanceScheduler(cfg *config.SchedulerConfig) *balanceScheduler {
	return &balanceScheduler{
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: time.Duration(cfg.CheckBalanceInterval),
	}
}

func (b *balanceScheduler) Name() string {
	return string(schedulerTypeBalance)
}

func (b *balanceScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
) []*scheduleTask {
	now := time.Now()
	if now.Sub(b.lastRebalanceTime) < b.checkBalanceInterval {
		return nil
	}
	b.lastRebalanceTime = now

	tasks := make([]*scheduleTask, 0)
	task := buildBurstBalanceMoveTables(b.random, currentTables, captures, replications)
	if task != nil {
		tasks = append(tasks, task)
	}
	return tasks
}

func buildBurstBalanceMoveTables(
	random *rand.Rand,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
) *scheduleTask {
	captureTables := make(map[model.CaptureID][]model.TableID)
	for _, tableID := range currentTables {
		rep, ok := replications[tableID]
		if !ok {
			continue
		}
		if rep.Primary != "" {
			captureTables[rep.Primary] = append(captureTables[rep.Primary], tableID)
		}
		if rep.Secondary != "" {
			captureTables[rep.Secondary] = append(captureTables[rep.Secondary], tableID)
		}
	}

	// We does not need accept callback here.
	accept := (callback)(nil)
	return newBurstBalanceMoveTables(accept, random, captures, replications)
}
