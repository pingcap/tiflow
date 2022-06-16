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
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const captureIDNotDraining = ""

var _ scheduler = &drainCaptureScheduler{}

type drainCaptureScheduler struct {
	mu     sync.Mutex
	target model.CaptureID
	random *rand.Rand
}

func newDrainCaptureScheduler() *drainCaptureScheduler {
	return &drainCaptureScheduler{
		target: captureIDNotDraining,
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (d *drainCaptureScheduler) Name() string {
	return string(schedulerTypeDrainCapture)
}

func (d *drainCaptureScheduler) setTarget(target model.CaptureID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.target != captureIDNotDraining {
		return false
	}

	d.target = target
	return true
}

func (d *drainCaptureScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
) []*scheduleTask {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.target == captureIDNotDraining {
		return nil
	}

	var availableCaptureCount int
	for id := range captures {
		if id == d.target {
			availableCaptureCount++
		}
	}

	// this may happen when inject the target, there is at least 2 alive captures
	// but when schedule the task, only owner alive.
	if availableCaptureCount == 0 {
		log.Warn("tpscheduler: drain capture scheduler ignore drain target capture, "+
			"since cannot found destination captures",
			zap.String("target", d.target), zap.Any("captures", captures))
		d.target = captureIDNotDraining
		return nil
	}

	// victims record all table instance should be dropped from the target capture
	victims := make([]model.TableID, 0)
	captureWorkload := make(map[model.CaptureID]int)
	for tableID, rep := range replications {
		if rep.State != ReplicationSetStateReplicating {
			// only drain the target capture if all tables is replicating,
			log.Warn("tpscheduler: drain capture scheduler skip this tick,"+
				"since it's secondary to some table",
				zap.String("target", d.target),
				zap.Any("replication", rep))
			return nil
		}

		if rep.Primary == d.target {
			victims = append(victims, tableID)
		}

		// only calculate workload of other captures not the drain target.
		if rep.Primary != d.target {
			captureWorkload[rep.Primary] += 1
		}
	}

	if len(victims) == 0 {
		d.target = captureIDNotDraining
		return nil
	}

	for captureID, w := range captureWorkload {
		captureWorkload[captureID] = randomizeWorkload(d.random, w)
	}

	// for each victim table, find the target for it
	moveTables := make([]moveTable, 0, len(victims))
	for _, tableID := range victims {
		target := ""
		minWorkload := math.MaxInt64

		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("tpscheduler: rebalance meet unexpected min workload " +
				"when try to the the target capture")
		}

		moveTables = append(moveTables, moveTable{
			TableID:     tableID,
			DestCapture: target,
		})

		captureWorkload[target] = randomizeWorkload(d.random, minWorkload+1)
	}

	// todo: accept should be called, after all tables moved to other captures,
	// make sure this is guaranteed by the scheduler.
	accept := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		// todo: before all tables removed from the target capture, this should always be set.
		d.target = captureIDNotDraining
	}

	// todo: return a burst balance at the moment, adjust this if concurrency control necessary.
	task := &scheduleTask{
		burstBalance: &burstBalance{MoveTables: moveTables},
		accept:       accept,
	}

	return []*scheduleTask{task}
}
