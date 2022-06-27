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

// captureIDNotDraining is the default capture ID if the drain target not set
const captureIDNotDraining = ""

var _ scheduler = &drainCaptureScheduler{}

type drainCaptureScheduler struct {
	mu     sync.Mutex
	target model.CaptureID
	random *rand.Rand

	maxTaskConcurrency int
}

func newDrainCaptureScheduler(concurrency int) *drainCaptureScheduler {
	return &drainCaptureScheduler{
		target:             captureIDNotDraining,
		random:             rand.New(rand.NewSource(time.Now().UnixNano())),
		maxTaskConcurrency: concurrency,
	}
}

func (d *drainCaptureScheduler) Name() string {
	return "drain-capture-scheduler"
}

func (d *drainCaptureScheduler) getTarget() model.CaptureID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.target
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
	_ model.Ts,
	_ []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
	_ bool,
) []*scheduleTask {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.target == captureIDNotDraining {
		return nil
	}

	var availableCaptureCount int
	for id := range captures {
		if id != d.target {
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
			log.Debug("tpscheduler: drain capture scheduler skip this tick,"+
				"not all table is replicating",
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

	// this always indicate that the whole draining process finished, and can be triggered by:
	// 1. the target capture has no table at the beginning
	// 2. all tables moved from the target capture
	// 3. the target capture cannot be found in the latest captures
	if len(victims) == 0 {
		log.Info("tpscheduler: drain capture scheduler finished, since no table",
			zap.String("target", d.target), zap.Any("captures", captures))
		d.target = captureIDNotDraining
		return nil
	}

	for captureID, w := range captureWorkload {
		captureWorkload[captureID] = randomizeWorkload(d.random, w)
	}

	maxTaskConcurrency := d.maxTaskConcurrency
	if len(victims) < maxTaskConcurrency {
		maxTaskConcurrency = len(victims)
	}

	// for each victim table, find the target for it
	result := make([]*scheduleTask, 0, maxTaskConcurrency)
	for i := 0; i < maxTaskConcurrency; i++ {
		tableID := victims[i]
		target := ""
		minWorkload := math.MaxInt64

		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("tpscheduler: drain capture meet unexpected min workload " +
				"when try to the the target capture")
		}

		result = append(result, &scheduleTask{
			moveTable: &moveTable{
				TableID:     tableID,
				DestCapture: target,
			},
			accept: (callback)(nil), // We do not need to accept callback here.
		})

		captureWorkload[target] = randomizeWorkload(d.random, minWorkload+1)
	}

	return result
}
