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
	"math"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"go.uber.org/zap"
)

// captureIDNotDraining is the default capture ID if the drain target not set
const captureIDNotDraining = ""

var _ scheduler = &drainCaptureScheduler{}

type drainCaptureScheduler struct {
	mu     sync.Mutex
	target model.CaptureID

	changefeedID       model.ChangeFeedID
	maxTaskConcurrency int
}

func newDrainCaptureScheduler(
	concurrency int, changefeed model.ChangeFeedID,
) *drainCaptureScheduler {
	return &drainCaptureScheduler{
		target:             captureIDNotDraining,
		maxTaskConcurrency: concurrency,
		changefeedID:       changefeed,
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
	captures map[model.CaptureID]*member.CaptureStatus,
	replications map[model.TableID]*replication.ReplicationSet,
) []*replication.ScheduleTask {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.target == captureIDNotDraining {
		// There are two ways to make a capture "stopping",
		// 1. PUT /api/v1/capture/drain
		// 2. kill <TiCDC_PID>
		for id, capture := range captures {
			if capture.IsOwner {
				// Skip draining owner.
				continue
			}
			if capture.State == member.CaptureStateStopping {
				d.target = id
				break
			}
		}

		if d.target == captureIDNotDraining {
			return nil
		}

		log.Info("schedulerv3: drain a stopping capture",
			zap.String("namespace", d.changefeedID.Namespace),
			zap.String("changefeed", d.changefeedID.ID),
			zap.String("captureID", d.target))
	}

	// Currently, the workload is the number of tables in a capture.
	captureWorkload := make(map[model.CaptureID]int)
	for id := range captures {
		if id != d.target {
			captureWorkload[id] = 0
		}
	}

	// this may happen when inject the target, there is at least 2 alive captures
	// but when schedule the task, only owner alive.
	if len(captureWorkload) == 0 {
		log.Warn("schedulerv3: drain capture scheduler ignore drain target capture, "+
			"since cannot found destination captures",
			zap.String("namespace", d.changefeedID.Namespace),
			zap.String("changefeed", d.changefeedID.ID),
			zap.String("target", d.target), zap.Any("captures", captures))
		d.target = captureIDNotDraining
		return nil
	}

	maxTaskConcurrency := d.maxTaskConcurrency
	// victimTables record tables should be moved out from the target capture
	victimTables := make([]model.TableID, 0, maxTaskConcurrency)
	for tableID, rep := range replications {
		if rep.State != replication.ReplicationSetStateReplicating {
			// only drain the target capture if all tables is replicating,
			log.Debug("schedulerv3: drain capture scheduler skip this tick,"+
				"not all table is replicating",
				zap.String("namespace", d.changefeedID.Namespace),
				zap.String("changefeed", d.changefeedID.ID),
				zap.String("target", d.target),
				zap.Any("replication", rep))
			return nil
		}

		if rep.Primary == d.target {
			if len(victimTables) < maxTaskConcurrency {
				victimTables = append(victimTables, tableID)
			}
		}

		// only calculate workload of other captures not the drain target.
		if rep.Primary != d.target {
			captureWorkload[rep.Primary]++
		}
	}

	// this always indicate that the whole draining process finished, and can be triggered by:
	// 1. the target capture has no table at the beginning
	// 2. all tables moved from the target capture
	// 3. the target capture cannot be found in the latest captures
	if len(victimTables) == 0 {
		log.Info("schedulerv3: drain capture scheduler finished, since no table",
			zap.String("namespace", d.changefeedID.Namespace),
			zap.String("changefeed", d.changefeedID.ID),
			zap.String("target", d.target))
		d.target = captureIDNotDraining
		return nil
	}

	// For each victim table, find the target for it
	result := make([]*replication.ScheduleTask, 0, maxTaskConcurrency)
	for _, tableID := range victimTables {
		target := ""
		minWorkload := math.MaxInt64
		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("schedulerv3: drain capture meet unexpected min workload",
				zap.String("namespace", d.changefeedID.Namespace),
				zap.String("changefeed", d.changefeedID.ID),
				zap.Any("workload", captureWorkload))
		}

		result = append(result, &replication.ScheduleTask{
			MoveTable: &replication.MoveTable{
				TableID:     tableID,
				DestCapture: target,
			},
			Accept: (replication.Callback)(nil), // No need for accept callback here.
		})

		// Increase target workload to make sure tables are evenly distributed.
		captureWorkload[target]++
	}

	return result
}
