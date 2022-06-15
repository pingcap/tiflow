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
	"sync"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
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

	accept := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.target = captureIDNotDraining
	}

	otherCaptures := make(map[model.CaptureID]*model.CaptureInfo)
	for id, info := range captures {
		if id != d.target {
			otherCaptures[id] = info
		}
	}

	task := newBurstBalanceMoveTables(accept, d.random, captures, replications)
	if task == nil {
		return nil
	}
	return []*scheduleTask{task}
}
