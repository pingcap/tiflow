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

package v3

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
)

var _ internal.InfoProvider = (*coordinator)(nil)

// GetTaskStatuses returns the task statuses.
func (c *coordinator) GetTaskStatuses() (map[model.CaptureID]*model.TaskStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks := make(map[model.CaptureID]*model.TaskStatus, len(c.captureM.Captures))
	for captureID, status := range c.captureM.Captures {
		taskStatus := &model.TaskStatus{
			Tables: make(map[model.TableID]*model.TableReplicaInfo),
		}
		for _, s := range status.Tables {
			taskStatus.Tables[s.TableID] = &model.TableReplicaInfo{
				StartTs: s.Checkpoint.CheckpointTs,
			}
		}
		tasks[captureID] = taskStatus
	}
	return tasks, nil
}

// GetTaskPositions returns the task positions.
func (c *coordinator) GetTaskPositions() (map[model.CaptureID]*model.TaskPosition, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	p := &model.TaskPosition{}
	pos := make(map[model.CaptureID]*model.TaskPosition, len(c.captureM.Captures))
	for captureID := range c.captureM.Captures {
		pos[captureID] = p
	}
	return pos, nil
}

// GetTotalTableCounts returns the number of tables associated
// with each capture.
func (c *coordinator) GetTotalTableCounts() map[model.CaptureID]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	tables := make(map[model.CaptureID]int, len(c.captureM.Captures))
	for captureID, status := range c.captureM.Captures {
		tables[captureID] = len(status.Tables)
	}
	return tables
}

// GetPendingTableCounts returns the number of tables in a non-ready
// status (Adding & Removing) associated with each capture.
func (c *coordinator) GetPendingTableCounts() map[model.CaptureID]int {
	return make(map[model.CaptureID]int)
}
