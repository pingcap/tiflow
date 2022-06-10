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

package metadata

import (
	"context"

	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

// TaskStage represents internal stage of a task
// TODO: use Stage in lib or move Stage to lib.
type TaskStage int

// These stages may updated in later pr.
const (
	StageInit TaskStage = iota + 1
	StageRunning
	StagePaused
	StageFinished
	StageError
	StagePausing
	// UnScheduled means the task is not scheduled.
	// This usually happens when the worker is offline.
	StageUnscheduled
)

// Job represents the state of a job.
type Job struct {
	State

	// taskID -> task
	Tasks map[string]*Task
}

// NewJob creates a new Job instance
func NewJob(jobCfg *config.JobCfg) *Job {
	taskCfgs := jobCfg.ToTaskCfgs()
	job := &Job{
		Tasks: make(map[string]*Task, len(taskCfgs)),
	}

	for taskID, taskCfg := range taskCfgs {
		job.Tasks[taskID] = NewTask(taskCfg)
	}
	return job
}

// Task is the minimum working unit of a job.
// A job may contain multiple upstream and it will be converted into multiple tasks.
type Task struct {
	Cfg   *config.TaskCfg
	Stage TaskStage
}

// NewTask creates a new Task instance
func NewTask(taskCfg *config.TaskCfg) *Task {
	return &Task{
		Cfg:   taskCfg,
		Stage: StageRunning, // TODO: support set stage when create task.
	}
}

// JobStore manages the state of a job.
type JobStore struct {
	*TomlStore

	id libModel.MasterID
}

// NewJobStore creates a new JobStore instance
func NewJobStore(id libModel.MasterID, kvClient metaclient.KVClient) *JobStore {
	jobStore := &JobStore{
		TomlStore: NewTomlStore(kvClient),
		id:        id,
	}
	jobStore.TomlStore.Store = jobStore
	return jobStore
}

// CreateState returns an empty Job object
func (jobStore *JobStore) CreateState() State {
	return &Job{}
}

// Key returns encoded key for job store id
func (jobStore *JobStore) Key() string {
	return adapter.DMJobKeyAdapter.Encode(jobStore.id)
}

// UpdateStages will be called if user operate job.
func (jobStore *JobStore) UpdateStages(ctx context.Context, taskIDs []string, stage TaskStage) error {
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	job := state.(*Job)
	if len(taskIDs) == 0 {
		for task := range job.Tasks {
			taskIDs = append(taskIDs, task)
		}
	}
	for _, taskID := range taskIDs {
		if _, ok := job.Tasks[taskID]; !ok {
			return errors.Errorf("task %s not found", taskID)
		}
		t := job.Tasks[taskID]
		t.Stage = stage
	}

	return jobStore.Put(ctx, job)
}
