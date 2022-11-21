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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/bootstrap"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// TaskStage represents internal stage of a task
// TODO: use Stage in lib or move Stage to lib.
type TaskStage int

// These stages may be updated in later pr.
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

var typesStringify = [...]string{
	0:                "",
	StageInit:        "Initing",
	StageRunning:     "Running",
	StagePaused:      "Paused",
	StageFinished:    "Finished",
	StageError:       "Error",
	StagePausing:     "Pausing",
	StageUnscheduled: "Unscheduled",
}

var toTaskStage map[string]TaskStage

func init() {
	toTaskStage = make(map[string]TaskStage, len(typesStringify))
	for i, s := range typesStringify {
		toTaskStage[s] = TaskStage(i)
	}
}

// String implements fmt.Stringer interface
func (ts TaskStage) String() string {
	if int(ts) >= len(typesStringify) || ts < 0 {
		return fmt.Sprintf("Unknown TaskStage %d", ts)
	}
	return typesStringify[ts]
}

// MarshalJSON marshals the enum as a quoted json string
func (ts TaskStage) MarshalJSON() ([]byte, error) {
	return json.Marshal(ts.String())
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (ts *TaskStage) UnmarshalJSON(b []byte) error {
	var (
		j  string
		ok bool
	)
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*ts, ok = toTaskStage[j]
	if !ok {
		return errors.Errorf("Unknown TaskStage %s", j)
	}
	return nil
}

// Job represents the state of a job.
type Job struct {
	// taskID -> task
	Tasks map[string]*Task

	// Deleting represents whether the job is being deleted.
	Deleting bool
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
	Cfg              *config.TaskCfg
	Stage            TaskStage
	StageUpdatedTime time.Time
}

// NewTask creates a new Task instance
func NewTask(taskCfg *config.TaskCfg) *Task {
	return &Task{
		Cfg:              taskCfg,
		Stage:            StageRunning, // TODO: support set stage when create task.
		StageUpdatedTime: time.Now(),
	}
}

// JobStore manages the state of a job.
type JobStore struct {
	*frameworkMetaStore
	*bootstrap.DefaultUpgrader

	mu     sync.Mutex
	logger *zap.Logger
}

// NewJobStore creates a new JobStore instance
func NewJobStore(kvClient metaModel.KVClient, pLogger *zap.Logger) *JobStore {
	logger := pLogger.With(zap.String("component", "job_store"))
	jobStore := &JobStore{
		frameworkMetaStore: newTOMLFrameworkMetaStore(kvClient),
		DefaultUpgrader:    bootstrap.NewDefaultUpgrader(logger),
		logger:             logger,
	}
	jobStore.frameworkMetaStore.stateFactory = jobStore
	jobStore.DefaultUpgrader.Upgrader = jobStore
	return jobStore
}

// CreateState returns an empty Job object
func (jobStore *JobStore) createState() state {
	return &Job{}
}

// Key returns encoded key for job store
func (jobStore *JobStore) key() string {
	return adapter.DMJobKeyAdapter.Encode()
}

// UpdateStages will be called if user operate job.
func (jobStore *JobStore) UpdateStages(ctx context.Context, taskIDs []string, stage TaskStage) error {
	jobStore.mu.Lock()
	defer jobStore.mu.Unlock()
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	job := state.(*Job)
	if job.Deleting {
		return errors.New("failed to update stages because job is being deleted")
	}
	if len(taskIDs) == 0 {
		for task := range job.Tasks {
			taskIDs = append(taskIDs, task)
		}
	}
	for _, taskID := range taskIDs {
		t, ok := job.Tasks[taskID]
		if !ok {
			return errors.Errorf("task %s not found", taskID)
		}
		t.Stage = stage
		t.StageUpdatedTime = time.Now()
	}

	return jobStore.Put(ctx, job)
}

// UpdateConfig will be called if user update job config.
func (jobStore *JobStore) UpdateConfig(ctx context.Context, jobCfg *config.JobCfg) error {
	jobStore.mu.Lock()
	defer jobStore.mu.Unlock()
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	oldJob := state.(*Job)
	if oldJob.Deleting {
		return errors.New("failed to update config because job is being deleted")
	}

	// TODO: we may diff the config at task level in the future, that way different tasks will have different modify revisions.
	// so that changing the configuration of one task will not affect other tasks.
	var oldVersion uint64
	for _, task := range oldJob.Tasks {
		oldVersion = task.Cfg.ModRevision
		break
	}
	jobCfg.ModRevision = oldVersion + 1
	newJob := NewJob(jobCfg)

	for taskID, newTask := range newJob.Tasks {
		// task stage will not be updated.
		if oldTask, ok := oldJob.Tasks[taskID]; ok {
			newTask.Stage = oldTask.Stage
			newTask.StageUpdatedTime = oldTask.StageUpdatedTime
		}
	}

	return jobStore.Put(ctx, newJob)
}

// MarkDeleting marks the job as deleting.
func (jobStore *JobStore) MarkDeleting(ctx context.Context) error {
	jobStore.mu.Lock()
	defer jobStore.mu.Unlock()
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	job := state.(*Job)
	job.Deleting = true
	return jobStore.Put(ctx, job)
}

// UpgradeFuncs implement the Upgrader interface.
func (jobStore *JobStore) UpgradeFuncs() []bootstrap.UpgradeFunc {
	return nil
}
