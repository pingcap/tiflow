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

package dm

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
)

const (
	jobTemplatePath = "./config/job_template.yaml"
)

func (t *testDMJobmasterSuite) TestUpdateTaskStatus() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", mock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))
	taskManager := NewTaskManager(nil, jobStore, nil)

	require.Len(t.T(), taskManager.TaskStatus(), 0)

	dumpStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[0].SourceID,
			Stage: metadata.StageRunning,
		},
	}
	dumpStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[1].SourceID,
			Stage: metadata.StageRunning,
		},
	}

	taskManager.UpdateTaskStatus(dumpStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	taskStatusMap := taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], dumpStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	loadStatus1 := &runtime.LoadStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[0].SourceID,
			Stage: metadata.StageRunning,
		},
	}
	taskManager.UpdateTaskStatus(loadStatus1)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	// offline
	offlineStatus := runtime.NewOfflineStatus(jobCfg.Upstreams[1].SourceID)
	taskManager.UpdateTaskStatus(offlineStatus)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], offlineStatus)

	// online
	taskManager.UpdateTaskStatus(dumpStatus2)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	// mock jobmaster recover
	taskStatusList := make([]runtime.TaskStatus, 0, len(taskStatusMap))
	for _, taskStatus := range taskStatusMap {
		taskStatusList = append(taskStatusList, taskStatus)
	}
	taskManager = NewTaskManager(taskStatusList, jobStore, nil)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)
}

func (t *testDMJobmasterSuite) TestOperateTask() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jobStore := metadata.NewJobStore("task_manager_test", mock.NewMetaMock())
	taskManager := NewTaskManager(nil, jobStore, nil)

	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID

	state, err := jobStore.Get(context.Background())
	require.EqualError(t.T(), err, "state not found")
	require.Nil(t.T(), state)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), Create, jobCfg, nil))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job := state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StageRunning)
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StageRunning)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), Pause, nil, []string{source1, source2}))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StagePaused)
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StagePaused)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), Resume, nil, []string{source1}))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StageRunning)
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StagePaused)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), Update, jobCfg, nil))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StageRunning)
	// TODO: should it be paused?
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StageRunning)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), Delete, nil, []string{source1, source2}))
	state, err = jobStore.Get(context.Background())
	require.EqualError(t.T(), err, "state not found")
	require.Nil(t.T(), state)

	require.EqualError(t.T(), taskManager.OperateTask(context.Background(), -1, nil, nil), "unknown operate type")
}

func (t *testDMJobmasterSuite) TestClearTaskStatus() {
	taskManager := NewTaskManager(nil, nil, nil)
	syncStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  "source1",
			Stage: metadata.StageRunning,
		},
	}
	syncStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  "source2",
			Stage: metadata.StageRunning,
		},
	}

	taskManager.UpdateTaskStatus(syncStatus1)
	taskManager.UpdateTaskStatus(syncStatus2)
	require.Len(t.T(), taskManager.TaskStatus(), 2)

	job := metadata.NewJob(&config.JobCfg{})
	job.Tasks["source1"] = metadata.NewTask(&config.TaskCfg{})

	taskManager.removeTaskStatus(job)
	require.Len(t.T(), taskManager.TaskStatus(), 1)

	job.Tasks["source3"] = metadata.NewTask(&config.TaskCfg{})
	taskManager.removeTaskStatus(job)
	require.Len(t.T(), taskManager.TaskStatus(), 1)

	taskManager.onJobNotExist(context.Background())
	require.Len(t.T(), taskManager.TaskStatus(), 0)

	taskManager.onJobNotExist(context.Background())
	require.Len(t.T(), taskManager.TaskStatus(), 0)
}

func (t *testDMJobmasterSuite) TestTaskAsExpected() {
	require.True(t.T(), taskAsExpected(&metadata.Task{Stage: metadata.StagePaused},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StagePaused}}))
	require.True(t.T(), taskAsExpected(&metadata.Task{Stage: metadata.StageRunning},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StageRunning}}))
	require.False(t.T(), taskAsExpected(&metadata.Task{Stage: metadata.StagePaused},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StageRunning}}))

	// TODO: change below results if needed.
	require.False(t.T(), taskAsExpected(&metadata.Task{Stage: metadata.StageRunning},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StagePaused}}))
	require.False(t.T(), taskAsExpected(&metadata.Task{Stage: metadata.StageRunning},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StageFinished}}))
}

func (t *testDMJobmasterSuite) TestCheckAndOperateTasks() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	mockAgent := &MockTaskAgent{}
	taskManager := NewTaskManager(nil, nil, mockAgent)

	require.EqualError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job), "get task running status failed")

	dumpStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[0].SourceID,
			Stage: metadata.StageRunning,
		},
	}
	dumpStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[1].SourceID,
			Stage: metadata.StageRunning,
		},
	}
	taskManager.UpdateTaskStatus(dumpStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	require.NoError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job))

	mockAgent.SetStages(map[string]metadata.TaskStage{jobCfg.Upstreams[0].SourceID: metadata.StageRunning, jobCfg.Upstreams[1].SourceID: metadata.StagePaused})
	dumpStatus2.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(dumpStatus2)
	e := errors.New("operate task failed")
	mockAgent.SetResult([]error{e})
	require.EqualError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job), e.Error())
}

func (t *testDMJobmasterSuite) TestTaskManager() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", mock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))

	mockAgent := &MockTaskAgent{}
	taskManager := NewTaskManager(nil, jobStore, mockAgent)
	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	// run task manager
	go func() {
		defer wg.Done()
		t := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				taskManager.Tick(ctx)
			}
		}
	}()

	syncStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  source1,
			Stage: metadata.StageRunning,
		},
	}
	syncStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  source2,
			Stage: metadata.StageRunning,
		},
	}

	mockAgent.SetStages(map[string]metadata.TaskStage{source1: metadata.StageRunning, source2: metadata.StageRunning})
	// receive worker online
	taskManager.UpdateTaskStatus(syncStatus1)
	taskManager.UpdateTaskStatus(syncStatus2)

	// task1 paused unexpectedly
	agentError := errors.New("agent error")
	mockAgent.SetResult([]error{agentError, agentError, agentError, nil})
	syncStatus1.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(syncStatus1)

	// mock check by interval
	taskManager.SetNextCheckTime(time.Now().Add(time.Second))
	// resumed eventually
	require.Eventually(t.T(), func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && mockAgent.stages[source1] == metadata.StageRunning
	}, 5*time.Second, 100*time.Millisecond)
	syncStatus1.Stage = metadata.StageRunning
	taskManager.UpdateTaskStatus(syncStatus1)

	// manually pause task2
	taskManager.OperateTask(ctx, Pause, nil, []string{source2})
	mockAgent.SetResult([]error{agentError, agentError, agentError, nil})
	// paused eventually
	require.Eventually(t.T(), func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && mockAgent.stages[source2] == metadata.StagePaused
	}, 5*time.Second, 100*time.Millisecond)
	syncStatus2.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(syncStatus2)

	// worker of task2 offline
	taskManager.UpdateTaskStatus(runtime.NewOfflineStatus(source2))
	// mock check by interval
	taskManager.SetNextCheckTime(time.Now().Add(time.Millisecond))
	// no request, no panic in mockAgent
	time.Sleep(1 * time.Second)

	// task2 online
	taskManager.UpdateTaskStatus(syncStatus2)

	// mock remove task2 by update-job
	jobCfg.Upstreams = jobCfg.Upstreams[:1]
	taskManager.OperateTask(ctx, Update, jobCfg, nil)
	require.Eventually(t.T(), func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && len(taskManager.TaskStatus()) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// mock delete job
	taskManager.OperateTask(ctx, Delete, nil, nil)
	require.Eventually(t.T(), func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && len(taskManager.TaskStatus()) == 0
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

type MockTaskAgent struct {
	sync.Mutex
	results []error
	stages  map[string]metadata.TaskStage
}

func (mockAgent *MockTaskAgent) SetResult(results []error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.results = append(mockAgent.results, results...)
}

func (mockAgent *MockTaskAgent) SetStages(stages map[string]metadata.TaskStage) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.stages = stages
}

func (mockAgent *MockTaskAgent) OperateTask(ctx context.Context, taskID string, stage metadata.TaskStage) error {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.results) == 0 {
		panic("no result in mock agent")
	}
	mockAgent.stages[taskID] = stage
	result := mockAgent.results[0]
	mockAgent.results = mockAgent.results[1:]
	return result
}
