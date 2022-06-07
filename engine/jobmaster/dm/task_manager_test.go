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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/lib"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
)

const (
	jobTemplatePath = "./config/job_template.yaml"
)

func (t *testDMJobmasterSuite) TestUpdateTaskStatus() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", kvmock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))
	taskManager := NewTaskManager(nil, jobStore, nil)

	require.Len(t.T(), taskManager.TaskStatus(), 0)

	dumpStatus1 := runtime.TaskStatus{
		Unit:  lib.WorkerDMDump,
		Task:  jobCfg.Upstreams[0].SourceID,
		Stage: metadata.StageRunning,
	}
	dumpStatus2 := runtime.TaskStatus{
		Unit:  lib.WorkerDMDump,
		Task:  jobCfg.Upstreams[1].SourceID,
		Stage: metadata.StageRunning,
	}

	taskManager.UpdateTaskStatus(dumpStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	taskStatusMap := taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], dumpStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	loadStatus1 := runtime.TaskStatus{
		Unit:  lib.WorkerDMDump,
		Task:  jobCfg.Upstreams[0].SourceID,
		Stage: metadata.StageRunning,
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
	jobStore := metadata.NewJobStore("task_manager_test", kvmock.NewMetaMock())
	taskManager := NewTaskManager(nil, jobStore, &dmpkg.MockMessageAgent{})

	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID

	state, err := jobStore.Get(context.Background())
	require.EqualError(t.T(), err, "state not found")
	require.Nil(t.T(), state)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Create, jobCfg, nil))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job := state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StageRunning)
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StageRunning)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Pause, nil, []string{source1, source2}))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StagePaused)
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StagePaused)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Resume, nil, []string{source1}))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StageRunning)
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StagePaused)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Update, jobCfg, nil))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.Equal(t.T(), job.Tasks[source1].Stage, metadata.StageRunning)
	// TODO: should it be paused?
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StageRunning)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, []string{source1, source2}))
	state, err = jobStore.Get(context.Background())
	require.EqualError(t.T(), err, "state not found")
	require.Nil(t.T(), state)

	require.EqualError(t.T(), taskManager.OperateTask(context.Background(), -1, nil, nil), "unknown operate type")
}

func (t *testDMJobmasterSuite) TestClearTaskStatus() {
	taskManager := NewTaskManager(nil, nil, nil)
	syncStatus1 := runtime.TaskStatus{
		Unit:  lib.WorkerDMSync,
		Task:  "source1",
		Stage: metadata.StageRunning,
	}
	syncStatus2 := runtime.TaskStatus{
		Unit:  lib.WorkerDMSync,
		Task:  "source2",
		Stage: metadata.StageRunning,
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

	taskStatus, ok := taskManager.GetTaskStatus("source2")
	require.False(t.T(), ok)
	require.Equal(t.T(), runtime.NewOfflineStatus("source2"), taskStatus)
	taskStatus, ok = taskManager.GetTaskStatus("source1")
	require.True(t.T(), ok)
	require.Equal(t.T(), syncStatus1, taskStatus)

	taskManager.onJobNotExist(context.Background())
	require.Len(t.T(), taskManager.TaskStatus(), 0)

	taskManager.onJobNotExist(context.Background())
	require.Len(t.T(), taskManager.TaskStatus(), 0)
}

func (t *testDMJobmasterSuite) TestGenOp() {
	require.Equal(t.T(), genOp(metadata.StagePaused, metadata.StagePaused), dmpkg.None)
	require.Equal(t.T(), genOp(metadata.StageRunning, metadata.StageRunning), dmpkg.None)
	require.Equal(t.T(), genOp(metadata.StageRunning, metadata.StagePaused), dmpkg.Pause)
	require.Equal(t.T(), genOp(metadata.StageFinished, metadata.StageRunning), dmpkg.None)

	// TODO: change below results if needed.
	require.Equal(t.T(), genOp(metadata.StagePaused, metadata.StageRunning), dmpkg.Resume)
}

func (t *testDMJobmasterSuite) TestCheckAndOperateTasks() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	mockAgent := &dmpkg.MockMessageAgent{}
	taskManager := NewTaskManager(nil, nil, mockAgent)

	require.EqualError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job), "get task running status failed")

	dumpStatus1 := runtime.TaskStatus{
		Unit:  lib.WorkerDMDump,
		Task:  jobCfg.Upstreams[0].SourceID,
		Stage: metadata.StageRunning,
	}
	dumpStatus2 := runtime.TaskStatus{
		Unit:  lib.WorkerDMDump,
		Task:  jobCfg.Upstreams[1].SourceID,
		Stage: metadata.StageRunning,
	}
	taskManager.UpdateTaskStatus(dumpStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	require.NoError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job))

	dumpStatus2.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(dumpStatus2)
	e := errors.New("operate task failed")
	mockAgent.On("SendMessage").Return(e).Once()
	require.EqualError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job), e.Error())
}

func (t *testDMJobmasterSuite) TestTaskManager() {
	var meetExpected atomic.Bool
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", kvmock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))

	mockAgent := &dmpkg.MockMessageAgent{}
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

	syncStatus1 := runtime.TaskStatus{
		Unit:  lib.WorkerDMSync,
		Task:  source1,
		Stage: metadata.StageRunning,
	}
	syncStatus2 := runtime.TaskStatus{
		Unit:  lib.WorkerDMSync,
		Task:  source2,
		Stage: metadata.StageRunning,
	}

	// receive worker online
	taskManager.UpdateTaskStatus(syncStatus1)
	taskManager.UpdateTaskStatus(syncStatus2)

	// task1 paused unexpectedly
	agentError := errors.New("agent error")
	mockAgent.On("SendMessage").Return(agentError).Times(3)
	mockAgent.On("SendMessage").Return(nil).Once().Run(func(args mock.Arguments) { meetExpected.Store(true) })
	syncStatus1.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(syncStatus1)

	// mock check by interval
	taskManager.SetNextCheckTime(time.Now().Add(10 * time.Millisecond))
	require.Eventually(t.T(), func() bool {
		return meetExpected.CAS(true, false)
	}, 5*time.Second, 100*time.Millisecond)
	// resumed eventually
	syncStatus1.Stage = metadata.StageRunning
	taskManager.UpdateTaskStatus(syncStatus1)

	// manually pause task2
	taskManager.OperateTask(ctx, dmpkg.Pause, nil, []string{source2})
	mockAgent.On("SendMessage").Return(agentError).Times(3)
	mockAgent.On("SendMessage").Return(nil).Once().Run(func(args mock.Arguments) { meetExpected.Store(true) })
	require.Eventually(t.T(), func() bool {
		return meetExpected.CAS(true, false)
	}, 5*time.Second, 100*time.Millisecond)
	// paused eventually
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
	taskManager.OperateTask(ctx, dmpkg.Update, jobCfg, nil)
	require.Eventually(t.T(), func() bool {
		return len(taskManager.TaskStatus()) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// mock delete job
	taskManager.OperateTask(ctx, dmpkg.Delete, nil, nil)
	require.Eventually(t.T(), func() bool {
		return len(taskManager.TaskStatus()) == 0
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
	mockAgent.AssertExpectations(t.T())
}
