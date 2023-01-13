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

	"github.com/pingcap/log"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const (
	jobTemplatePath = "./config/job_template.yaml"
)

func (t *testDMJobmasterSuite) TestUpdateTaskStatus() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jobCfg.TaskMode = dmconfig.ModeFull
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore(kvmock.NewMetaMock(), log.L())
	taskManager := NewTaskManager("test-job", nil, jobStore, nil, log.L(), promutil.NewFactory4Test(t.T().TempDir()))

	require.Len(t.T(), taskManager.TaskStatus(), 0)
	require.False(t.T(), taskManager.allFinished(context.Background()))
	require.NoError(t.T(), jobStore.Put(context.Background(), job))

	dumpStatus1 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMDump,
		Task:  jobCfg.Upstreams[0].SourceID,
		Stage: metadata.StageRunning,
	}
	dumpStatus2 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMDump,
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
		Unit:  frameModel.WorkerDMLoad,
		Task:  jobCfg.Upstreams[0].SourceID,
		Stage: metadata.StageRunning,
	}
	loadStatus2 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMLoad,
		Task:  jobCfg.Upstreams[1].SourceID,
		Stage: metadata.StageFinished,
	}
	taskManager.UpdateTaskStatus(loadStatus1)
	taskStatusMap = taskManager.TaskStatus()
	// copy undetermined time.Now
	loadStatus1.StageUpdatedTime = taskStatusMap[jobCfg.Upstreams[0].SourceID].StageUpdatedTime
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
	taskManager = NewTaskManager("test-job", taskStatusList, jobStore, nil, log.L(), promutil.NewFactory4Test(t.T().TempDir()))
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t.T(), taskStatusMap, 2)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t.T(), taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t.T(), taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	require.False(t.T(), taskManager.allFinished(context.Background()))
	loadStatus1.Stage = metadata.StageFinished
	dumpStatus2.Stage = metadata.StageFinished
	taskManager.UpdateTaskStatus(loadStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	require.False(t.T(), taskManager.allFinished(context.Background()))
	taskManager.UpdateTaskStatus(loadStatus2)
	require.True(t.T(), taskManager.allFinished(context.Background()))
	taskManager.tasks.Delete(loadStatus2.Task)
	require.False(t.T(), taskManager.allFinished(context.Background()))
}

func (t *testDMJobmasterSuite) TestOperateTask() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jobStore := metadata.NewJobStore(kvmock.NewMetaMock(), log.L())
	taskManager := NewTaskManager("test-job", nil, jobStore, &dmpkg.MockMessageAgent{}, log.L(), promutil.NewFactory4Test(t.T().TempDir()))

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
	require.Equal(t.T(), job.Tasks[source2].Stage, metadata.StagePaused)
	require.False(t.T(), job.Deleting)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Deleting, nil, nil))
	state, err = jobStore.Get(context.Background())
	require.NoError(t.T(), err)
	job = state.(*metadata.Job)
	require.True(t.T(), job.Deleting)

	require.NoError(t.T(), taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, []string{source1, source2}))
	state, err = jobStore.Get(context.Background())
	require.EqualError(t.T(), err, "state not found")
	require.Nil(t.T(), state)

	require.EqualError(t.T(), taskManager.OperateTask(context.Background(), 0, nil, nil), "unknown operate type")
}

func (t *testDMJobmasterSuite) TestClearTaskStatus() {
	taskManager := NewTaskManager("test-job", nil, nil, nil, log.L(), promutil.NewFactory4Test(t.T().TempDir()))
	syncStatus1 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMSync,
		Task:  "source1",
		Stage: metadata.StageRunning,
	}
	syncStatus2 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMSync,
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

	taskManager.onJobDel()
	require.Len(t.T(), taskManager.TaskStatus(), 0)

	taskManager.onJobDel()
	require.Len(t.T(), taskManager.TaskStatus(), 0)
}

func (t *testDMJobmasterSuite) TestGenOp() {
	earlierTime := time.Now()
	laterTime := earlierTime.Add(time.Second)
	cases := []struct {
		runningStage             metadata.TaskStage
		runningStageUpdatedTime  time.Time
		expectedStage            metadata.TaskStage
		expectedStageUpdatedTime time.Time
		op                       dmpkg.OperateType
	}{
		{
			runningStage:             metadata.StagePaused,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StagePaused,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.None,
		},
		{
			runningStage:             metadata.StageRunning,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StageRunning,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.None,
		},
		{
			runningStage:             metadata.StageRunning,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StagePaused,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.Pause,
		},
		{
			runningStage:             metadata.StageError,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StagePaused,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.Pause,
		},
		{
			runningStage:             metadata.StageFinished,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StageRunning,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.None,
		},
		{
			runningStage:             metadata.StagePaused,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StageRunning,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.Resume,
		},
		{
			runningStage:             metadata.StageError,
			runningStageUpdatedTime:  earlierTime,
			expectedStage:            metadata.StageRunning,
			expectedStageUpdatedTime: laterTime,
			op:                       dmpkg.Resume,
		},
		{
			runningStage:             metadata.StageError,
			runningStageUpdatedTime:  laterTime,
			expectedStage:            metadata.StageRunning,
			expectedStageUpdatedTime: earlierTime,
			op:                       dmpkg.None,
		},
		{
			runningStage:             metadata.StageRunning,
			runningStageUpdatedTime:  laterTime,
			expectedStage:            metadata.StagePaused,
			expectedStageUpdatedTime: earlierTime,
			op:                       dmpkg.Pause,
		},
	}

	for i, c := range cases {
		t.T().Logf("case %d", i)
		op := genOp(c.runningStage, c.runningStageUpdatedTime, c.expectedStage, c.expectedStageUpdatedTime)
		require.Equal(t.T(), c.op, op)
	}
}

func (t *testDMJobmasterSuite) TestCheckAndOperateTasks() {
	now := time.Now()
	oldTime := now.Add(-time.Second)
	newTime := now.Add(time.Second)
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	mockAgent := &dmpkg.MockMessageAgent{}
	taskManager := NewTaskManager("test-job", nil, nil, mockAgent, log.L(), promutil.NewFactory4Test(t.T().TempDir()))

	require.EqualError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job), "get task running status failed")

	dumpStatus1 := runtime.TaskStatus{
		Unit:             frameModel.WorkerDMDump,
		Task:             jobCfg.Upstreams[0].SourceID,
		Stage:            metadata.StageRunning,
		StageUpdatedTime: now,
	}
	dumpStatus2 := runtime.TaskStatus{
		Unit:             frameModel.WorkerDMDump,
		Task:             jobCfg.Upstreams[1].SourceID,
		Stage:            metadata.StageRunning,
		StageUpdatedTime: now,
	}
	taskManager.UpdateTaskStatus(dumpStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	job.Tasks[jobCfg.Upstreams[0].SourceID].StageUpdatedTime = newTime
	job.Tasks[jobCfg.Upstreams[1].SourceID].StageUpdatedTime = newTime
	require.NoError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job))

	dumpStatus2.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(dumpStatus2)
	e := errors.New("operate task failed")
	mockAgent.On("SendMessage").Return(e).Once()
	job.Tasks[jobCfg.Upstreams[0].SourceID].StageUpdatedTime = newTime
	job.Tasks[jobCfg.Upstreams[1].SourceID].StageUpdatedTime = newTime
	require.EqualError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job), e.Error())

	// newer error stage will not trigger SendMessage, so will not meet injected error
	dumpStatus2.Stage = metadata.StageError
	taskManager.UpdateTaskStatus(dumpStatus2)
	e = errors.New("operate task failed")
	mockAgent.On("SendMessage").Return(e).Once()
	job.Tasks[jobCfg.Upstreams[0].SourceID].StageUpdatedTime = oldTime
	job.Tasks[jobCfg.Upstreams[1].SourceID].StageUpdatedTime = oldTime
	require.NoError(t.T(), taskManager.checkAndOperateTasks(context.Background(), job))
}

func (t *testDMJobmasterSuite) TestTaskManager() {
	var meetExpected atomic.Bool
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore(kvmock.NewMetaMock(), log.L())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))

	mockAgent := &dmpkg.MockMessageAgent{}
	taskManager := NewTaskManager("test-job", nil, jobStore, mockAgent, log.L(), promutil.NewFactory4Test(t.T().TempDir()))
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
				taskManager.DoTick(ctx)
			}
		}
	}()

	syncStatus1 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMSync,
		Task:  source1,
		Stage: metadata.StageRunning,
	}
	syncStatus2 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMSync,
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
