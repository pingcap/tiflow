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
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func (t *testDMJobmasterSuite) TestUpdateWorkerStatus() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore(kvmock.NewMetaMock(), log.L())
	unitStore := metadata.NewUnitStateStore(kvmock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))
	workerManager := NewWorkerManager("job_id", nil, jobStore, unitStore, nil, nil, nil, log.L(), resModel.ResourceTypeLocalFile)

	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID
	workerStatus1 := runtime.InitWorkerStatus(source1, frameModel.WorkerDMDump, "worker-id-1")
	workerStatus2 := runtime.InitWorkerStatus(source2, frameModel.WorkerDMDump, "worker-id-2")
	require.True(t.T(), workerManager.allTombStone())

	// Creating
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	require.False(t.T(), workerManager.allTombStone())

	// Online
	workerStatus1.Stage = runtime.WorkerOnline
	workerStatus2.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	require.False(t.T(), workerManager.allTombStone())

	// Offline
	workerStatus1.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	require.False(t.T(), workerManager.allTombStone())

	// Finished
	workerStatus1.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	require.False(t.T(), workerManager.allTombStone())

	// mock jobmaster recover
	workerStatus1.Stage = runtime.WorkerOnline
	workerStatus2.Stage = runtime.WorkerOnline
	workerStatusList := []runtime.WorkerStatus{workerStatus1, workerStatus2}
	workerManager = NewWorkerManager("job_id", workerStatusList, jobStore, nil, nil, nil, nil, log.L(), resModel.ResourceTypeLocalFile)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	require.False(t.T(), workerManager.allTombStone())

	// mock dispatch error
	workerManager.removeWorkerStatusByWorkerID("worker-not-exist")
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	workerManager.removeWorkerStatusByWorkerID(workerStatus1.ID)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)
	require.False(t.T(), workerManager.allTombStone())

	workerStatus2.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus2)
	require.True(t.T(), workerManager.allTombStone())
}

func (t *testDMJobmasterSuite) TestClearWorkerStatus() {
	messageAgent := &dmpkg.MockMessageAgent{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source1 := "source1"
	source2 := "source2"
	workerStatus1 := runtime.InitWorkerStatus(source1, frameModel.WorkerDMDump, "worker-id-1")
	workerStatus2 := runtime.InitWorkerStatus(source2, frameModel.WorkerDMDump, "worker-id-2")

	workerManager := NewWorkerManager("job_id", []runtime.WorkerStatus{workerStatus1, workerStatus2}, nil, nil, nil, messageAgent, nil,
		log.L(), resModel.ResourceTypeLocalFile)
	require.Len(t.T(), workerManager.WorkerStatus(), 2)

	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 2)

	workerStatus1.Stage = runtime.WorkerOffline
	workerStatus2.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	job := metadata.NewJob(&config.JobCfg{})
	destroyError := errors.New("destroy error")

	job.Tasks[source2] = metadata.NewTask(&config.TaskCfg{})
	require.NoError(t.T(), workerManager.stopOutdatedWorkers(context.Background(), job))
	messageAgent.On("SendMessage").Return(destroyError).Once()
	jobCfg := &config.JobCfg{ModRevision: 1}
	taskCfg := jobCfg.ToTaskCfg()
	job.Tasks[source2] = metadata.NewTask(taskCfg)
	require.EqualError(t.T(), workerManager.stopOutdatedWorkers(context.Background(), job), destroyError.Error())
	messageAgent.On("SendMessage").Return(nil).Once()
	job.Tasks[source2] = metadata.NewTask(taskCfg)
	require.NoError(t.T(), workerManager.stopOutdatedWorkers(context.Background(), job))

	job = metadata.NewJob(&config.JobCfg{})
	job.Tasks[source2] = metadata.NewTask(&config.TaskCfg{})
	err := workerManager.stopUnneededWorkers(ctx, job)
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	delete(job.Tasks, source2)
	messageAgent.On("SendMessage").Return(destroyError).Once()
	err = workerManager.stopUnneededWorkers(ctx, job)
	require.EqualError(t.T(), err, destroyError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	messageAgent.On("SendMessage").Return(nil).Once()
	err = workerManager.stopUnneededWorkers(ctx, job)
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	workerStatus2.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus2)
	require.NoError(t.T(), workerManager.stopUnneededWorkers(ctx, job))
	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	err = workerManager.onJobDel(context.Background())
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	workerStatus1.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatus2.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus2)

	messageAgent.On("SendMessage").Return(destroyError).Once()
	messageAgent.On("SendMessage").Return(nil).Once()
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	require.Len(t.T(), workerManager.WorkerStatus(), 2)
	err = workerManager.onJobDel(context.Background())
	require.EqualError(t.T(), err, destroyError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 2)
	workerStatus1.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	workerManager.UpdateWorkerStatus(runtime.InitWorkerStatus("task", frameModel.WorkerDMDump, "worker-id"))
	require.Len(t.T(), workerManager.WorkerStatus(), 2)
	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 2)
	require.Eventually(t.T(), func() bool {
		workerManager.removeOfflineWorkers()
		return len(workerManager.WorkerStatus()) == 1
	}, 10*time.Second, 200*time.Millisecond)
}

func (t *testDMJobmasterSuite) TestCreateWorker() {
	mockAgent := &MockWorkerAgent{}
	unitStore := metadata.NewUnitStateStore(kvmock.NewMetaMock())
	workerManager := NewWorkerManager("job_id", nil, nil, unitStore, mockAgent, nil, nil, log.L(), resModel.ResourceTypeLocalFile)

	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	taskCfgs := jobCfg.ToTaskCfgs()
	task1 := jobCfg.Upstreams[0].SourceID
	worker1 := "worker1"
	createError := errors.New("create error")
	mockAgent.On("CreateWorker").Return("", createError).Once()
	require.EqualError(t.T(), workerManager.createWorker(context.Background(), task1, frameModel.WorkerDMDump, taskCfgs[task1]), createError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	workerStatus1 := runtime.InitWorkerStatus(task1, frameModel.WorkerDMDump, worker1)
	mockAgent.On("CreateWorker").Return(worker1, createError).Once()
	require.EqualError(t.T(), workerManager.createWorker(context.Background(), task1, frameModel.WorkerDMDump, taskCfgs[task1]), createError.Error())
	workerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 1)
	require.Contains(t.T(), workerStatusMap, task1)
	require.Equal(t.T(), workerStatusMap[task1].ID, workerStatus1.ID)

	task2 := jobCfg.Upstreams[1].SourceID
	worker2 := "worker2"
	workerStatus2 := runtime.InitWorkerStatus(task2, frameModel.WorkerDMLoad, worker2)
	mockAgent.On("CreateWorker").Return(worker2, nil).Once()
	require.NoError(t.T(), workerManager.createWorker(context.Background(), task2, frameModel.WorkerDMLoad, taskCfgs[task2]))
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, task1)
	require.Contains(t.T(), workerStatusMap, task2)
	require.Equal(t.T(), workerStatusMap[task1].ID, workerStatus1.ID)
	require.Equal(t.T(), workerStatusMap[task2].ID, workerStatus2.ID)
}

func (t *testDMJobmasterSuite) TestGetUnit() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockAgent := &MockCheckpointAgent{}
	task := &metadata.Task{Cfg: &config.TaskCfg{}}
	task.Cfg.TaskMode = dmconfig.ModeFull
	workerManager := NewWorkerManager("job_id", nil, nil, nil, nil, nil, mockAgent, log.L(), resModel.ResourceTypeLocalFile)

	workerStatus := runtime.NewWorkerStatus("source", frameModel.WorkerDMDump, "worker-id-1", runtime.WorkerOnline, 0)
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMDump)
	workerStatus.Stage = runtime.WorkerFinished
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMLoad)
	workerStatus.Stage = runtime.WorkerOnline
	workerStatus.Unit = frameModel.WorkerDMLoad
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMLoad)
	workerStatus.Stage = runtime.WorkerFinished
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMLoad)

	task.Cfg.TaskMode = dmconfig.ModeAll
	workerStatus.Unit = frameModel.WorkerDMDump
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMLoad)
	workerStatus.Unit = frameModel.WorkerDMLoad
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMSync)
	workerStatus.Unit = frameModel.WorkerDMSync
	workerStatus.Stage = runtime.WorkerOnline
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMSync)

	task.Cfg.TaskMode = dmconfig.ModeIncrement
	require.Equal(t.T(), getNextUnit(task, workerStatus), frameModel.WorkerDMSync)

	task.Cfg.TaskMode = dmconfig.ModeFull
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, errors.New("checkpoint error")).Once()
	unit, isFresh, err := workerManager.getCurrentUnit(ctx, task)
	require.Error(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerType(0))
	require.False(t.T(), isFresh)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Twice()
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMDump)
	require.True(t.T(), isFresh)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMLoad)
	require.False(t.T(), isFresh)

	task.Cfg.TaskMode = dmconfig.ModeAll
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMDump)
	require.True(t.T(), isFresh)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMLoad)
	require.False(t.T(), isFresh)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMSync)
	require.False(t.T(), isFresh)

	task.Cfg.TaskMode = dmconfig.ModeIncrement
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMSync)
	require.True(t.T(), isFresh)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, isFresh, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, frameModel.WorkerDMSync)
	require.False(t.T(), isFresh)
}

func (t *testDMJobmasterSuite) TestCheckAndScheduleWorkers() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jobCfg.TaskMode = dmconfig.ModeFull
	job := metadata.NewJob(jobCfg)
	checkpointAgent := &MockCheckpointAgent{}
	workerAgent := &MockWorkerAgent{}
	unitStore := metadata.NewUnitStateStore(kvmock.NewMetaMock())
	workerManager := NewWorkerManager("job_id", nil, nil, unitStore, workerAgent, nil, checkpointAgent, log.L(), resModel.ResourceTypeLocalFile)

	// new tasks
	worker1 := "worker1"
	worker2 := "worker2"
	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID
	checkpointError := errors.New("checkpoint error")
	createError := errors.New("create error")

	getCurrentStatus := func() map[string]*metadata.UnitStatus {
		state, err := workerManager.unitStore.Get(context.Background())
		require.NoError(t.T(), err)
		unitState, ok := state.(*metadata.UnitState)
		require.True(t.T(), ok)
		return unitState.CurrentUnitStatus
	}
	var currentStatus map[string]*metadata.UnitStatus

	getTaskID := func() (string, string) {
		if _, ok := currentStatus[source1]; ok {
			return source1, source2
		}
		if _, ok := currentStatus[source2]; ok {
			return source2, source1
		}
		return "", ""
	}

	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, checkpointError).Once()
	workerAgent.On("CreateWorker").Return(worker1, nil).Once()
	require.EqualError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job), checkpointError.Error())
	wokerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 1)

	currentStatus = getCurrentStatus()
	taskID1, taskID2 := getTaskID()
	require.Len(t.T(), currentStatus, 1)
	require.Contains(t.T(), currentStatus, taskID1)
	require.True(t.T(), time.Since(currentStatus[taskID1].CreatedTime).Seconds() < float64(time.Second))

	// check again
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	workerAgent.On("CreateWorker").Return(worker2, createError).Once()
	require.EqualError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job), createError.Error())
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	workerStatus1 := wokerStatusMap[source1]
	workerStatus2 := wokerStatusMap[source2]
	currentStatus = getCurrentStatus()
	require.Len(t.T(), currentStatus, 2)
	require.Contains(t.T(), currentStatus, taskID2)
	require.True(t.T(), time.Since(currentStatus[taskID2].CreatedTime).Seconds() < float64(time.Second))

	// expected
	workerStatus1.Stage = runtime.WorkerOnline
	workerStatus2.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), wokerStatusMap[source2], workerStatus2)

	// switch unit
	worker3 := "worker3"
	workerStatus1.Stage = runtime.WorkerFinished
	workerStatus3 := runtime.InitWorkerStatus(source1, frameModel.WorkerDMLoad, worker3)
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatus1.Stage = runtime.WorkerFinished
	workerAgent.On("CreateWorker").Return(worker3, nil).Once()
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1].ID, workerStatus3.ID)
	require.Equal(t.T(), wokerStatusMap[source2].ID, workerStatus2.ID)
	currentStatus = getCurrentStatus()
	require.Contains(t.T(), currentStatus, source1)
	require.True(t.T(), time.Since(currentStatus[source1].CreatedTime).Seconds() < float64(time.Second))
	require.Equal(t.T(), frameModel.WorkerDMLoad, currentStatus[source1].Unit)

	// unexpected
	worker4 := "worker3"
	workerStatus3.Stage = runtime.WorkerOffline
	workerStatus4 := runtime.InitWorkerStatus(source1, frameModel.WorkerDMLoad, worker4)
	workerManager.UpdateWorkerStatus(workerStatus3)
	workerAgent.On("CreateWorker").Return(worker4, nil).Once()
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1].ID, workerStatus4.ID)
	require.Equal(t.T(), wokerStatusMap[source2].ID, workerStatus2.ID)

	// finished
	workerStatus4.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus4)
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1].ID, workerStatus4.ID)
	require.Equal(t.T(), wokerStatusMap[source2].ID, workerStatus2.ID)
}

func (t *testDMJobmasterSuite) TestWorkerManager() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore(kvmock.NewMetaMock(), log.L())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))

	unitStore := metadata.NewUnitStateStore(kvmock.NewMetaMock())
	checkpointAgent := &MockCheckpointAgent{}
	workerAgent := &MockWorkerAgent{}
	messageAgent := &dmpkg.MockMessageAgent{}
	workerManager := NewWorkerManager("job_id", nil, jobStore, unitStore, workerAgent, messageAgent, checkpointAgent, log.L(), resModel.ResourceTypeLocalFile)
	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker1 := "worker1"
	worker2 := "worker2"
	checkpointError := errors.New("checkpoint error")
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, checkpointError).Once()
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(6)
	createError := errors.New("create error")
	workerAgent.On("CreateWorker").Return(worker1, nil).Once()
	workerAgent.On("CreateWorker").Return("", createError).Once()
	workerAgent.On("CreateWorker").Return(worker2, nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	// run worker manager
	go func() {
		defer wg.Done()
		t := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				workerManager.DoTick(ctx)
			}
		}
	}()

	// first check
	require.Eventually(t.T(), func() bool {
		return len(workerManager.WorkerStatus()) == 2
	}, 5*time.Second, 100*time.Millisecond)

	workerStatus1 := workerManager.WorkerStatus()[source1]
	workerStatus2 := workerManager.WorkerStatus()[source2]
	require.Equal(t.T(), runtime.WorkerCreating, workerStatus1.Stage)
	require.Equal(t.T(), runtime.WorkerCreating, workerStatus2.Stage)

	// worker online
	workerStatus1.Stage = runtime.WorkerOnline
	workerStatus2.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)

	// mock check by interval
	workerManager.SetNextCheckTime(time.Now().Add(10 * time.Millisecond))

	// expected, no panic in mock agent
	time.Sleep(1 * time.Second)

	// worker2 offline
	source := workerStatus2.TaskID
	worker3 := "worker3"
	workerStatus2.Stage = runtime.WorkerOffline
	workerStatus3 := runtime.InitWorkerStatus(source, frameModel.WorkerDMDump, worker3)
	// check by offline
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerManager.SetNextCheckTime(time.Now())
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	workerAgent.On("CreateWorker").Return(worker3, nil).Once()

	// scheduled eventually
	require.Eventually(t.T(), func() bool {
		return workerManager.WorkerStatus()[source].ID == workerStatus3.ID
	}, 5*time.Second, 100*time.Millisecond)
	workerStatus3.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus3)

	// mock remove task2 by update-job
	delete(job.Tasks, source2)
	job.Tasks[source1].Cfg.ModRevision++
	jobStore.Put(context.Background(), job)
	messageAgent.On("SendMessage").Return(nil).Twice()
	// check by update-job, task2 stops, task1 restarts
	workerManager.SetNextCheckTime(time.Now())
	// both task removed eventually
	require.Eventually(t.T(), func() bool {
		messageAgent.Lock()
		defer messageAgent.Unlock()
		return len(messageAgent.Calls) == 2
	}, 5*time.Second, 100*time.Millisecond)
	workerStatus1.Stage = runtime.WorkerOffline
	workerStatus3.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus3)

	// task1 eventually restarts
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	workerAgent.On("CreateWorker").Return(worker1, nil).Once()
	workerManager.SetNextCheckTime(time.Now())
	require.Eventually(t.T(), func() bool {
		return len(workerManager.WorkerStatus()) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// mock task1 finished
	worker4 := "worker4"
	workerStatus := workerManager.WorkerStatus()[source1]
	workerStatus.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus)
	workerAgent.On("CreateWorker").Return(worker4, nil).Once()
	// check by finished
	workerManager.SetNextCheckTime(time.Now())
	// scheduled eventually
	require.Eventually(t.T(), func() bool {
		return workerManager.WorkerStatus()[source1].ID == worker4
	}, 5*time.Second, 100*time.Millisecond)

	// mock deleting job
	jobStore.MarkDeleting(ctx)
	destroyError := errors.New("destroy error")
	messageAgent.On("SendMessage").Return(destroyError).Once()
	messageAgent.On("SendMessage").Return(nil).Once()

	// check by delete
	workerManager.SetNextCheckTime(time.Now())
	require.Eventually(t.T(), func() bool {
		messageAgent.Lock()
		defer messageAgent.Unlock()
		return len(messageAgent.Calls) == 4
	}, 5*time.Second, 100*time.Millisecond)

	workerStatus.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus)
	// deleted eventually
	workerManager.SetNextCheckTime(time.Now())
	require.Eventually(t.T(), func() bool {
		return len(workerManager.WorkerStatus()) == 0
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()

	checkpointAgent.AssertExpectations(t.T())
	workerAgent.AssertExpectations(t.T())
	messageAgent.AssertExpectations(t.T())
}

type MockWorkerAgent struct {
	sync.Mutex
	mock.Mock
}

func (mockAgent *MockWorkerAgent) CreateWorker(
	workerType framework.WorkerType, taskCfg interface{},
	opts ...framework.CreateWorkerOpt,
) (frameModel.WorkerID, error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	args := mockAgent.Called()
	return args.Get(0).(frameModel.WorkerID), args.Error(1)
}
