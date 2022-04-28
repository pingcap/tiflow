package dm

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/pingcap/errors"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	kvmock "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/stretchr/testify/mock"
)

func (t *testDMJobmasterSuite) TestUpdateWorkerStatus() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("worker_manager_test", kvmock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))
	workerManager := NewWorkerManager(nil, jobStore, nil, nil)

	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID
	workerStatus1 := runtime.InitWorkerStatus(source1, lib.WorkerDMDump, "worker-id-1")
	workerStatus2 := runtime.InitWorkerStatus(source2, lib.WorkerDMDump, "worker-id-2")

	// Creating
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)

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

	// Offline
	workerStatus1.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)

	// Finished
	workerStatus1.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)

	// mock jobmaster recover
	workerStatus1.Stage = runtime.WorkerOnline
	workerStatus2.Stage = runtime.WorkerOnline
	workerStatusList := []runtime.WorkerStatus{workerStatus1, workerStatus2}
	workerManager = NewWorkerManager(workerStatusList, jobStore, nil, nil)
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, source1)
	require.Contains(t.T(), workerStatusMap, source2)
	require.Equal(t.T(), workerStatusMap[source1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[source2], workerStatus2)

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
}

func (t *testDMJobmasterSuite) TestClearWorkerStatus() {
	mockAgent := &MockWorkerAgent{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source1 := "source1"
	source2 := "source2"
	workerStatus1 := runtime.InitWorkerStatus(source1, lib.WorkerDMDump, "worker-id-1")
	workerStatus2 := runtime.InitWorkerStatus(source2, lib.WorkerDMDump, "worker-id-2")

	workerManager := NewWorkerManager([]runtime.WorkerStatus{workerStatus1, workerStatus2}, nil, mockAgent, nil)
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
	job.Tasks[source2] = metadata.NewTask(&config.TaskCfg{})
	err := workerManager.stopUnneededWorkers(ctx, job)
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	delete(job.Tasks, source2)
	destroyError := errors.New("destroy error")
	mockAgent.SetDestroyResult([]error{destroyError})
	err = workerManager.stopUnneededWorkers(ctx, job)
	require.EqualError(t.T(), err, destroyError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	mockAgent.SetDestroyResult([]error{nil})
	err = workerManager.stopUnneededWorkers(ctx, job)
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	err = workerManager.onJobNotExist(context.Background())
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	workerStatus1.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus1)

	mockAgent.SetDestroyResult([]error{destroyError, nil})
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.UpdateWorkerStatus(workerStatus2)
	require.Len(t.T(), workerManager.WorkerStatus(), 2)
	err = workerManager.onJobNotExist(context.Background())
	require.EqualError(t.T(), err, destroyError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	workerManager.UpdateWorkerStatus(runtime.InitWorkerStatus("task", lib.WorkerDMDump, "worker-id"))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerManager := NewWorkerManager(nil, nil, mockAgent, nil)

	task1 := "task-1"
	worker1 := "worker1"
	createError := errors.New("create error")
	mockAgent.SetCreateResult([]CreateResult{{"", createError}})
	require.EqualError(t.T(), workerManager.createWorker(ctx, task1, lib.WorkerDMDump, &config.TaskCfg{}), createError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	workerStatus1 := runtime.InitWorkerStatus(task1, lib.WorkerDMDump, worker1)
	mockAgent.SetCreateResult([]CreateResult{{worker1, createError}})
	require.EqualError(t.T(), workerManager.createWorker(ctx, task1, lib.WorkerDMDump, &config.TaskCfg{}), createError.Error())
	workerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 1)
	require.Contains(t.T(), workerStatusMap, task1)
	require.Equal(t.T(), workerStatusMap[task1].ID, workerStatus1.ID)

	task2 := "task-2"
	worker2 := "worker2"
	workerStatus2 := runtime.InitWorkerStatus(task2, lib.WorkerDMLoad, worker2)
	mockAgent.SetCreateResult([]CreateResult{{worker2, nil}})
	require.NoError(t.T(), workerManager.createWorker(ctx, task2, lib.WorkerDMLoad, &config.TaskCfg{}))
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
	workerManager := NewWorkerManager(nil, nil, nil, mockAgent)

	workerStatus := runtime.NewWorkerStatus("source", lib.WorkerDMDump, "worker-id-1", runtime.WorkerOnline)
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMDump)
	workerStatus.Stage = runtime.WorkerFinished
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMLoad)
	workerStatus.Stage = runtime.WorkerOnline
	workerStatus.Unit = lib.WorkerDMLoad
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMLoad)
	workerStatus.Stage = runtime.WorkerFinished
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMLoad)

	task.Cfg.TaskMode = dmconfig.ModeAll
	workerStatus.Unit = lib.WorkerDMDump
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMLoad)
	workerStatus.Unit = lib.WorkerDMLoad
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMSync)
	workerStatus.Unit = lib.WorkerDMSync
	workerStatus.Stage = runtime.WorkerOnline
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMSync)

	task.Cfg.TaskMode = dmconfig.ModeIncrement
	require.Equal(t.T(), getNextUnit(task, workerStatus), lib.WorkerDMSync)

	task.Cfg.TaskMode = dmconfig.ModeFull
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, errors.New("checkpoint error")).Once()
	unit, err := workerManager.getCurrentUnit(ctx, task)
	require.Error(t.T(), err)
	require.Equal(t.T(), unit, libModel.WorkerType(0))
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Twice()
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMDump)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMLoad)

	task.Cfg.TaskMode = dmconfig.ModeAll
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMDump)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMLoad)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMSync)

	task.Cfg.TaskMode = dmconfig.ModeIncrement
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMSync)
	mockAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMSync)
}

func (t *testDMJobmasterSuite) TestCheckAndScheduleWorkers() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jobCfg.TaskMode = dmconfig.ModeFull
	job := metadata.NewJob(jobCfg)
	checkpointAgent := &MockCheckpointAgent{}
	workerAgent := &MockWorkerAgent{}
	workerManager := NewWorkerManager(nil, nil, workerAgent, checkpointAgent)

	// new tasks
	worker1 := "worker1"
	worker2 := "worker2"
	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID
	checkpointError := errors.New("checkpoint error")
	createError := errors.New("create error")
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, checkpointError).Once()
	workerAgent.SetCreateResult([]CreateResult{{worker1, nil}})
	require.EqualError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job), checkpointError.Error())
	wokerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 1)

	// check again
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	workerAgent.SetCreateResult([]CreateResult{{worker2, createError}})
	require.EqualError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job), createError.Error())
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	workerStatus1 := wokerStatusMap[source1]
	workerStatus2 := wokerStatusMap[source2]

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
	workerStatus3 := runtime.InitWorkerStatus(source1, lib.WorkerDMLoad, worker3)
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatus1.Stage = runtime.WorkerFinished
	workerAgent.SetCreateResult([]CreateResult{{worker3, nil}})
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1].ID, workerStatus3.ID)
	require.Equal(t.T(), wokerStatusMap[source2].ID, workerStatus2.ID)

	// unexpected
	worker4 := "worker3"
	workerStatus3.Stage = runtime.WorkerOffline
	workerStatus4 := runtime.InitWorkerStatus(source1, lib.WorkerDMLoad, worker4)
	workerManager.UpdateWorkerStatus(workerStatus3)
	workerAgent.SetCreateResult([]CreateResult{{worker4, nil}})
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
	jobStore := metadata.NewJobStore("worker_manager_test", kvmock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))

	checkpointAgent := &MockCheckpointAgent{}
	workerAgent := &MockWorkerAgent{}
	workerManager := NewWorkerManager(nil, jobStore, workerAgent, checkpointAgent)
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
	workerAgent.SetCreateResult([]CreateResult{{worker1, nil}, {"", createError}, {worker2, nil}})

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
				workerManager.Tick(ctx)
			}
		}
	}()

	// first check
	require.Eventually(t.T(), func() bool {
		workerAgent.Lock()
		defer workerAgent.Unlock()
		return len(workerAgent.createResults) == 0 && len(workerManager.WorkerStatus()) == 2
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
	workerStatus3 := runtime.InitWorkerStatus(source, lib.WorkerDMDump, worker3)
	// check by offline
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerManager.SetNextCheckTime(time.Now())
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Times(3)
	workerAgent.SetCreateResult([]CreateResult{{worker3, nil}})

	// scheduled eventually
	require.Eventually(t.T(), func() bool {
		return workerManager.WorkerStatus()[source].ID == workerStatus3.ID
	}, 5*time.Second, 100*time.Millisecond)
	workerStatus3.Stage = runtime.WorkerOnline
	workerManager.UpdateWorkerStatus(workerStatus3)

	// mock remove task2 by update-job
	delete(job.Tasks, source2)
	jobStore.Put(context.Background(), job)
	destroyError := errors.New("destroy error")
	workerAgent.SetDestroyResult([]error{destroyError, nil})
	// check by update-job
	workerManager.SetNextCheckTime(time.Now())
	// removed eventually
	require.Eventually(t.T(), func() bool {
		return len(workerManager.WorkerStatus()) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// mock task1 finished
	worker4 := "worker4"
	workerStatus := workerManager.WorkerStatus()[source1]
	workerStatus.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus)
	workerStatus4 := runtime.InitWorkerStatus(source1, lib.WorkerDMLoad, worker4)
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
	checkpointAgent.On("IsFresh", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
	workerAgent.SetCreateResult([]CreateResult{{worker4, nil}})
	// check by finished
	workerManager.SetNextCheckTime(time.Now())
	// scheduled eventually
	require.Eventually(t.T(), func() bool {
		return workerManager.WorkerStatus()[source1].ID == workerStatus4.ID
	}, 5*time.Second, 100*time.Millisecond)

	// mock delete job
	jobStore.Delete(ctx)
	workerAgent.SetDestroyResult([]error{destroyError, nil})
	// check by delete
	workerManager.SetNextCheckTime(time.Now())
	// deleted eventually
	require.Eventually(t.T(), func() bool {
		workerAgent.Lock()
		defer workerAgent.Unlock()
		return len(workerAgent.destroyResults) == 0 && len(workerManager.WorkerStatus()) == 0
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

type CreateResult struct {
	workerID libModel.WorkerID
	err      error
}

type MockWorkerAgent struct {
	sync.Mutex
	destroyResults []error
	createResults  []CreateResult
	stages         map[string]metadata.TaskStage
}

func (mockAgent *MockWorkerAgent) SetDestroyResult(results []error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.destroyResults = append(mockAgent.destroyResults, results...)
}

func (mockAgent *MockWorkerAgent) SetCreateResult(results []CreateResult) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.createResults = append(mockAgent.createResults, results...)
}

func (mockAgent *MockWorkerAgent) SetStages(stages map[string]metadata.TaskStage) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.stages = stages
}

func (mockAgent *MockWorkerAgent) CreateWorker(ctx context.Context, taskID string, workerType lib.WorkerType, taskCfg *config.TaskCfg, resources ...resourcemeta.ResourceID) (libModel.WorkerID, error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.createResults) == 0 {
		panic("no result in mock agent")
	}
	result := mockAgent.createResults[0]
	mockAgent.createResults = mockAgent.createResults[1:]
	return result.workerID, result.err
}

func (mockAgent *MockWorkerAgent) StopWorker(ctx context.Context, taskID string, workerID libModel.WorkerID) error {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.destroyResults) == 0 {
		panic("no result in mock agent")
	}
	result := mockAgent.destroyResults[0]
	mockAgent.destroyResults = mockAgent.destroyResults[1:]
	return result
}
