package dm

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
)

func (t *testDMJobmasterSuite) TestUpdateWorkerStatus() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("worker_manager_test", mock.NewMetaMock())
	require.NoError(t.T(), jobStore.Put(context.Background(), job))
	workerManager := NewWorkerManager(nil, jobStore, nil, nil)

	require.Len(t.T(), workerManager.WorkerStatus(), 0)

	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID
	workerStatus1 := runtime.NewWorkerStatus(source1, lib.WorkerDMDump, "worker-id-1", runtime.WorkerCreating)
	workerStatus2 := runtime.NewWorkerStatus(source2, lib.WorkerDMDump, "worker-id-1", runtime.WorkerCreating)

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
}

func (t *testDMJobmasterSuite) TestClearWorkerStatus() {
	mockAgent := &MockWorkerAgent{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source1 := "source1"
	source2 := "source2"
	workerStatus1 := runtime.NewWorkerStatus(source1, lib.WorkerDMDump, "worker-id-1", runtime.WorkerCreating)
	workerStatus2 := runtime.NewWorkerStatus(source2, lib.WorkerDMDump, "worker-id-2", runtime.WorkerCreating)

	workerManager := NewWorkerManager([]runtime.WorkerStatus{workerStatus1, workerStatus2}, nil, mockAgent, nil)
	require.Len(t.T(), workerManager.WorkerStatus(), 2)

	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 2)

	workerStatus1.Stage = runtime.WorkerOffline
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerManager.removeOfflineWorkers()
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	job := metadata.NewJob(&config.JobCfg{})
	job.Tasks[source2] = metadata.NewTask(&config.TaskCfg{})
	err := workerManager.destroyUnneededWorkers(ctx, job)
	require.NoError(t.T(), err)
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	delete(job.Tasks, source2)
	destroyError := errors.New("destroy error")
	mockAgent.SetDestroyResult([]error{destroyError})
	err = workerManager.destroyUnneededWorkers(ctx, job)
	require.EqualError(t.T(), err, destroyError.Error())
	require.Len(t.T(), workerManager.WorkerStatus(), 1)

	mockAgent.SetDestroyResult([]error{nil})
	err = workerManager.destroyUnneededWorkers(ctx, job)
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

	workerStatus1 := runtime.NewWorkerStatus(task1, lib.WorkerDMDump, worker1, runtime.WorkerCreating)
	mockAgent.SetCreateResult([]CreateResult{{worker1, createError}})
	require.EqualError(t.T(), workerManager.createWorker(ctx, task1, lib.WorkerDMDump, &config.TaskCfg{}), createError.Error())
	workerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 1)
	require.Contains(t.T(), workerStatusMap, task1)
	require.Equal(t.T(), workerStatusMap[task1], workerStatus1)

	task2 := "task-2"
	worker2 := "worker2"
	workerStatus2 := runtime.NewWorkerStatus(task2, lib.WorkerDMLoad, worker2, runtime.WorkerCreating)
	mockAgent.SetCreateResult([]CreateResult{{worker2, nil}})
	require.NoError(t.T(), workerManager.createWorker(ctx, task2, lib.WorkerDMLoad, &config.TaskCfg{}))
	workerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), workerStatusMap, 2)
	require.Contains(t.T(), workerStatusMap, task1)
	require.Contains(t.T(), workerStatusMap, task2)
	require.Equal(t.T(), workerStatusMap[task1], workerStatus1)
	require.Equal(t.T(), workerStatusMap[task2], workerStatus2)
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
	mockAgent.SetResult([]CheckpointResult{{false, errors.New("checkpoint error")}})
	unit, err := workerManager.getCurrentUnit(ctx, task)
	require.Error(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerType(0))
	mockAgent.SetResult([]CheckpointResult{{true, nil}, {true, nil}})
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMDump)
	mockAgent.SetResult([]CheckpointResult{{false, nil}})
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMLoad)

	task.Cfg.TaskMode = dmconfig.ModeAll
	mockAgent.SetResult([]CheckpointResult{{true, nil}, {true, nil}, {true, nil}})
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMDump)
	mockAgent.SetResult([]CheckpointResult{{true, nil}, {false, nil}})
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMLoad)
	mockAgent.SetResult([]CheckpointResult{{false, nil}})
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMSync)

	task.Cfg.TaskMode = dmconfig.ModeIncrement
	mockAgent.SetResult([]CheckpointResult{{true, nil}})
	unit, err = workerManager.getCurrentUnit(ctx, task)
	require.NoError(t.T(), err)
	require.Equal(t.T(), unit, lib.WorkerDMSync)
	mockAgent.SetResult([]CheckpointResult{{false, nil}})
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
	checkpointAgent.SetResult([]CheckpointResult{{true, nil}, {true, nil}, {true, nil}, {false, checkpointError}})
	workerAgent.SetCreateResult([]CreateResult{{worker1, nil}})
	require.EqualError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job), checkpointError.Error())
	wokerStatusMap := workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 1)

	// check again
	checkpointAgent.SetResult([]CheckpointResult{{true, nil}, {true, nil}, {true, nil}})
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
	workerStatus3 := runtime.NewWorkerStatus(source1, lib.WorkerDMLoad, worker3, runtime.WorkerCreating)
	workerManager.UpdateWorkerStatus(workerStatus1)
	workerStatus1.Stage = runtime.WorkerFinished
	workerAgent.SetCreateResult([]CreateResult{{worker3, nil}})
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1], workerStatus3)
	require.Equal(t.T(), wokerStatusMap[source2], workerStatus2)

	// unexpected
	worker4 := "worker3"
	workerStatus3.Stage = runtime.WorkerOffline
	workerStatus4 := runtime.NewWorkerStatus(source1, lib.WorkerDMLoad, worker4, runtime.WorkerCreating)
	workerManager.UpdateWorkerStatus(workerStatus3)
	workerAgent.SetCreateResult([]CreateResult{{worker4, nil}})
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1], workerStatus4)
	require.Equal(t.T(), wokerStatusMap[source2], workerStatus2)

	// finished
	workerStatus4.Stage = runtime.WorkerFinished
	workerManager.UpdateWorkerStatus(workerStatus4)
	require.NoError(t.T(), workerManager.checkAndScheduleWorkers(context.Background(), job))
	wokerStatusMap = workerManager.WorkerStatus()
	require.Len(t.T(), wokerStatusMap, 2)
	require.Contains(t.T(), wokerStatusMap, source1)
	require.Contains(t.T(), wokerStatusMap, source2)
	require.Equal(t.T(), wokerStatusMap[source1], workerStatus4)
	require.Equal(t.T(), wokerStatusMap[source2], workerStatus2)
}

func (t *testDMJobmasterSuite) TestWorkerManager() {
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("worker_manager_test", mock.NewMetaMock())
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
	checkpointAgent.SetResult([]CheckpointResult{{true, nil}, {true, nil}, {true, nil}, {false, checkpointError}, {true, nil}, {true, nil}, {true, nil}, {true, nil}, {true, nil}, {true, nil}})
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
		checkpointAgent.Lock()
		defer checkpointAgent.Unlock()
		workerAgent.Lock()
		defer workerAgent.Unlock()
		return len(checkpointAgent.results) == 0 && len(workerAgent.createResults) == 0 && len(workerManager.WorkerStatus()) == 2
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
	time.Sleep(1)

	// worker2 offline
	source := workerStatus2.TaskID
	worker3 := "worker3"
	workerStatus2.Stage = runtime.WorkerOffline
	workerStatus3 := runtime.NewWorkerStatus(source, lib.WorkerDMDump, worker3, runtime.WorkerCreating)
	// check by offline
	workerManager.UpdateWorkerStatus(workerStatus2)
	workerManager.SetNextCheckTime(time.Now())
	checkpointAgent.SetResult([]CheckpointResult{{true, nil}, {true, nil}, {true, nil}})
	workerAgent.SetCreateResult([]CreateResult{{worker3, nil}})

	// scheduled eventually
	require.Eventually(t.T(), func() bool {
		return workerManager.WorkerStatus()[source] == workerStatus3
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
	workerStatus4 := runtime.NewWorkerStatus(source1, lib.WorkerDMLoad, worker4, runtime.WorkerCreating)
	checkpointAgent.SetResult([]CheckpointResult{{true, nil}, {false, nil}})
	workerAgent.SetCreateResult([]CreateResult{{worker4, nil}})
	// check by finished
	workerManager.SetNextCheckTime(time.Now())
	// scheduled eventually
	require.Eventually(t.T(), func() bool {
		return workerManager.WorkerStatus()[source1] == workerStatus4
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
	workerID lib.WorkerID
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

func (mockAgent *MockWorkerAgent) CreateWorker(ctx context.Context, taskID string, workerType lib.WorkerType, taskCfg *config.TaskCfg) (lib.WorkerID, error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.createResults) == 0 {
		panic("no result in mock agent")
	}
	result := mockAgent.createResults[0]
	mockAgent.createResults = mockAgent.createResults[1:]
	return result.workerID, result.err
}

func (mockAgent *MockWorkerAgent) DestroyWorker(ctx context.Context, taskID string, workerID lib.WorkerID) error {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.destroyResults) == 0 {
		panic("no result in mock agent")
	}
	result := mockAgent.destroyResults[0]
	mockAgent.destroyResults = mockAgent.destroyResults[1:]
	return result
}

type CheckpointResult struct {
	isFresh bool
	err     error
}

type MockCheckpointAgent struct {
	sync.Mutex
	results []CheckpointResult
}

func (mockAgent *MockCheckpointAgent) SetResult(results []CheckpointResult) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.results = append(mockAgent.results, results...)
}

func (mockAgent *MockCheckpointAgent) IsFresh(ctx context.Context, workerType lib.WorkerType, taskCfg *metadata.Task) (bool, error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.results) == 0 {
		panic("no result in mock agent")
	}
	result := mockAgent.results[0]
	mockAgent.results = mockAgent.results[1:]
	return result.isFresh, result.err
}
