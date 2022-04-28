package dm

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/hanfei1991/microcosm/lib/master"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
)

func TestUpdateWorkerHandle(t *testing.T) {
	messageAgent := NewMessageAgent(nil, "mock-jobmaster", nil)
	require.Equal(t, lenSyncMap(&messageAgent.sendHandles), 0)
	workerHandle1 := &master.MockHandle{WorkerID: "worker1", WorkerStatus: &libModel.WorkerStatus{}, IsTombstone: true}
	workerHandle2 := &master.MockHandle{WorkerID: "worker2", WorkerStatus: &libModel.WorkerStatus{}, IsTombstone: true}

	// add worker handle
	messageAgent.UpdateWorkerHandle("task1", workerHandle1)
	require.Equal(t, lenSyncMap(&messageAgent.sendHandles), 1)
	w, ok := messageAgent.sendHandles.Load("task1")
	require.True(t, ok)
	require.Equal(t, w, workerHandle1)
	messageAgent.UpdateWorkerHandle("task2", workerHandle2)
	require.Equal(t, lenSyncMap(&messageAgent.sendHandles), 2)
	w, ok = messageAgent.sendHandles.Load("task1")
	require.True(t, ok)
	require.Equal(t, w, workerHandle1)
	w, ok = messageAgent.sendHandles.Load("task2")
	require.True(t, ok)
	require.Equal(t, w, workerHandle2)

	// remove worker handle
	messageAgent.UpdateWorkerHandle("task3", nil)
	require.Equal(t, lenSyncMap(&messageAgent.sendHandles), 2)
	w, ok = messageAgent.sendHandles.Load("task1")
	require.True(t, ok)
	require.Equal(t, w, workerHandle1)
	w, ok = messageAgent.sendHandles.Load("task2")
	require.True(t, ok)
	require.Equal(t, w, workerHandle2)
	messageAgent.UpdateWorkerHandle("task2", nil)
	require.Equal(t, lenSyncMap(&messageAgent.sendHandles), 1)
	w, ok = messageAgent.sendHandles.Load("task1")
	require.True(t, ok)
	require.Equal(t, w, workerHandle1)

	// mock jobmaster recover
	initWorkerHandleMap := make(map[string]SendHandle)
	messageAgent.sendHandles.Range(func(key, value interface{}) bool {
		initWorkerHandleMap[key.(string)] = value.(SendHandle)
		return true
	})
	messageAgent = NewMessageAgent(initWorkerHandleMap, "mock-jobmaster", nil)
	require.Equal(t, lenSyncMap(&messageAgent.sendHandles), 1)
	w, ok = messageAgent.sendHandles.Load("task1")
	require.True(t, ok)
	require.Equal(t, w, workerHandle1)
}

func TestOperateWorker(t *testing.T) {
	mockMasterImpl := &MockMaster{}
	messageAgent := NewMessageAgent(nil, "mock-jobmaster", mockMasterImpl)
	task1 := "task1"
	worker1 := "worker1"
	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	taskCfgs := jobCfg.ToTaskConfigs()
	taskCfg := taskCfgs[jobCfg.Upstreams[0].SourceID]

	// create worker
	_, err := messageAgent.CreateWorker(context.Background(), task1, lib.WorkerDMDump, taskCfg)
	require.NoError(t, err)
	// create again
	_, err = messageAgent.CreateWorker(context.Background(), task1, lib.WorkerDMDump, taskCfg)
	require.NoError(t, err)
	// create again
	workerHandle := &master.MockHandle{WorkerID: "worker1", WorkerStatus: &libModel.WorkerStatus{}, IsTombstone: true}
	messageAgent.UpdateWorkerHandle(task1, workerHandle)
	_, err = messageAgent.CreateWorker(context.Background(), task1, lib.WorkerDMDump, taskCfg)
	require.EqualError(t, err, fmt.Sprintf("worker for task %s already exist", task1))

	// destroy worker
	require.EqualError(t, messageAgent.StopWorker(context.Background(), "task-not-exist", "worker-not-exist"), fmt.Sprintf("worker for task %s not exist", "task-not-exist"))
	require.EqualError(t, messageAgent.StopWorker(context.Background(), task1, "worker-not-exist"), fmt.Sprintf("worker for task %s mismatch: want %s, get %s", task1, "worker-not-exist", worker1))
	// worker offline
	require.Error(t, messageAgent.StopWorker(context.Background(), task1, worker1))
	// worker normal
	messageAgent.UpdateWorkerHandle(task1, &MockSender{id: worker1})
	require.NoError(t, messageAgent.StopWorker(context.Background(), task1, worker1))
}

func TestOperateTask(t *testing.T) {
	mockMasterImpl := &MockMaster{}
	messageAgent := NewMessageAgent(nil, "mock-jobmaster", mockMasterImpl)
	task1 := "task1"
	worker1 := "worker1"

	workerHandle := &master.MockHandle{WorkerID: "worker1", WorkerStatus: &libModel.WorkerStatus{}, IsTombstone: true}
	messageAgent.UpdateWorkerHandle(task1, workerHandle)
	// worker offline
	require.Error(t, messageAgent.OperateTask(context.Background(), task1, metadata.StagePaused))
	// worker normal
	messageAgent.UpdateWorkerHandle(task1, &MockSender{id: worker1})
	require.NoError(t, messageAgent.OperateTask(context.Background(), task1, metadata.StagePaused))
	// task not exist
	require.EqualError(t, messageAgent.OperateTask(context.Background(), "task-not-exist", metadata.StagePaused), fmt.Sprintf("worker for task %s not exist", "task-not-exist"))
	// wrong stage
	require.EqualError(t, messageAgent.OperateTask(context.Background(), task1, metadata.StageInit), fmt.Sprintf("invalid expected stage %d for task %s", metadata.StageInit, task1))
}

func TestOnWorkerMessage(t *testing.T) {
	messageAgent := NewMessageAgent(nil, "", nil)
	require.EqualError(t, messageAgent.OnWorkerMessage(dmpkg.MessageWithID{ID: 0, Message: "response"}), "request 0 not found")
}

func lenSyncMap(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}

type MockMaster struct{}

func (m *MockMaster) CreateWorker(workerType lib.WorkerType, config lib.WorkerConfig, cost model.RescUnit, resources ...resourcemeta.ResourceID) (libModel.WorkerID, error) {
	return "mock-worker", nil
}

func (m *MockMaster) CurrentEpoch() libModel.Epoch {
	return 0
}

type MockSender struct {
	id libModel.WorkerID
}

func (s *MockSender) ID() libModel.WorkerID {
	return s.id
}

func (s *MockSender) SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error {
	return nil
}
