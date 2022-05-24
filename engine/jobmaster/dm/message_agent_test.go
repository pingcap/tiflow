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
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/engine/lib/master"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
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
	taskCfgs := jobCfg.ToTaskCfgs()
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
