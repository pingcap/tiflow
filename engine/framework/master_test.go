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

package framework

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/uuid"
)

const (
	masterName            = "my-master"
	masterNodeName        = "node-1"
	executorNodeID1       = "node-exec-1"
	executorNodeID3       = "node-exec-3"
	workerTypePlaceholder = 999
	workerID1             = frameModel.WorkerID("worker-1")
)

type dummyConfig struct {
	param int
}

func prepareMeta(ctx context.Context, t *testing.T, metaclient pkgOrm.Client) {
	masterInfo := &frameModel.MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		StatusCode: frameModel.MasterStatusUninit,
	}
	err := metaclient.UpsertJob(ctx, masterInfo)
	require.NoError(t, err)
}

func TestMasterInit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	prepareMeta(ctx, t, master.GetFrameMetaClient())

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	resp, err := master.GetFrameMetaClient().GetJobByID(ctx, masterName)
	require.NoError(t, err)
	require.Equal(t, frameModel.MasterStatusInit, resp.StatusCode)

	master.On("CloseImpl", mock.Anything).Return(nil)
	err = master.Close(ctx)
	require.NoError(t, err)

	// Restart the master
	master.Reset()
	master.timeoutConfig.WorkerTimeoutDuration = 10 * time.Millisecond
	master.timeoutConfig.WorkerTimeoutGracefulDuration = 10 * time.Millisecond

	master.On("OnMasterRecovered", mock.Anything).Return(nil)
	err = master.Init(ctx)
	require.NoError(t, err)

	master.On("CloseImpl", mock.Anything).Return(nil)
	err = master.Close(ctx)
	require.NoError(t, err)

	master.AssertExpectations(t)
}

func TestMasterPollAndClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	prepareMeta(ctx, t, master.GetFrameMetaClient())

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	master.On("Tick", mock.Anything).Return(nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := master.Poll(ctx)
			if err != nil {
				if errors.ErrMasterClosed.Equal(err) {
					return
				}
			}
			require.NoError(t, err)
		}
	}()

	require.Eventually(t, func() bool {
		return master.TickCount() > 10
	}, time.Millisecond*2000, time.Millisecond*10)

	master.On("CloseImpl", mock.Anything).Return(nil)
	err = master.Close(ctx)
	require.NoError(t, err)

	master.AssertExpectations(t)
	wg.Wait()
}

func TestMasterCreateWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	master.timeoutConfig.WorkerTimeoutDuration = time.Second * 1000
	master.timeoutConfig.MasterHeartbeatCheckLoopInterval = time.Millisecond * 10
	master.uuidGen = uuid.NewMock()
	prepareMeta(ctx, t, master.GetFrameMetaClient())

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	MockBaseMasterCreateWorker(
		t,
		master.DefaultBaseMaster,
		workerTypePlaceholder,
		&dummyConfig{param: 1},
		100,
		masterName,
		workerID1,
		executorNodeID1,
		[]resourcemeta.ResourceID{"resource-1", "resource-2"})

	workerID, err := master.CreateWorker(
		workerTypePlaceholder,
		&dummyConfig{param: 1},
		100,
		"resource-1",
		"resource-2")
	require.NoError(t, err)
	require.Equal(t, workerID1, workerID)

	master.On("OnWorkerDispatched", mock.AnythingOfType("*master.runningHandleImpl"), nil).Return(nil)
	master.On("OnWorkerOnline", mock.AnythingOfType("*master.runningHandleImpl")).Return(nil)

	require.Eventuallyf(t, func() bool {
		MockBaseMasterWorkerHeartbeat(t, master.DefaultBaseMaster, masterName, workerID1, executorNodeID1)
		master.On("Tick", mock.Anything).Return(nil)
		err = master.Poll(ctx)
		require.NoError(t, err)
		return master.onlineWorkerCount.Load() == 1
	}, time.Second*10, time.Millisecond*10, "final worker count %d", master.onlineWorkerCount.Load())

	workerList := master.GetWorkers()
	require.Len(t, workerList, 1)
	require.Contains(t, workerList, workerID)

	workerMetaClient := metadata.NewWorkerMetadataClient(masterName, master.GetFrameMetaClient())
	dummySt := &dummyStatus{Val: 4}
	ext, err := dummySt.Marshal()
	require.NoError(t, err)
	err = workerMetaClient.Store(ctx, &frameModel.WorkerStatus{
		Code:     frameModel.WorkerStatusNormal,
		ExtBytes: ext,
	})
	require.NoError(t, err)

	master.On("OnWorkerStatusUpdated", mock.Anything, &frameModel.WorkerStatus{
		Code:     frameModel.WorkerStatusNormal,
		ExtBytes: ext,
	}).Return(nil)

	err = master.messageHandlerManager.InvokeHandler(
		t,
		statusutil.WorkerStatusTopic(masterName),
		masterName,
		&statusutil.WorkerStatusMessage{
			Worker:      workerID1,
			MasterEpoch: master.currentEpoch.Load(),
			Status: &frameModel.WorkerStatus{
				Code:     frameModel.WorkerStatusNormal,
				ExtBytes: ext,
			},
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		err := master.Poll(ctx)
		require.NoError(t, err)

		select {
		case updatedStatus := <-master.updatedStatuses:
			require.Equal(t, &frameModel.WorkerStatus{
				Code:     frameModel.WorkerStatusNormal,
				ExtBytes: ext,
			}, updatedStatus)
		default:
			return false
		}

		status := master.GetWorkers()[workerID1].Status()
		return status.Code == frameModel.WorkerStatusNormal
	}, 1*time.Second, 10*time.Millisecond)
}

func TestMasterCreateWorkerMetError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	master.timeoutConfig.MasterHeartbeatCheckLoopInterval = time.Millisecond * 10
	master.uuidGen = uuid.NewMock()
	prepareMeta(ctx, t, master.GetFrameMetaClient())

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	MockBaseMasterCreateWorkerMetScheduleTaskError(
		t,
		master.DefaultBaseMaster,
		workerTypePlaceholder,
		&dummyConfig{param: 1},
		100,
		masterName,
		workerID1,
		executorNodeID1)

	master.On("OnWorkerDispatched", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			err := args.Error(1)
			require.Regexp(t, ".*ErrClusterResourceNotEnough.*", err)
		})

	_, err = master.CreateWorker(workerTypePlaceholder, &dummyConfig{param: 1}, 100)
	require.NoError(t, err)
}

func TestPrepareWorkerConfig(t *testing.T) {
	t.Parallel()

	master := &DefaultBaseMaster{
		uuidGen: uuid.NewMock(),
	}

	type fakeConfig struct {
		JobName     string `json:"job-name"`
		WorkerCount int    `json:"worker-count"`
	}
	fakeWorkerCfg := &fakeConfig{"fake-job", 3}
	fakeCfgBytes := []byte(`{"job-name":"fake-job","worker-count":3}`)
	fakeWorkerID := "worker-1"
	master.uuidGen.(*uuid.MockGenerator).Push(fakeWorkerID)
	testCases := []struct {
		workerType frameModel.WorkerType
		config     WorkerConfig
		// expected return result
		rawConfig []byte
		workerID  string
	}{
		{
			FakeJobMaster, &frameModel.MasterMetaKVData{ID: "master-1", Config: fakeCfgBytes},
			fakeCfgBytes, "master-1",
		},
		{
			FakeTask, fakeWorkerCfg,
			fakeCfgBytes, fakeWorkerID,
		},
	}
	for _, tc := range testCases {
		rawConfig, workerID, err := master.prepareWorkerConfig(tc.workerType, tc.config)
		require.NoError(t, err)
		require.Equal(t, tc.rawConfig, rawConfig)
		require.Equal(t, tc.workerID, workerID)
	}
}
