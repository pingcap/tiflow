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

// This file provides helper function to let the implementation of MasterImpl
// can finish its unit tests.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
)

// MockBaseMaster returns a mock DefaultBaseMaster
func MockBaseMaster(t *testing.T, id frameModel.MasterID, masterImpl MasterImpl) *DefaultBaseMaster {
	ctx := dcontext.Background()
	dp := deps.NewDeps()
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	err = dp.Provide(func() masterParamListForTest {
		return masterParamListForTest{
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MessageSender:         p2p.NewMockMessageSender(),
			FrameMetaClient:       cli,
			BusinessClientConn:    metaMock.NewMockClientConn(),
			ExecutorGroup:         client.NewMockExecutorGroup(),
			ServerMasterClient:    client.NewMockServerMasterClient(gomock.NewController(t)),
		}
	})
	require.NoError(t, err)

	ctx = ctx.WithDeps(dp)
	ctx.Environ.NodeID = "test-node-id"
	ctx.Environ.Addr = "127.0.0.1:10000"
	ctx.ProjectInfo = tenant.TestProjectInfo
	epoch, err := cli.GenEpoch(ctx)
	require.NoError(t, err)
	masterMeta := &frameModel.MasterMeta{
		ProjectID: tenant.TestProjectInfo.UniqueID(),
		Addr:      ctx.Environ.Addr,
		NodeID:    ctx.Environ.NodeID,
		ID:        id,
		Type:      frameModel.FakeJobMaster,
		Epoch:     epoch,
		State:     frameModel.MasterStateUninit,
	}
	masterMetaBytes, err := masterMeta.Marshal()
	require.NoError(t, err)
	ctx.Environ.MasterMetaBytes = masterMetaBytes

	ret := NewBaseMaster(
		ctx,
		masterImpl,
		id,
		frameModel.FakeTask,
	)

	return ret.(*DefaultBaseMaster)
}

// MockMasterPrepareMeta simulates the meta persistence for MockMasterImpl
func MockMasterPrepareMeta(ctx context.Context, t *testing.T, master *MockMasterImpl) {
	err := master.GetFrameMetaClient().UpsertJob(
		ctx, master.DefaultBaseMaster.MasterMeta())
	require.NoError(t, err)
}

// MockBaseMasterCreateWorker mocks to create worker in base master
func MockBaseMasterCreateWorker(
	t *testing.T,
	master *DefaultBaseMaster,
	workerType frameModel.WorkerType,
	config WorkerConfig,
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	executorID model.ExecutorID,
	resources []resModel.ResourceID,
	workerEpoch frameModel.Epoch,
) {
	master.uuidGen = uuid.NewMock()
	expectedSchedulerReq := &pb.ScheduleTaskRequest{
		TaskId:    workerID,
		Resources: resModel.ToResourceRequirement(masterID, resources...),
	}
	master.serverMasterClient.(*client.MockServerMasterClient).EXPECT().
		ScheduleTask(gomock.Any(), gomock.Eq(expectedSchedulerReq)).
		Return(&pb.ScheduleTaskResponse{
			ExecutorId: string(executorID),
		}, nil).Times(1)

	mockExecutorClient := client.NewMockExecutorClient(gomock.NewController(t))
	master.executorGroup.(*client.MockExecutorGroup).AddClient(executorID, mockExecutorClient)
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	mockExecutorClient.EXPECT().DispatchTask(gomock.Any(),
		gomock.Eq(&client.DispatchTaskArgs{
			ProjectInfo:  tenant.TestProjectInfo,
			WorkerID:     workerID,
			MasterID:     masterID,
			WorkerType:   int64(workerType),
			WorkerConfig: configBytes,
			WorkerEpoch:  workerEpoch,
		}), gomock.Any()).Do(
		func(
			ctx context.Context,
			args *client.DispatchTaskArgs,
			start client.StartWorkerCallback,
		) {
			start()
		}).Times(1).Return(nil)

	master.uuidGen.(*uuid.MockGenerator).Push(workerID)
}

// MockBaseMasterCreateWorkerMetScheduleTaskError mocks ScheduleTask meets error
func MockBaseMasterCreateWorkerMetScheduleTaskError(
	t *testing.T,
	master *DefaultBaseMaster,
	workerType frameModel.WorkerType,
	config WorkerConfig,
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	executorID model.ExecutorID,
) {
	master.uuidGen = uuid.NewMock()
	expectedSchedulerReq := &pb.ScheduleTaskRequest{
		TaskId: workerID,
	}
	master.serverMasterClient.(*client.MockServerMasterClient).EXPECT().
		ScheduleTask(gomock.Any(), gomock.Eq(expectedSchedulerReq)).
		Return(nil, errors.ErrClusterResourceNotEnough.FastGenByArgs()).
		Times(1)

	master.uuidGen.(*uuid.MockGenerator).Push(workerID)
}

// MockBaseMasterWorkerHeartbeat sends HeartbeatPingMessage with mock message handler
func MockBaseMasterWorkerHeartbeat(
	t *testing.T,
	master *DefaultBaseMaster,
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	executorID p2p.NodeID,
) error {
	worker, ok := master.workerManager.GetWorkers()[workerID]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(workerID)
	}
	workerEpoch := worker.Status().Epoch
	err := master.messageHandlerManager.(*p2p.MockMessageHandlerManager).InvokeHandler(
		t,
		frameModel.HeartbeatPingTopic(masterID),
		executorID,
		&frameModel.HeartbeatPingMessage{
			SendTime:     clock.MonoNow(),
			FromWorkerID: workerID,
			Epoch:        master.currentEpoch.Load(),
			WorkerEpoch:  workerEpoch,
		})
	return err
}

// MockBaseMasterWorkerUpdateStatus mocks to store status in metastore and sends
// WorkerStatusMessage.
func MockBaseMasterWorkerUpdateStatus(
	ctx context.Context,
	t *testing.T,
	master *DefaultBaseMaster,
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	executorID p2p.NodeID,
	status *frameModel.WorkerStatus,
) {
	workerMetaClient := metadata.NewWorkerStatusClient(masterID, master.frameMetaClient)
	err := workerMetaClient.Store(ctx, status)
	require.NoError(t, err)

	err = master.messageHandlerManager.(*p2p.MockMessageHandlerManager).InvokeHandler(
		t,
		statusutil.WorkerStatusTopic(masterID),
		executorID,
		&statusutil.WorkerStatusMessage{
			Worker:      workerID,
			MasterEpoch: master.currentEpoch.Load(),
			Status:      status,
		})
	require.NoError(t, err)
}
