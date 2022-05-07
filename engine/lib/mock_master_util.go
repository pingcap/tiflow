package lib

// This file provides helper function to let the implementation of MasterImpl
// can finish its unit tests.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib/metadata"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/errors"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

func MockBaseMaster(id libModel.MasterID, masterImpl MasterImpl) *DefaultBaseMaster {
	ctx := dcontext.Background()
	dp := deps.NewDeps()
	cli, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	err = dp.Provide(func() masterParamListForTest {
		return masterParamListForTest{
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MessageSender:         p2p.NewMockMessageSender(),
			FrameMetaClient:       cli,
			UserRawKVClient:       mockkv.NewMetaMock(),
			ExecutorClientManager: client.NewClientManager(),
			ServerMasterClient:    &client.MockServerMasterClient{},
		}
	})
	if err != nil {
		panic(err)
	}

	ctx = ctx.WithDeps(dp)

	ret := NewBaseMaster(
		ctx,
		masterImpl,
		id)

	return ret.(*DefaultBaseMaster)
}

func MockBaseMasterCreateWorker(
	t *testing.T,
	master *DefaultBaseMaster,
	workerType libModel.WorkerType,
	config WorkerConfig,
	cost model.RescUnit,
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
	executorID model.ExecutorID,
	resources []resourcemeta.ResourceID,
) {
	master.uuidGen = uuid.NewMock()
	expectedSchedulerReq := &pb.ScheduleTaskRequest{
		TaskId:               workerID,
		Cost:                 int64(cost),
		ResourceRequirements: resources,
	}
	master.serverMasterClient.(*client.MockServerMasterClient).On(
		"ScheduleTask",
		mock.Anything,
		expectedSchedulerReq,
		mock.Anything).Return(
		&pb.ScheduleTaskResponse{
			ExecutorId: string(executorID),
		}, nil)

	mockExecutorClient := &client.MockExecutorClient{}
	err := master.executorClientManager.(*client.Manager).AddExecutorClient(executorID, mockExecutorClient)
	require.NoError(t, err)
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	mockExecutorClient.On("DispatchTask",
		mock.Anything,
		&client.DispatchTaskArgs{
			WorkerID:     workerID,
			MasterID:     masterID,
			WorkerType:   int64(workerType),
			WorkerConfig: configBytes,
		}, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			startWorker := args.Get(2).(client.StartWorkerCallback)
			startWorker()
		})

	master.uuidGen.(*uuid.MockGenerator).Push(workerID)
}

func MockBaseMasterCreateWorkerMetScheduleTaskError(
	t *testing.T,
	master *DefaultBaseMaster,
	workerType libModel.WorkerType,
	config WorkerConfig,
	cost model.RescUnit,
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
	executorID model.ExecutorID,
) {
	master.uuidGen = uuid.NewMock()
	expectedSchedulerReq := &pb.ScheduleTaskRequest{
		TaskId: workerID,
		Cost:   int64(cost),
	}
	master.serverMasterClient.(*client.MockServerMasterClient).On(
		"ScheduleTask",
		mock.Anything,
		expectedSchedulerReq,
		mock.Anything).Return(
		&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs())
	master.uuidGen.(*uuid.MockGenerator).Push(workerID)
}

func MockBaseMasterWorkerHeartbeat(
	t *testing.T,
	master *DefaultBaseMaster,
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
	executorID p2p.NodeID,
) {
	err := master.messageHandlerManager.(*p2p.MockMessageHandlerManager).InvokeHandler(
		t,
		libModel.HeartbeatPingTopic(masterID),
		executorID,
		&libModel.HeartbeatPingMessage{
			SendTime:     clock.MonoNow(),
			FromWorkerID: workerID,
			Epoch:        master.currentEpoch.Load(),
		})

	require.NoError(t, err)
}

func MockBaseMasterWorkerUpdateStatus(
	ctx context.Context,
	t *testing.T,
	master *DefaultBaseMaster,
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
	executorID p2p.NodeID,
	status *libModel.WorkerStatus,
) {
	workerMetaClient := metadata.NewWorkerMetadataClient(masterID, master.frameMetaClient)
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
