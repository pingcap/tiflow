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
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/errors"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

func MockBaseMaster(id MasterID, masterImpl MasterImpl) *DefaultBaseMaster {
	ctx := dcontext.Background()
	dp := deps.NewDeps()
	err := dp.Provide(func() masterParamListForTest {
		return masterParamListForTest{
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MessageSender:         p2p.NewMockMessageSender(),
			MetaKVClient:          mockkv.NewMetaMock(),
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
	workerType WorkerType,
	config WorkerConfig,
	cost model.RescUnit,
	masterID MasterID,
	workerID WorkerID,
	executorID model.ExecutorID,
) {
	master.uuidGen = uuid.NewMock()

	expectedSchedulerReq := &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
		Task: &pb.TaskRequest{
			Id: 0,
		},
		Cost: int64(cost),
	}}}
	master.serverMasterClient.(*client.MockServerMasterClient).On(
		"ScheduleTask",
		mock.Anything,
		expectedSchedulerReq,
		mock.Anything).Return(
		&pb.TaskSchedulerResponse{
			Schedule: map[int64]*pb.ScheduleResult{
				0: {
					ExecutorId: string(executorID),
				},
			},
		}, nil)

	mockExecutorClient := &client.MockExecutorClient{}
	err := master.executorClientManager.(*client.Manager).AddExecutorClient(executorID, mockExecutorClient)
	require.NoError(t, err)
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	mockExecutorClient.On("Send",
		mock.Anything,
		&client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerType),
				TaskConfig: configBytes,
				MasterId:   masterID,
				WorkerId:   workerID,
			},
		}).Return(&client.ExecutorResponse{Resp: &pb.DispatchTaskResponse{
		ErrorCode: 1,
	}}, nil)

	master.uuidGen.(*uuid.MockGenerator).Push(workerID)
}

func MockBaseMasterCreateWorkerMetScheduleTaskError(
	t *testing.T,
	master *DefaultBaseMaster,
	workerType WorkerType,
	config WorkerConfig,
	cost model.RescUnit,
	masterID MasterID,
	workerID WorkerID,
	executorID model.ExecutorID,
) {
	master.uuidGen = uuid.NewMock()

	expectedSchedulerReq := &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
		Task: &pb.TaskRequest{
			Id: 0,
		},
		Cost: int64(cost),
	}}}
	master.serverMasterClient.(*client.MockServerMasterClient).On(
		"ScheduleTask",
		mock.Anything,
		expectedSchedulerReq,
		mock.Anything).Return(
		&pb.TaskSchedulerResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs())
	master.uuidGen.(*uuid.MockGenerator).Push(workerID)
}

func MockBaseMasterWorkerHeartbeat(
	t *testing.T,
	master *DefaultBaseMaster,
	masterID MasterID,
	workerID WorkerID,
	executorID p2p.NodeID,
) {
	err := master.messageHandlerManager.(*p2p.MockMessageHandlerManager).InvokeHandler(
		t,
		HeartbeatPingTopic(masterID),
		executorID,
		&HeartbeatPingMessage{
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
	masterID MasterID,
	workerID WorkerID,
	executorID p2p.NodeID,
	status *libModel.WorkerStatus,
) {
	workerMetaClient := NewWorkerMetadataClient(masterID, master.metaKVClient)
	err := workerMetaClient.Store(ctx, workerID, status)
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
