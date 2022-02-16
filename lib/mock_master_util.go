package lib

// This file provides helper function to let the implementation of MasterImpl
// can finish its unit tests.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

func MockBaseMaster(id MasterID, masterImpl MasterImpl) *DefaultBaseMaster {
	ret := NewBaseMaster(
		// ctx is nil for now
		// TODO refine this
		nil,
		masterImpl,
		id,
		p2p.NewMockMessageHandlerManager(),
		p2p.NewMockMessageSender(),
		metadata.NewMetaMock(),
		client.NewClientManager(),
		&client.MockServerMasterClient{})

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
		HeartbeatPingTopic(masterID, workerID),
		executorID,
		&HeartbeatPingMessage{
			SendTime:     clock.MonoNow(),
			FromWorkerID: workerID,
			Epoch:        master.currentEpoch.Load(),
		})

	require.NoError(t, err)
}
