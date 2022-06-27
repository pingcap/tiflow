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

package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/ctxmu"
	"github.com/pingcap/tiflow/engine/pkg/dm"
	resManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/uuid"
)

func TestJobManagerSubmitJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		uuidGen:           uuid.NewGenerator(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     int32(engineModel.JobTypeCVSDemo),
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)
	err = mockMaster.Poll(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return mgr.JobFsm.JobCount(pb.QueryJobResponse_online) == 0 &&
			mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched) == 1 &&
			mgr.JobFsm.JobCount(pb.QueryJobResponse_pending) == 0
	}, time.Second*2, time.Millisecond*20)
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: resp.JobId})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_dispatched, queryResp.Status)
}

type mockBaseMasterCreateWorkerFailed struct {
	*framework.MockMasterImpl
}

func (m *mockBaseMasterCreateWorkerFailed) CreateWorker(
	workerType framework.WorkerType,
	config framework.WorkerConfig,
	cost model.RescUnit,
	resources ...resourcemeta.ResourceID,
) (frameModel.WorkerID, error) {
	return "", errors.ErrMasterConcurrencyExceeded.FastGenByArgs()
}

func TestCreateWorkerReturnError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterImpl := framework.NewMockMasterImpl("", "create-worker-with-error")
	mockMaster := &mockBaseMasterCreateWorkerFailed{
		MockMasterImpl: masterImpl,
	}
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     int32(engineModel.JobTypeCVSDemo),
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.NotNil(t, resp.Err)
	require.Regexp(t, ".*ErrMasterConcurrencyExceeded.*", resp.Err.Message)
}

func TestJobManagerPauseJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl("", "pause-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImplV2{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		jobStatusChangeMu: ctxmu.New(),
	}

	pauseWorkerID := "pause-worker-id"
	meta := &frameModel.MasterMetaKVData{ID: pauseWorkerID}
	mgr.JobFsm.JobDispatched(meta, false)

	mockWorkerHandle := &framework.MockHandle{WorkerID: pauseWorkerID, ExecutorID: "executor-1"}
	err := mgr.JobFsm.JobOnline(mockWorkerHandle)
	require.Nil(t, err)

	req := &pb.PauseJobRequest{
		JobId: pauseWorkerID,
	}
	resp := mgr.PauseJob(ctx, req)
	require.Nil(t, resp.Err)

	require.Equal(t, 1, mockWorkerHandle.SendMessageCount())

	req.JobId = pauseWorkerID + "-unknown"
	resp = mgr.PauseJob(ctx, req)
	require.NotNil(t, resp.Err)
	require.Equal(t, pb.ErrorCode_UnKnownJob, resp.Err.Code)
}

func TestJobManagerCancelJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl("", "cancel-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImplV2{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}

	err := mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMetaKVData{
		ID:         "job-to-be-canceled",
		Tp:         framework.FakeJobMaster,
		StatusCode: frameModel.MasterStatusStopped,
	})
	require.NoError(t, err)

	err = mgr.OnMasterRecovered(ctx)
	require.NoError(t, err)

	resp := mgr.CancelJob(ctx, &pb.CancelJobRequest{
		JobId: "job-to-be-canceled",
	})
	require.Equal(t, &pb.CancelJobResponse{}, resp)
}

func TestJobManagerDebug(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobmanagerID := "debug-job-test"
	mockMaster := framework.NewMockMasterImpl("", jobmanagerID)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	manager := p2p.NewMockMessageHandlerManager()
	mgr := &JobManagerImplV2{
		BaseMaster:            mockMaster.DefaultBaseMaster,
		JobFsm:                NewJobFsm(),
		clocker:               clock.New(),
		frameMetaClient:       mockMaster.GetFrameMetaClient(),
		jobStatusChangeMu:     ctxmu.New(),
		messageHandlerManager: manager,
	}

	debugJobID := "Debug-Job-id"
	meta := &frameModel.MasterMetaKVData{ID: debugJobID}
	mgr.JobFsm.JobDispatched(meta, false)

	mockWorkerHandle := &framework.MockHandle{WorkerID: debugJobID, ExecutorID: "executor-1"}
	err := mgr.JobFsm.JobOnline(mockWorkerHandle)
	require.Nil(t, err)

	req := &pb.DebugJobRequest{
		JobId:   debugJobID,
		Command: "Debug",
		JsonArg: "",
	}

	go func() {
		time.Sleep(time.Second)
		manager.InvokeHandler(t, dm.GenerateTopic(debugJobID, jobmanagerID),
			"node-1", dm.GenerateResponse(1, "DebugJob", &pb.DebugJobResponse{}))
	}()
	resp := mgr.DebugJob(ctx, req)
	require.Nil(t, resp.Err)

	req.JobId = debugJobID + "-unknown"
	resp = mgr.DebugJob(ctx, req)
	require.NotNil(t, resp.Err)
	require.Equal(t, pb.ErrorCode_UnKnownJob, resp.Err.Code)
}

func TestJobManagerQueryJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		meta             *frameModel.MasterMetaKVData
		expectedPBStatus pb.QueryJobResponse_JobStatus
	}{
		{
			&frameModel.MasterMetaKVData{
				ID:         "master-1",
				Tp:         framework.FakeJobMaster,
				StatusCode: frameModel.MasterStatusFinished,
			},
			pb.QueryJobResponse_finished,
		},
		{
			&frameModel.MasterMetaKVData{
				ID:         "master-2",
				Tp:         framework.FakeJobMaster,
				StatusCode: frameModel.MasterStatusStopped,
			},
			pb.QueryJobResponse_stopped,
		},
	}

	mockMaster := framework.NewMockMasterImpl("", "job-manager-query-job-test")
	for _, tc := range testCases {
		cli := metadata.NewMasterMetadataClient(tc.meta.ID, mockMaster.GetFrameMetaClient())
		err := cli.Store(ctx, tc.meta)
		require.Nil(t, err)
	}

	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
	}

	statuses, err := mgr.GetJobStatuses(ctx)
	require.NoError(t, err)
	require.Len(t, statuses, len(testCases))

	for _, tc := range testCases {
		req := &pb.QueryJobRequest{
			JobId: tc.meta.ID,
		}
		resp := mgr.QueryJob(ctx, req)
		require.Nil(t, resp.Err)
		require.Equal(t, tc.expectedPBStatus, resp.GetStatus())

		require.Contains(t, statuses, tc.meta.ID)
		require.Equal(t, tc.meta.StatusCode, statuses[tc.meta.ID])
	}
}

func TestJobManagerOnlineJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		uuidGen:           uuid.NewGenerator(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		jobStatusChangeMu: ctxmu.New(),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     int32(engineModel.JobTypeCVSDemo),
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)

	err = mgr.JobFsm.JobOnline(&framework.MockHandle{
		WorkerID:   resp.JobId,
		ExecutorID: "executor-1",
	})
	require.Nil(t, err)
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: resp.JobId})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_online, queryResp.Status)
	require.Equal(t, queryResp.JobMasterInfo.Id, resp.JobId)
}

func TestJobManagerRecover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl("", "job-manager-recover-test")
	// prepare mockvk with two job masters
	meta := []*frameModel.MasterMetaKVData{
		{
			ID: "master-1",
			Tp: framework.FakeJobMaster,
		},
		{
			ID: "master-2",
			Tp: framework.FakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := metadata.NewMasterMetadataClient(data.ID, mockMaster.GetFrameMetaClient())
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}

	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
	}
	err := mgr.OnMasterRecovered(ctx)
	require.Nil(t, err)
	require.Equal(t, 2, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: "master-1"})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_dispatched, queryResp.Status)
}

func TestJobManagerTickExceedQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterImpl := framework.NewMockMasterImpl("", "create-worker-with-error")
	mockMaster := &mockBaseMasterCreateWorkerFailed{
		MockMasterImpl: masterImpl,
	}
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.NoError(t, err)

	mgr.JobFsm.JobDispatched(&frameModel.MasterMetaKVData{ID: "failover-job-master"}, true)
	// try to recreate failover job master, will meet quota error
	err = mgr.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))

	// try to recreate failover job master again, will meet quota error again
	err = mgr.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))
}

func TestJobManagerWatchJobStatuses(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl("", "cancel-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImplV2{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}

	err := mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMetaKVData{
		ID:         "job-to-be-canceled",
		Tp:         framework.FakeJobMaster,
		StatusCode: frameModel.MasterStatusStopped,
	})
	require.NoError(t, err)

	err = mgr.OnMasterRecovered(ctx)
	require.NoError(t, err)

	snap, stream, err := mgr.WatchJobStatuses(ctx)
	require.NoError(t, err)
	require.Equal(t, map[frameModel.MasterID]frameModel.MasterStatusCode{
		"job-to-be-canceled": frameModel.MasterStatusStopped,
	}, snap)

	resp := mgr.CancelJob(ctx, &pb.CancelJobRequest{
		JobId: "job-to-be-canceled",
	})
	require.Equal(t, &pb.CancelJobResponse{}, resp)

	event := <-stream.C
	require.Equal(t, resManager.JobStatusChangeEvent{
		EventType: resManager.JobRemovedEvent,
		JobID:     "job-to-be-canceled",
	}, event)
}
