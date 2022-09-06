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
	gerrors "errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/pingcap/tiflow/engine/enginepb"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/ctxmu"
	resManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	jobMock "github.com/pingcap/tiflow/engine/pkg/httputil/mock"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/servermaster/jobop"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/uuid"
)

func TestJobManagerCreateJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl(t, "", "create-job-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().EXPECT().ScheduleTask(
		gomock.Any(),
		gomock.Any()).Return(&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs()).Times(1)
	mgr := &JobManagerImpl{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		uuidGen:           uuid.NewGenerator(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}
	wg, ctx := errgroup.WithContext(ctx)
	mgr.wg = wg
	// set master impl to JobManagerImpl
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.CreateJobRequest{
		Job: &pb.Job{
			Type:   pb.Job_CVSDemo,
			Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
		},
	}
	job, err := mgr.CreateJob(ctx, req)
	require.NoError(t, err)
	err = mockMaster.Poll(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return mgr.JobFsm.QueryJob(job.Id) != nil
	}, time.Second*2, time.Millisecond*20)

	// Create a new job with the same id.
	req = &pb.CreateJobRequest{
		Job: &pb.Job{
			Id:     job.Id,
			Type:   pb.Job_CVSDemo,
			Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
		},
	}
	_, err = mgr.CreateJob(ctx, req)
	require.True(t, ErrJobAlreadyExists.Is(err))
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

func (m *mockBaseMasterCreateWorkerFailed) CreateWorkerV2(
	workerType framework.WorkerType,
	config framework.WorkerConfig,
	opts ...framework.CreateWorkerOpt,
) (frameModel.WorkerID, error) {
	return "", errors.ErrMasterConcurrencyExceeded.FastGenByArgs()
}

func TestCreateWorkerReturnError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterImpl := framework.NewMockMasterImpl(t, "", "create-worker-with-error")
	framework.MockMasterPrepareMeta(ctx, t, masterImpl)
	mockMaster := &mockBaseMasterCreateWorkerFailed{
		MockMasterImpl: masterImpl,
	}
	mgr := &JobManagerImpl{
		BaseMaster:      mockMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.CreateJobRequest{
		Job: &pb.Job{
			Type:   pb.Job_CVSDemo,
			Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
		},
	}
	_, err = mgr.CreateJob(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ErrMasterConcurrencyExceeded")
}

func TestJobManagerCancelJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl(t, "", "cancel-job-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImpl{
		BaseMaster:          mockMaster.DefaultBaseMaster,
		JobFsm:              NewJobFsm(),
		clocker:             clock.New(),
		frameMetaClient:     mockMaster.GetFrameMetaClient(),
		jobStatusChangeMu:   ctxmu.New(),
		jobOperatorNotifier: new(notify.Notifier),
	}
	mgr.jobOperator = jobop.NewJobOperatorImpl(mgr.frameMetaClient, mgr)

	cancelWorkerID := "cancel-worker-id"
	meta := &frameModel.MasterMeta{
		ID:    cancelWorkerID,
		Type:  framework.CvsJobMaster,
		State: frameModel.MasterStateInit,
	}
	mgr.JobFsm.JobDispatched(meta, false)

	err := mgr.frameMetaClient.UpsertJob(ctx, meta)
	require.NoError(t, err)
	mockWorkerHandle := &framework.MockHandle{WorkerID: cancelWorkerID, ExecutorID: "executor-1"}
	err = mgr.JobFsm.JobOnline(mockWorkerHandle)
	require.NoError(t, err)

	req := &pb.CancelJobRequest{
		Id: cancelWorkerID,
	}
	job, err := mgr.CancelJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.Job_Canceling, job.State)

	for i := 0; i < 5; i++ {
		err = mgr.jobOperator.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, i+1, mockWorkerHandle.SendMessageCount())
	}

	req.Id = cancelWorkerID + "-unknown"
	_, err = mgr.CancelJob(ctx, req)
	require.Error(t, err)
	require.True(t, ErrJobNotFound.Is(err))
}

func TestJobManagerDeleteJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl(t, "", "delete-job-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImpl{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}

	err := mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMeta{
		ID:    "job-to-be-deleted",
		Type:  framework.FakeJobMaster,
		State: frameModel.MasterStateStopped,
	})
	require.NoError(t, err)

	err = mgr.OnMasterRecovered(ctx)
	require.NoError(t, err)

	_, err = mgr.DeleteJob(ctx, &pb.DeleteJobRequest{
		Id: "job-to-be-deleted",
	})
	require.NoError(t, err)
	_, err = mgr.frameMetaClient.GetJobByID(ctx, "job-to-be-deleted")
	require.True(t, pkgOrm.IsNotFoundError(err))
}

func TestJobManagerGetJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		meta             *frameModel.MasterMeta
		expectedPBStatus pb.Job_State
	}{
		{
			&frameModel.MasterMeta{
				ID:    "master-1",
				Type:  framework.FakeJobMaster,
				State: frameModel.MasterStateUninit,
			},
			pb.Job_Created,
		},
		{
			&frameModel.MasterMeta{
				ID:    "master-2",
				Type:  framework.FakeJobMaster,
				State: frameModel.MasterStateInit,
			},
			pb.Job_Running,
		},
		{
			&frameModel.MasterMeta{
				ID:    "master-3",
				Type:  framework.FakeJobMaster,
				State: frameModel.MasterStateFinished,
			},
			pb.Job_Finished,
		},
		{
			&frameModel.MasterMeta{
				ID:    "master-4",
				Type:  framework.FakeJobMaster,
				State: frameModel.MasterStateStopped,
			},
			pb.Job_Canceled,
		},
	}

	mockMaster := framework.NewMockMasterImpl(t, "", "job-manager-get-job-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	for _, tc := range testCases {
		cli := metadata.NewMasterMetadataClient(tc.meta.ID, mockMaster.GetFrameMetaClient())
		err := cli.Store(ctx, tc.meta)
		require.Nil(t, err)
	}

	mgr := &JobManagerImpl{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
		jobHTTPClient:    jobMock.NewMockNilReturnJobHTTPClient(),
	}

	statuses, err := mgr.GetJobStatuses(ctx)
	require.NoError(t, err)
	require.Len(t, statuses, len(testCases)+1)

	for _, tc := range testCases {
		req := &pb.GetJobRequest{
			Id: tc.meta.ID,
		}
		job, err := mgr.GetJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, tc.expectedPBStatus, job.GetState())

		require.Contains(t, statuses, tc.meta.ID)
		require.Equal(t, tc.meta.State, statuses[tc.meta.ID])
	}
}

func TestJobManagerOnlineJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl(t, "", "submit-job-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().EXPECT().ScheduleTask(gomock.Any(), gomock.Any()).
		Return(&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs()).MinTimes(0)
	mgr := &JobManagerImpl{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		uuidGen:           uuid.NewGenerator(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		jobStatusChangeMu: ctxmu.New(),
	}
	// set master impl to JobManagerImpl
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.CreateJobRequest{
		Job: &pb.Job{
			Type:   pb.Job_CVSDemo,
			Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
		},
	}
	job, err := mgr.CreateJob(ctx, req)
	require.NoError(t, err)

	err = mgr.JobFsm.JobOnline(&framework.MockHandle{
		WorkerID:   job.Id,
		ExecutorID: "executor-1",
	})
	require.NoError(t, err)
	require.Len(t, mgr.JobFsm.waitAckJobs, 0)
	require.Len(t, mgr.JobFsm.onlineJobs, 1)
}

func TestJobManagerRecover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl(t, "", "job-manager-recover-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	// prepare mockvk with two job masters
	meta := []*frameModel.MasterMeta{
		{
			ID:   "master-1",
			Type: framework.FakeJobMaster,
		},
		{
			ID:   "master-2",
			Type: framework.FakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := metadata.NewMasterMetadataClient(data.ID, mockMaster.GetFrameMetaClient())
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}

	mgr := &JobManagerImpl{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
		jobHTTPClient:    jobMock.NewMockNilReturnJobHTTPClient(),
	}
	err := mgr.OnMasterRecovered(ctx)
	require.NoError(t, err)
	require.Len(t, mgr.JobFsm.waitAckJobs, 3)
}

func TestJobManagerTickExceedQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterImpl := framework.NewMockMasterImpl(t, "", "create-worker-with-error")
	framework.MockMasterPrepareMeta(ctx, t, masterImpl)
	mockMaster := &mockBaseMasterCreateWorkerFailed{
		MockMasterImpl: masterImpl,
	}
	mgr := &JobManagerImpl{
		BaseMaster:      mockMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
		jobHTTPClient:   jobMock.NewMockNilReturnJobHTTPClient(),
	}
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.NoError(t, err)

	mgr.JobFsm.JobDispatched(&frameModel.MasterMeta{ID: "failover-job-master"}, true)
	// try to recreate failover job master, will meet quota error
	err = mgr.Tick(ctx)
	require.NoError(t, err)
	require.Len(t, mgr.JobFsm.waitAckJobs, 1)

	// try to recreate failover job master again, will meet quota error again
	err = mgr.Tick(ctx)
	require.NoError(t, err)
	require.Len(t, mgr.JobFsm.waitAckJobs, 1)
}

func TestJobManagerWatchJobStatuses(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := framework.NewMockMasterImpl(t, "", "delete-job-test")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImpl{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
		jobHTTPClient:     jobMock.NewMockNilReturnJobHTTPClient(),
	}

	err := mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMeta{
		ID:    "job-to-be-deleted",
		Type:  framework.FakeJobMaster,
		State: frameModel.MasterStateStopped,
	})
	require.NoError(t, err)

	err = mgr.OnMasterRecovered(ctx)
	require.NoError(t, err)

	snap, stream, err := mgr.WatchJobStatuses(ctx)
	require.NoError(t, err)
	require.Equal(t, map[frameModel.MasterID]frameModel.MasterState{
		"delete-job-test":   frameModel.MasterStateUninit,
		"job-to-be-deleted": frameModel.MasterStateStopped,
	}, snap)

	_, err = mgr.DeleteJob(ctx, &pb.DeleteJobRequest{
		Id: "job-to-be-deleted",
	})
	require.NoError(t, err)

	event := <-stream.C
	require.Equal(t, resManager.JobStatusChangeEvent{
		EventType: resManager.JobRemovedEvent,
		JobID:     "job-to-be-deleted",
	}, event)
}

func TestGetJobDetailFromJobMaster(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	mockMaster := framework.NewMockMasterImpl(t, "", "get-job-detail")
	framework.MockMasterPrepareMeta(ctx, t, mockMaster)
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJobClient := jobMock.NewMockJobHTTPClient(mockCtrl)

	mgr := &JobManagerImpl{
		BaseMaster:        mockMaster.DefaultBaseMaster,
		JobFsm:            NewJobFsm(),
		clocker:           clock.New(),
		frameMetaClient:   mockMaster.GetFrameMetaClient(),
		masterMetaClient:  metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
		jobHTTPClient:     mockJobClient,
	}

	// normal case, return job detail
	err := mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMeta{
		ID:   "new-job",
		Type: framework.FakeJobMaster,
		// set state to running
		State:    frameModel.MasterStateInit,
		Addr:     "1.1.1.1:1",
		ErrorMsg: "error_message",
	})
	require.NoError(t, err)

	mockJobClient.EXPECT().GetJobDetail(ctx, "1.1.1.1:1", "new-job").Return([]byte("detail test"), nil).Times(1)
	job, err := mgr.GetJob(ctx, &pb.GetJobRequest{Id: "new-job"})
	require.NoError(t, err)
	require.True(t, proto.Equal(&pb.Job{
		Id:     "new-job",
		Type:   enginepb.Job_FakeJob,
		State:  enginepb.Job_Running,
		Detail: []byte("detail test"),
		Error: &pb.Error{
			Message: "error_message",
		},
	}, job))

	// test return 404
	err = mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMeta{
		ID:   "new-job",
		Type: framework.FakeJobMaster,
		// set status code to running state
		State:    frameModel.MasterStateInit,
		Addr:     "1.1.1.1:1",
		ErrorMsg: "error_message",
	})
	require.NoError(t, err)

	mockJobClient.EXPECT().GetJobDetail(ctx, "1.1.1.1:1", "new-job").Return(nil, nil).Times(1)
	job, err = mgr.GetJob(ctx, &pb.GetJobRequest{Id: "new-job"})
	require.NoError(t, err)
	require.True(t, proto.Equal(&pb.Job{
		Id:    "new-job",
		Type:  enginepb.Job_FakeJob,
		State: enginepb.Job_Running,
		Error: &pb.Error{
			Message: "error_message",
		},
	}, job))

	// test wrong url
	err = mgr.frameMetaClient.UpsertJob(ctx, &frameModel.MasterMeta{
		ID:   "new-job",
		Type: framework.FakeJobMaster,
		// set status code to running state
		State:    frameModel.MasterStateInit,
		Addr:     "123.123.12.1:234",
		ErrorMsg: "error_message",
	})
	require.NoError(t, err)

	mockJobClient.EXPECT().GetJobDetail(ctx, "123.123.12.1:234", "new-job").Return(nil, gerrors.New("error test")).Times(1)
	job, err = mgr.GetJob(ctx, &pb.GetJobRequest{Id: "new-job"})
	require.NoError(t, err)
	require.True(t, proto.Equal(&pb.Job{
		Id:    "new-job",
		Type:  enginepb.Job_FakeJob,
		State: enginepb.Job_Running,
		Error: &pb.Error{
			Message: "error test",
		},
	}, job))
}

func TestSetDetailToMasterMeta(t *testing.T) {
	cases := []struct {
		name             string
		detail           []byte
		err              error
		expectMasterMeta *frameModel.MasterMeta
	}{
		{
			name:   "NoDetailNoError",
			detail: nil,
			err:    nil,
			expectMasterMeta: &frameModel.MasterMeta{
				ErrorMsg: "original error",
				Detail:   []byte("original job detail"),
			},
		},
		{
			name:   "DetailWithNoError",
			detail: []byte("job detail for jobmaster"),
			err:    nil,
			expectMasterMeta: &frameModel.MasterMeta{
				ErrorMsg: "original error",
				Detail:   []byte("job detail for jobmaster"),
			},
		},
		{
			name:   "404Error",
			detail: nil,
			err:    errors.ErrJobManagerRespStatusCode404.GenWithStackByArgs(),
			expectMasterMeta: &frameModel.MasterMeta{
				ErrorMsg: "original error",
				Detail:   []byte("original job detail"),
			},
		},
		{
			name:   "NO404Error",
			detail: nil,
			err:    gerrors.New("some error"),
			expectMasterMeta: &frameModel.MasterMeta{
				ErrorMsg: "some error",
				Detail:   []byte("original job detail"),
			},
		},
	}

	for _, cs := range cases {
		masterMeta := &frameModel.MasterMeta{
			ErrorMsg: "original error",
			Detail:   []byte("original job detail"),
		}
		setDetailToMasterMeta(masterMeta, cs.detail, cs.err)
		require.Equal(t, cs.expectMasterMeta, masterMeta)
	}
}
