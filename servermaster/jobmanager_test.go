package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestJobManagerSubmitJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.TaskSchedulerResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster: mockMaster.DefaultBaseMaster,
		JobFsm:     NewJobFsm(),
		uuidGen:    uuid.NewGenerator(),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)
	time.Sleep(time.Millisecond * 10)
	require.Eventually(t, func() bool {
		return mgr.JobFsm.JobCount(pb.QueryJobResponse_online) == 0 &&
			mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched) == 0 &&
			mgr.JobFsm.JobCount(pb.QueryJobResponse_pending) == 1
	}, time.Second*2, time.Millisecond*20)
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: resp.JobIdStr})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_pending, queryResp.Status)
}

func TestJobManagerPauseJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "pause-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImplV2{
		BaseMaster: mockMaster.DefaultBaseMaster,
		JobFsm:     NewJobFsm(),
	}

	pauseWorkerID := "pause-worker-id"
	meta := &lib.MasterMetaKVData{ID: pauseWorkerID}
	mgr.JobFsm.JobDispatched(meta, false)

	mockWorkerHandler := &lib.MockWorkerHandler{WorkerID: pauseWorkerID}
	mockWorkerHandler.On("SendMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err := mgr.JobFsm.JobOnline(mockWorkerHandler)
	require.Nil(t, err)

	req := &pb.PauseJobRequest{
		JobIdStr: pauseWorkerID,
	}
	resp := mgr.PauseJob(ctx, req)
	require.Nil(t, resp.Err)

	req.JobIdStr = pauseWorkerID + "-unknown"
	resp = mgr.PauseJob(ctx, req)
	require.NotNil(t, resp.Err)
	require.Equal(t, pb.ErrorCode_UnKnownJob, resp.Err.Code)
}

func TestJobManagerQueryJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		meta             *lib.MasterMetaKVData
		expectedPBStatus pb.QueryJobResponse_JobStatus
	}{
		{
			&lib.MasterMetaKVData{
				ID:         "master-1",
				Tp:         lib.FakeJobMaster,
				StatusCode: lib.MasterStatusFinished,
			},
			pb.QueryJobResponse_finished,
		},
		{
			&lib.MasterMetaKVData{
				ID:         "master-2",
				Tp:         lib.FakeJobMaster,
				StatusCode: lib.MasterStatusStopped,
			},
			pb.QueryJobResponse_stopped,
		},
	}

	mockMaster := lib.NewMockMasterImpl("", "job-manager-query-job-test")
	for _, tc := range testCases {
		cli := lib.NewMasterMetadataClient(tc.meta.ID, mockMaster.MetaKVClient())
		err := cli.Store(ctx, tc.meta)
		require.Nil(t, err)
	}

	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: lib.NewMasterMetadataClient(lib.JobManagerUUID, mockMaster.MetaKVClient()),
	}

	for _, tc := range testCases {
		req := &pb.QueryJobRequest{
			JobId: tc.meta.ID,
		}
		resp := mgr.QueryJob(ctx, req)
		require.Nil(t, resp.Err)
		require.Equal(t, tc.expectedPBStatus, resp.GetStatus())
	}
}

func TestJobManagerOnlineJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.TaskSchedulerResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster: mockMaster.DefaultBaseMaster,
		JobFsm:     NewJobFsm(),
		uuidGen:    uuid.NewGenerator(),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)

	mockWorkerHandler := &lib.MockWorkerHandler{WorkerID: resp.JobIdStr}
	mockWorkerHandler.On("ToPB").Return(&pb.WorkerInfo{Id: resp.JobIdStr}, nil)
	err = mgr.JobFsm.JobOnline(mockWorkerHandler)
	require.Nil(t, err)
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: resp.JobIdStr})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_online, queryResp.Status)
	require.Equal(t, queryResp.JobMasterInfo.Id, resp.JobIdStr)
}

func TestJobManagerRecover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prepare mockvk with two job masters
	metaKVClient := mockkv.NewMetaMock()
	meta := []*lib.MasterMetaKVData{
		{
			ID: "master-1",
			Tp: lib.FakeJobMaster,
		},
		{
			ID: "master-2",
			Tp: lib.FakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := lib.NewMasterMetadataClient(data.ID, metaKVClient)
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}

	mockMaster := lib.NewMockMasterImpl("", "job-manager-recover-test")
	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: lib.NewMasterMetadataClient(lib.JobManagerUUID, metaKVClient),
	}
	err := mgr.OnMasterRecovered(ctx)
	require.Nil(t, err)
	require.Equal(t, 2, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: "master-1"})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_dispatched, queryResp.Status)
}
