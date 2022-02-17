package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
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
		jobFsm:     NewJobFsm(),
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
		return mgr.jobFsm.OnlineJobCount() == 0 &&
			mgr.jobFsm.WaitAckJobCount() == 0 &&
			mgr.jobFsm.PendingJobCount() == 1
	}, time.Second*2, time.Millisecond*20)
}

func TestJobManagerRecover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prepare metadata with two job masters
	metaKVClient := metadata.NewMetaMock()
	meta := []*lib.MasterMetaKVData{
		{
			ID: "master-1",
			MasterMetaExt: &lib.MasterMetaExt{
				ID: "master-1",
				Tp: lib.FakeJobMaster,
			},
		},
		{
			ID: "master-2",
			MasterMetaExt: &lib.MasterMetaExt{
				ID: "master-2",
				Tp: lib.FakeJobMaster,
			},
		},
	}
	for _, data := range meta {
		cli := lib.NewMasterMetadataClient(data.MasterMetaExt.ID, metaKVClient)
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}

	mockMaster := lib.NewMockMasterImpl("", "job-manager-recover-test")
	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		jobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: lib.NewMasterMetadataClient(lib.JobManagerUUID, metaKVClient),
	}
	err := mgr.OnMasterRecovered(ctx)
	require.Nil(t, err)
	require.Equal(t, 2, mgr.jobFsm.WaitAckJobCount())
}
